"""Classify a scorecard seen for the first time: baseline, new completion, or review.

Pinpoint may only expose a user scorecard once the evaluator completes it, so a
first sighting with status=complete can be a genuinely new evaluation. It can
also be an old record we could not see before (scope widened, previous coverage
gaps, long downtime). The evidence used to tell them apart is persistent:

- monitoring baseline: per monitoring *scope* (the filter set), established by the
  first run of that scope that had complete coverage and no incomplete responses.
  Before it exists, every first sighting is part of the baseline.
- watermark: `started_at` of the last *successful* run of the same scope, i.e. the
  last time the whole scope was observed. Partial, failed and interrupted runs
  never move it.

A first sighting with status=complete is a new, actionable completion only if:
  baseline exists for this scope
  AND the gap since the watermark is within `max_observation_gap`
  AND completed_at is known
  AND completed_at >= watermark - tolerance   (completed after our last full look)
Anything else is recorded as skipped with a `review_*` reason for audit.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from .discovery import ScorecardRecord
from .snapshot import COMPLETE_STATUSES

EVENT_DISCOVERED = "scorecard.discovered"
EVENT_COMPLETED = "scorecard.completed"

REASON_BASELINE_DISCOVERY = "baseline_discovery"
REASON_DISCOVERY_INCOMPLETE = "discovery_incomplete"
REASON_REVIEW_SCOPE_CHANGED = "review_scope_changed"
REASON_REVIEW_NO_WATERMARK = "review_no_watermark"
REASON_REVIEW_OBSERVATION_GAP = "review_observation_gap"
REASON_REVIEW_UNKNOWN_COMPLETION_TIME = "review_unknown_completion_time"
REASON_REVIEW_COMPLETED_BEFORE_WATERMARK = "review_completed_before_last_full_observation"

DETECTION_NEW_AFTER_BASELINE = "new_completion_after_baseline"


def scope_key(scope: dict[str, Any]) -> str:
    """Stable hash of the monitoring scope (endpoint + filters; not page size)."""
    canonical = json.dumps(
        {"endpoint": scope.get("endpoint", "/applications"), "filters": scope.get("filters", {})},
        sort_keys=True, separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def canonical_scope(filters: dict[str, str]) -> dict[str, Any]:
    return {"endpoint": "/applications", "filters": dict(sorted(filters.items()))}


@dataclass(frozen=True)
class DetectionPolicy:
    tolerance: timedelta = timedelta(minutes=5)
    max_observation_gap: timedelta = timedelta(hours=24)


@dataclass(frozen=True)
class DetectionContext:
    scope_key: str
    baseline_run_id: str | None       # baseline for THIS scope
    any_baseline: bool                 # a baseline exists for some scope
    watermark_run_id: str | None       # last successful run of this scope (excluding current)
    watermark_at: datetime | None

    @property
    def baseline_established(self) -> bool:
        return self.baseline_run_id is not None


@dataclass(frozen=True)
class FirstSighting:
    event_type: str
    processing_status: str
    processing_reason: str | None
    evidence: dict[str, Any]


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).astimezone(UTC)


def classify_first_sighting(
    obs: ScorecardRecord,
    ctx: DetectionContext,
    observed_at: str,
    policy: DetectionPolicy = DetectionPolicy(),
) -> FirstSighting:
    evidence: dict[str, Any] = {
        "scope_key": ctx.scope_key,
        "baseline_run_id": ctx.baseline_run_id,
        "watermark_run_id": ctx.watermark_run_id,
        "watermark_at": ctx.watermark_at.isoformat() if ctx.watermark_at else None,
        "completed_at": obs.completed_at,
        "observed_at": observed_at,
        "tolerance_s": int(policy.tolerance.total_seconds()),
        "max_gap_s": int(policy.max_observation_gap.total_seconds()),
    }

    def skipped(reason: str) -> FirstSighting:
        return FirstSighting(EVENT_DISCOVERED, "skipped", reason, {**evidence, "decision": reason})

    if obs.status not in COMPLETE_STATUSES:
        # Not complete yet: never actionable. A later transition to complete is.
        return skipped(REASON_DISCOVERY_INCOMPLETE if ctx.baseline_established
                       else REASON_BASELINE_DISCOVERY)

    if not ctx.baseline_established:
        # Initial baseline: historical. Widened/changed scope: unknown age -> review.
        return skipped(REASON_REVIEW_SCOPE_CHANGED if ctx.any_baseline
                       else REASON_BASELINE_DISCOVERY)
    if ctx.watermark_at is None:
        return skipped(REASON_REVIEW_NO_WATERMARK)

    now = _parse(observed_at) or datetime.now(UTC)
    if now - ctx.watermark_at > policy.max_observation_gap:
        return skipped(REASON_REVIEW_OBSERVATION_GAP)
    completed_at = _parse(obs.completed_at)
    if completed_at is None:
        return skipped(REASON_REVIEW_UNKNOWN_COMPLETION_TIME)
    if completed_at < ctx.watermark_at - policy.tolerance:
        return skipped(REASON_REVIEW_COMPLETED_BEFORE_WATERMARK)

    return FirstSighting(EVENT_COMPLETED, "pending", None,
                         {**evidence, "decision": DETECTION_NEW_AFTER_BASELINE})
