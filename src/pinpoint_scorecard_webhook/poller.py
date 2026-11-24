"""One polling pass: discover scorecards, diff against SQLite, emit internal events.

Event rules (status values observed live: "complete", "incomplete"):

- first sighting                       -> scorecard.discovered (never a retroactive .completed)
- status non-complete -> complete      -> scorecard.completed
- status complete -> non-complete      -> scorecard.left_complete
- sentiment changed, no transition     -> scorecard.sentiment_changed
- any other observed change            -> scorecard.updated

A complete scorecard with sentiment null stays complete; no sentiment is inferred.
Missing scorecards are never treated as deleted: their state is simply not touched.
The whole pass (state + events + run row) commits in one SQLite transaction, and
nothing is written if fetching fails.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from .client import PinpointClient
from .discovery import (
    APP_INCOMPLETE_RESPONSE,
    APP_RELATIONSHIP_UNAVAILABLE,
    DiscoveryRun,
    ScorecardRecord,
    run_discovery,
)
from .errors import PinpointError
from .snapshot import COMPLETE_STATUSES
from .store import EventRow, StateStore, StoredScorecard

logger = logging.getLogger(__name__)

EVENT_DISCOVERED = "scorecard.discovered"
EVENT_COMPLETED = "scorecard.completed"
EVENT_LEFT_COMPLETE = "scorecard.left_complete"
EVENT_SENTIMENT_CHANGED = "scorecard.sentiment_changed"
EVENT_UPDATED = "scorecard.updated"


def observation_hash(sc: ScorecardRecord | StoredScorecard) -> str:
    payload = json.dumps([sc.status, sc.sentiment, sc.updated_at, sc.completed_at])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def derive_event_types(prev: StoredScorecard | None, obs: ScorecardRecord) -> list[str]:
    if prev is None:
        return [EVENT_DISCOVERED]
    if prev.observation_hash == observation_hash(obs):
        return []
    was_complete = prev.status in COMPLETE_STATUSES
    is_complete = obs.status in COMPLETE_STATUSES
    if not was_complete and is_complete:
        return [EVENT_COMPLETED]
    if was_complete and not is_complete:
        return [EVENT_LEFT_COMPLETE]
    if prev.sentiment != obs.sentiment:
        return [EVENT_SENTIMENT_CHANGED]
    return [EVENT_UPDATED]


def event_id(
    app_id: str,
    sc_id: str,
    event_type: str,
    prev_hash: str | None,
    new_hash: str,
    prev_changed_at: str | None = None,
) -> str:
    """Deterministic id for one transition.

    Replaying the same transition from the same stored state yields the same id
    (INSERT OR IGNORE dedupes it). `prev_changed_at` distinguishes a transition
    that repeats an earlier (prev_hash, new_hash) pair, e.g. A->B->A->B with
    identical observations, so the later one is not silently dropped.
    """
    raw = f"{app_id}|{sc_id}|{event_type}|{prev_hash or '-'}|{new_hash}|{prev_changed_at or '-'}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


@dataclass
class PollResult:
    run_id: str
    status: str  # success | partial | failed
    coverage: str | None = None
    end_reason: str | None = None
    pages_fetched: int = 0
    applications_inspected: int = 0
    scorecards_observed: int = 0
    scorecards_synced: int = 0
    scorecards_skipped: int = 0
    scorecards_not_seen: int = 0
    scorecards_in_state: int = 0
    events_by_type: dict[str, int] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def events_created(self) -> int:
        return sum(self.events_by_type.values())


def _now() -> str:
    return datetime.now(UTC).isoformat()


def poll_once(
    client: PinpointClient,
    store: StateStore,
    *,
    page_size: int,
    max_pages: int,
    include_confidential: bool = False,
    include_sourced: bool = False,
    now: Callable[[], str] = _now,
    run_id: str | None = None,
) -> PollResult:
    run_id = run_id or uuid.uuid4().hex
    store.start_run(run_id, now())

    try:
        run = run_discovery(
            client,
            page_size=page_size,
            max_pages=max_pages,
            include_confidential=include_confidential,
            include_sourced=include_sourced,
        )
    except (PinpointError, ValueError) as exc:
        # Nothing was applied: state and events stay exactly as before.
        message = f"{type(exc).__name__}: {exc}"
        store.fail_run(run_id, now(), message)
        logger.error("poll %s failed before applying any change: %s", run_id, message)
        return PollResult(run_id=run_id, status="failed", error=message,
                          scorecards_in_state=store.count_rows("scorecard_state"))

    try:
        return _apply(store, run, run_id, now())
    except Exception as exc:
        # The transaction was rolled back; record the failure and re-raise.
        store.fail_run(run_id, now(), f"apply failed: {type(exc).__name__}")
        raise


def _apply(store: StateStore, run: DiscoveryRun, run_id: str, seen_at: str) -> PollResult:
    result = PollResult(
        run_id=run_id,
        status="success",
        coverage=run.coverage,
        end_reason=run.end_reason,
        pages_fetched=run.pages_fetched,
        applications_inspected=len(run.applications),
        scorecards_observed=len(run.scorecards),
    )
    if run.coverage != "complete":
        result.reasons.append(f"partial coverage: stopped after {run.pages_fetched} page(s)")
    unavailable = [a for a in run.applications if a.classification == APP_RELATIONSHIP_UNAVAILABLE]
    incomplete = [a for a in run.applications if a.classification == APP_INCOMPLETE_RESPONSE]
    if unavailable:
        result.reasons.append(f"{len(unavailable)} application(s) without user_scorecards relationship")
    if incomplete:
        result.reasons.append(f"{len(incomplete)} application(s) with scorecards missing from included")
    if run.duplicate_application_rows:
        result.reasons.append(f"{run.duplicate_application_rows} duplicate application row(s) across pages")
    if result.reasons:
        result.status = "partial"

    events = Counter()
    with store.transaction() as conn:
        state = store.load_state()
        observed_keys: set[tuple[str, str]] = set()
        for obs in run.scorecards:
            if not obs.attributes_loaded:
                # Unknown attributes: leave the stored state untouched so no change is lost.
                result.scorecards_skipped += 1
                continue
            observed_keys.add(obs.key)
            prev = state.get(obs.key)
            new_hash = observation_hash(obs)
            for event_type in derive_event_types(prev, obs):
                inserted = store.insert_event(conn, EventRow(
                    event_id=event_id(obs.application_id, obs.user_scorecard_id, event_type,
                                      prev.observation_hash if prev else None, new_hash,
                                      prev.last_changed_at if prev else None),
                    application_id=obs.application_id,
                    user_scorecard_id=obs.user_scorecard_id,
                    event_type=event_type,
                    previous_status=prev.status if prev else None,
                    new_status=obs.status,
                    previous_sentiment=prev.sentiment if prev else None,
                    new_sentiment=obs.sentiment,
                    detected_at=seen_at,
                    processed_at=None,
                    run_id=run_id,
                ))
                if inserted:
                    events[event_type] += 1
            store.upsert_state(conn, {
                "application_id": obs.application_id,
                "user_scorecard_id": obs.user_scorecard_id,
                "status": obs.status,
                "sentiment": obs.sentiment,
                "updated_at": obs.updated_at,
                "completed_at": obs.completed_at,
                "observation_hash": new_hash,
                "seen_at": seen_at,
                "run_id": run_id,
            })
            result.scorecards_synced += 1

        # Not seen != deleted. Only counted; stored state is kept as-is.
        result.scorecards_not_seen = len(state.keys() - observed_keys)
        if result.scorecards_not_seen and run.coverage == "complete":
            result.reasons.append(
                f"{result.scorecards_not_seen} stored scorecard(s) not seen in this run (kept, not deleted)"
            )
        result.events_by_type = dict(events)
        store.finish_run(conn, run_id, {
            "finished_at": _now(),
            "status": result.status,
            "coverage": result.coverage,
            "end_reason": result.end_reason,
            "pages_fetched": result.pages_fetched,
            "applications_inspected": result.applications_inspected,
            "scorecards_observed": result.scorecards_observed,
            "scorecards_synced": result.scorecards_synced,
            "scorecards_skipped": result.scorecards_skipped,
            "scorecards_not_seen": result.scorecards_not_seen,
            "events_created": result.events_created,
            "reasons": json.dumps(result.reasons),
        })
    result.scorecards_in_state = store.count_rows("scorecard_state")
    return result
