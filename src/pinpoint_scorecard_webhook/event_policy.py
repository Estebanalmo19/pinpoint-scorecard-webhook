"""Which detected events may ever reach automation (the future Rules Engine).

Lifecycle (`processing_status`):
    pending     waiting for the Rules Engine (only actionable event types)
    processing  claimed by a consumer
    processed   an action was executed successfully
    skipped     intentionally excluded; `processing_reason` says why. Never "processed":
                no action ran.
    failed      the consumer tried and failed

Safety rules enforced at insert time:
- `scorecard.discovered` (first sighting that is not a proven new completion) is
  always skipped: `baseline_discovery`, `discovery_incomplete` or `review_*`.
  See detection.py: a first sighting that is complete AND proven newer than the
  last full observation is emitted as `scorecard.completed` (pending) instead.
- `scorecard.updated` (timestamps only) is not actionable.
- Events imported from the SQLite PoC are skipped/`historical_baseline`.
"""

from __future__ import annotations

ACTIONABLE_EVENT_TYPES = frozenset({
    "scorecard.completed",
    "scorecard.sentiment_changed",
    "scorecard.left_complete",
})

REASON_HISTORICAL_BASELINE = "historical_baseline"
REASON_BASELINE_DISCOVERY = "baseline_discovery"
REASON_NOT_ACTIONABLE = "not_actionable_event_type"

PROCESSING_STATUSES = ("pending", "processing", "processed", "skipped", "failed")


def initial_processing(event_type: str) -> tuple[str, str | None]:
    """(processing_status, processing_reason) for a newly detected event."""
    if event_type in ACTIONABLE_EVENT_TYPES:
        return "pending", None
    if event_type == "scorecard.discovered":
        return "skipped", REASON_BASELINE_DISCOVERY
    return "skipped", REASON_NOT_ACTIONABLE
