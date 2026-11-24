"""Copy the SQLite PoC history into another backend, then verify it value by value.

- The source file is opened read-only (`mode=ro`) and never modified.
- Everything is inserted in ONE target transaction with ON CONFLICT DO NOTHING,
  so re-running is idempotent and never overwrites newer target state.
- event_id, observation_hash and every timestamp are copied unchanged.
- Imported events are stored as skipped/`historical_baseline`: kept for audit,
  excluded from automation, and not "processed" (no action ran).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .detection import canonical_scope, scope_key
from .event_policy import REASON_HISTORICAL_BASELINE
from .store import EventRow, StateBackend, read_sqlite_export

logger = logging.getLogger(__name__)

TIMESTAMP_FIELDS = frozenset({
    "started_at", "finished_at", "updated_at", "completed_at", "first_seen_at",
    "last_seen_at", "last_changed_at", "detected_at", "processed_at",
})
RUN_COMPARE = (
    "run_id", "started_at", "finished_at", "status", "coverage", "end_reason", "pages_fetched",
    "applications_inspected", "scorecards_observed", "scorecards_synced", "scorecards_skipped",
    "scorecards_not_seen", "events_created", "reasons", "error",
)
STATE_COMPARE = (
    "application_id", "user_scorecard_id", "status", "sentiment", "updated_at", "completed_at",
    "observation_hash", "first_seen_at", "last_seen_at", "last_changed_at", "last_run_id",
)
EVENT_COMPARE = (
    "event_id", "application_id", "user_scorecard_id", "event_type", "previous_status",
    "new_status", "previous_sentiment", "new_sentiment", "detected_at", "processed_at", "run_id",
)


@dataclass
class MigrationReport:
    source_counts: dict[str, int] = field(default_factory=dict)
    inserted: dict[str, int] = field(default_factory=dict)
    already_present: dict[str, int] = field(default_factory=dict)


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    parsed = datetime.fromisoformat(str(value))
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).astimezone(UTC)


def _duration_ms(run: dict[str, Any]) -> int | None:
    start, end = _parse_ts(run.get("started_at")), _parse_ts(run.get("finished_at"))
    if start is None or end is None:
        return None
    return max(0, round((end - start).total_seconds() * 1000))


def migrate_history(source: Path, target: StateBackend) -> MigrationReport:
    data = read_sqlite_export(source)
    report = MigrationReport(source_counts={k: len(v) for k, v in data.items()})
    inserted = {k: 0 for k in data}
    with target.transaction() as conn:
        for run in data["poll_runs"]:
            row = {**run, "source": "sqlite_migration"}
            # The SQLite PoC only ever polled the default scope (no filters).
            if not row.get("scope_key"):
                default_scope = canonical_scope({})
                row["scope_key"], row["scope"] = scope_key(default_scope), default_scope
            if row.get("duration_ms") is None:
                row["duration_ms"] = _duration_ms(run)
            inserted["poll_runs"] += target.import_run(conn, row)
        for state in data["scorecard_state"]:
            inserted["scorecard_state"] += target.import_state(conn, state)
        for event in data["detected_events"]:
            row = {k: event.get(k) for k in EventRow.__dataclass_fields__}
            # Imported history never feeds automation: no action ran for it.
            row["processing_status"] = "skipped"
            row["processing_reason"] = REASON_HISTORICAL_BASELINE
            row["processed_at"] = None
            inserted["detected_events"] += target.insert_event(conn, EventRow(**row))
    report.inserted = inserted
    report.already_present = {k: report.source_counts[k] - inserted[k] for k in data}
    logger.info("sqlite history migrated", extra={
        "event": "history_migrated", "source_counts": report.source_counts,
        "inserted": report.inserted, "already_present": report.already_present})
    return report


def _normalize(row: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    out = {}
    for name in fields:
        value = row.get(name)
        if name in TIMESTAMP_FIELDS:
            value = _parse_ts(value)
        elif name.endswith("run_id") and value is not None:
            value = str(value).replace("-", "")
        elif name == "observation_hash" and value is not None:
            value = str(value).strip()
        elif name in ("application_id", "user_scorecard_id") and value is not None:
            value = str(value)
        out[name] = value
    return out


def verify_history(source: Path, target: StateBackend) -> list[str]:
    """Return human-readable mismatches between the SQLite source and the target."""
    src = read_sqlite_export(source)
    dst = target.export_all()
    problems: list[str] = []
    specs = {
        "poll_runs": (("run_id",), RUN_COMPARE),
        "scorecard_state": (("application_id", "user_scorecard_id"), STATE_COMPARE),
        "detected_events": (("event_id",), EVENT_COMPARE),
    }
    for table, (key_fields, fields) in specs.items():
        dst_index = {
            tuple(_normalize(r, key_fields).values()): _normalize(r, fields) for r in dst[table]
        }
        for row in src[table]:
            key = tuple(_normalize(row, key_fields).values())
            expected = _normalize(row, fields)
            actual = dst_index.get(key)
            if actual is None:
                problems.append(f"{table} {key}: missing in target")
                continue
            for name in fields:
                if expected[name] != actual[name]:
                    # scorecard_state may legitimately be newer in the target after a poll.
                    if table == "scorecard_state" and _target_is_newer(expected, actual):
                        break
                    problems.append(f"{table} {key}: {name} differs")
    return problems


def _target_is_newer(expected: dict[str, Any], actual: dict[str, Any]) -> bool:
    exp, act = expected.get("last_seen_at"), actual.get("last_seen_at")
    return exp is not None and act is not None and act > exp
