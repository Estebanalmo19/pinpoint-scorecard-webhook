"""State storage for the poller: shared types plus the SQLite backend.

Two backends implement the same interface (`StateBackend`): SQLite for local
tests and PostgreSQL (`pg_store.py`) for VM1. Only technical metadata is stored
(ids, timestamps, status, sentiment): no personal data.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Self

from .event_policy import ACTIONABLE_EVENT_TYPES, PROCESSING_STATUSES

SCHEMA_VERSION = 4

RUN_STATUSES = ("running", "success", "partial", "failed", "interrupted")
EVENT_TYPES = (
    "scorecard.discovered",
    "scorecard.completed",
    "scorecard.left_complete",
    "scorecard.sentiment_changed",
    "scorecard.updated",
)
STATE_TABLES = frozenset({"scorecard_state", "detected_events", "poll_runs"})

INTERRUPTED_ERROR = "process ended before the run completed (found 'running' while holding the lock)"

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS poll_runs (
    run_id                  TEXT PRIMARY KEY,
    started_at              TEXT NOT NULL,
    finished_at             TEXT,
    duration_ms             INTEGER,
    status                  TEXT NOT NULL CHECK (status IN {RUN_STATUSES}),
    coverage                TEXT,
    end_reason              TEXT,
    pages_fetched           INTEGER,
    http_requests           INTEGER,
    applications_inspected  INTEGER,
    scorecards_observed     INTEGER,
    scorecards_synced       INTEGER,
    scorecards_skipped      INTEGER,
    scorecards_not_seen     INTEGER,
    events_created          INTEGER,
    reasons                 TEXT,
    error                   TEXT,
    hostname                TEXT,
    pid                     INTEGER,
    source                  TEXT NOT NULL DEFAULT 'poller',
    scope_key               TEXT,
    scope                   TEXT
);

CREATE TABLE IF NOT EXISTS scorecard_state (
    application_id      TEXT NOT NULL,
    user_scorecard_id   TEXT NOT NULL,
    stage_id            TEXT,
    status              TEXT,
    sentiment           TEXT,
    updated_at          TEXT,
    completed_at        TEXT,
    observation_hash    TEXT NOT NULL,
    first_seen_at       TEXT NOT NULL,
    last_seen_at        TEXT NOT NULL,
    last_changed_at     TEXT NOT NULL,
    last_run_id         TEXT NOT NULL REFERENCES poll_runs (run_id),
    PRIMARY KEY (application_id, user_scorecard_id)
);

CREATE TABLE IF NOT EXISTS detected_events (
    event_id            TEXT PRIMARY KEY,
    application_id      TEXT NOT NULL,
    user_scorecard_id   TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    previous_status     TEXT,
    new_status          TEXT,
    previous_sentiment  TEXT,
    new_sentiment       TEXT,
    detected_at         TEXT NOT NULL,
    processed_at        TEXT,
    run_id              TEXT NOT NULL REFERENCES poll_runs (run_id),
    processing_status   TEXT NOT NULL DEFAULT 'pending' CHECK (processing_status IN {PROCESSING_STATUSES}),
    processing_reason   TEXT,
    evidence            TEXT,
    CHECK (processing_status <> 'skipped' OR processing_reason IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ix_events_detected_at ON detected_events (detected_at);
CREATE TABLE IF NOT EXISTS monitoring_baselines (
    scope_key        TEXT PRIMARY KEY,
    scope            TEXT NOT NULL,
    baseline_run_id  TEXT NOT NULL REFERENCES poll_runs (run_id),
    established_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_events_unprocessed ON detected_events (processed_at)
    WHERE processed_at IS NULL;
"""

RUN_COLUMNS = (
    "run_id", "started_at", "finished_at", "duration_ms", "status", "coverage", "end_reason",
    "pages_fetched", "http_requests", "applications_inspected", "scorecards_observed",
    "scorecards_synced", "scorecards_skipped", "scorecards_not_seen", "events_created",
    "reasons", "error", "hostname", "pid", "source", "scope_key", "scope",
)
STATE_COLUMNS = (
    "application_id", "user_scorecard_id", "stage_id", "status", "sentiment", "updated_at",
    "completed_at", "observation_hash", "first_seen_at", "last_seen_at", "last_changed_at",
    "last_run_id",
)
EVENT_COLUMNS = (
    "event_id", "application_id", "user_scorecard_id", "event_type", "previous_status",
    "new_status", "previous_sentiment", "new_sentiment", "detected_at", "processed_at", "run_id",
    "processing_status", "processing_reason", "evidence",
)


@dataclass(frozen=True)
class StoredScorecard:
    application_id: str
    user_scorecard_id: str
    status: str | None
    sentiment: str | None
    updated_at: str | None
    completed_at: str | None
    observation_hash: str
    # Moves only when the observation changes; identifies the state "version" an
    # event transitions from.
    last_changed_at: str | None = None


@dataclass(frozen=True)
class EventRow:
    event_id: str
    application_id: str
    user_scorecard_id: str
    event_type: str
    previous_status: str | None
    new_status: str | None
    previous_sentiment: str | None
    new_sentiment: str | None
    detected_at: str
    processed_at: str | None
    run_id: str
    processing_status: str = "pending"
    processing_reason: str | None = None
    # Why a first sighting was classified the way it was (audit).
    evidence: dict[str, Any] | None = None


class StateBackend(Protocol):
    """Interface used by the poller and the migration tool."""

    name: str

    def transaction(self) -> Any: ...
    def try_acquire_poller_lock(self) -> bool: ...
    def release_poller_lock(self) -> None: ...
    def verify_poller_lock(self) -> bool: ...
    def describe(self) -> dict[str, Any]: ...
    def actionable_events(self, *, limit: int = 100) -> list[EventRow]: ...
    def mark_interrupted_runs(self, finished_at: str) -> int: ...
    def start_run(self, run_id: str, started_at: str, *, hostname: str | None = None,
                  pid: int | None = None, scope_key: str | None = None,
                  scope: dict[str, Any] | None = None) -> None: ...
    def detection_context(self, scope_key: str, exclude_run_id: str) -> dict[str, Any]: ...
    def establish_baseline(self, conn: Any, scope_key: str, scope: dict[str, Any],
                           run_id: str, established_at: str) -> bool: ...
    def events_needing_review(self, *, limit: int = 100) -> list[EventRow]: ...
    def fail_run(self, run_id: str, finished_at: str, error: str,
                 extra: dict[str, Any] | None = None) -> None: ...
    def finish_run(self, conn: Any, run_id: str, values: dict[str, Any]) -> None: ...
    def get_run(self, run_id: str) -> dict[str, Any] | None: ...
    def load_state(self) -> dict[tuple[str, str], StoredScorecard]: ...
    def upsert_state(self, conn: Any, row: dict[str, Any]) -> None: ...
    def insert_event(self, conn: Any, event: EventRow) -> bool: ...
    def recent_events(self, *, limit: int = 20, event_types: Sequence[str] = (),
                      unprocessed_only: bool = False) -> list[EventRow]: ...
    def count_rows(self, table: str) -> int: ...
    def import_run(self, conn: Any, row: dict[str, Any]) -> bool: ...
    def import_state(self, conn: Any, row: dict[str, Any]) -> bool: ...
    def export_all(self) -> dict[str, list[dict[str, Any]]]: ...
    def close(self) -> None: ...


class LegacySchemaError(RuntimeError):
    """The SQLite file uses an older schema and is kept read-only as a migration source."""


class StateStore:
    """SQLite backend (local tests and the v0.1 PoC state file)."""

    name = "sqlite"

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        if str(path) != ":memory:":
            if self.path.exists():
                version = read_sqlite_version(self.path)
                if 0 < version < SCHEMA_VERSION:
                    raise LegacySchemaError(
                        f"{self.path} uses state schema v{version}; it is preserved unchanged. "
                        "Use it as a source for `migrate-sqlite`, or point STATE_DB_PATH elsewhere."
                    )
            self.path.parent.mkdir(parents=True, exist_ok=True)
        # Autocommit mode; transactions are opened explicitly with BEGIN IMMEDIATE.
        self._conn = sqlite3.connect(str(path), isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        self._migrate()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()

    def _migrate(self) -> None:
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if version > SCHEMA_VERSION:
            raise RuntimeError(f"state db schema v{version} is newer than supported v{SCHEMA_VERSION}")
        # executescript() would COMMIT implicitly; run statements one by one instead.
        with self.transaction():
            for statement in _split_sql(SCHEMA):
                self._conn.execute(statement)
            self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Single write transaction: everything inside commits or rolls back together."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        else:
            self._conn.execute("COMMIT")

    # ---------------------------------------------------------------- locking

    def try_acquire_poller_lock(self) -> bool:
        # Local/test backend: a single process is assumed. Writes are still
        # serialised by BEGIN IMMEDIATE and events stay idempotent.
        return True

    def release_poller_lock(self) -> None:
        return None

    def verify_poller_lock(self) -> bool:
        return True

    def describe(self) -> dict[str, Any]:
        return {"backend": "sqlite", "path": str(self.path)}

    # -------------------------------------------------------------- poll runs

    def mark_interrupted_runs(self, finished_at: str) -> int:
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE poll_runs SET status = 'interrupted', finished_at = ?, error = ? "
                "WHERE status = 'running'",
                (finished_at, INTERRUPTED_ERROR),
            )
            return cursor.rowcount

    def start_run(self, run_id: str, started_at: str, *, hostname: str | None = None,
                  pid: int | None = None, scope_key: str | None = None,
                  scope: dict[str, Any] | None = None) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO poll_runs (run_id, started_at, status, hostname, pid, scope_key, scope) "
                "VALUES (?, ?, 'running', ?, ?, ?, ?)",
                (run_id, started_at, hostname, pid, scope_key,
                 json.dumps(scope) if scope is not None else None),
            )

    # ------------------------------------------------------ baseline/watermark

    def detection_context(self, scope_key: str, exclude_run_id: str) -> dict[str, Any]:
        baseline = self._conn.execute(
            "SELECT baseline_run_id FROM monitoring_baselines WHERE scope_key = ?", (scope_key,)
        ).fetchone()
        any_baseline = self._conn.execute(
            "SELECT EXISTS (SELECT 1 FROM monitoring_baselines)").fetchone()[0]
        watermark = self._conn.execute(
            "SELECT run_id, started_at FROM poll_runs WHERE scope_key = ? AND status = 'success' "
            "AND run_id <> ? ORDER BY started_at DESC LIMIT 1",
            (scope_key, exclude_run_id),
        ).fetchone()
        return {
            "baseline_run_id": baseline["baseline_run_id"] if baseline else None,
            "any_baseline": bool(any_baseline),
            "watermark_run_id": watermark["run_id"] if watermark else None,
            "watermark_at": watermark["started_at"] if watermark else None,
        }

    def establish_baseline(self, conn: sqlite3.Connection, scope_key: str, scope: dict[str, Any],
                           run_id: str, established_at: str) -> bool:
        cursor = conn.execute(
            "INSERT INTO monitoring_baselines (scope_key, scope, baseline_run_id, established_at) "
            "VALUES (?, ?, ?, ?) ON CONFLICT (scope_key) DO NOTHING",
            (scope_key, json.dumps(scope), run_id, established_at),
        )
        return cursor.rowcount == 1

    def events_needing_review(self, *, limit: int = 100) -> list[EventRow]:
        rows = self._conn.execute(
            "SELECT * FROM detected_events WHERE processing_status = 'skipped' "
            "AND processing_reason LIKE 'review%' ORDER BY detected_at, rowid LIMIT ?",
            (limit,),
        ).fetchall()
        return [_event_from_row(r) for r in rows]

    def fail_run(self, run_id: str, finished_at: str, error: str,
                 extra: dict[str, Any] | None = None) -> None:
        values = {"status": "failed", "finished_at": finished_at, "error": error, **(extra or {})}
        with self.transaction() as conn:
            self.finish_run(conn, run_id, values)

    def finish_run(self, conn: sqlite3.Connection, run_id: str, values: dict[str, Any]) -> None:
        """Called inside the observation transaction so run and state commit together."""
        values = {k: (json.dumps(v) if k in ("reasons", "scope") else v) for k, v in values.items()}
        unknown = set(values) - set(RUN_COLUMNS)
        if unknown:
            raise ValueError(f"unknown poll_runs columns: {sorted(unknown)}")
        columns = ", ".join(f"{k} = ?" for k in values)
        conn.execute(f"UPDATE poll_runs SET {columns} WHERE run_id = ?", (*values.values(), run_id))

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM poll_runs WHERE run_id = ?", (run_id,)).fetchone()
        if not row:
            return None
        run = dict(row)
        run["scope"] = json.loads(run["scope"]) if run.get("scope") else None
        return run

    # ---------------------------------------------------------------- state

    def load_state(self) -> dict[tuple[str, str], StoredScorecard]:
        rows = self._conn.execute(
            "SELECT application_id, user_scorecard_id, status, sentiment, updated_at, "
            "completed_at, observation_hash, last_changed_at FROM scorecard_state"
        ).fetchall()
        return {
            (r["application_id"], r["user_scorecard_id"]): StoredScorecard(**dict(r)) for r in rows
        }

    @staticmethod
    def upsert_state(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO scorecard_state (
                application_id, user_scorecard_id, stage_id, status, sentiment, updated_at,
                completed_at, observation_hash, first_seen_at, last_seen_at, last_changed_at,
                last_run_id
            ) VALUES (
                :application_id, :user_scorecard_id, :stage_id, :status, :sentiment, :updated_at,
                :completed_at, :observation_hash, :seen_at, :seen_at, :seen_at, :run_id
            )
            ON CONFLICT (application_id, user_scorecard_id) DO UPDATE SET
                stage_id = excluded.stage_id,
                status = excluded.status,
                sentiment = excluded.sentiment,
                updated_at = excluded.updated_at,
                completed_at = excluded.completed_at,
                last_changed_at = CASE
                    WHEN scorecard_state.observation_hash = excluded.observation_hash
                    THEN scorecard_state.last_changed_at ELSE excluded.last_seen_at END,
                observation_hash = excluded.observation_hash,
                last_seen_at = excluded.last_seen_at,
                last_run_id = excluded.last_run_id
            """,
            {"stage_id": None, **row},
        )

    @staticmethod
    def insert_event(conn: sqlite3.Connection, event: EventRow) -> bool:
        """Insert unless the deterministic event_id exists. Returns True if inserted.

        ON CONFLICT (event_id) DO NOTHING, not INSERT OR IGNORE: the latter would
        also silently swallow CHECK/NOT NULL violations.
        """
        cursor = conn.execute(
            f"INSERT INTO detected_events ({', '.join(EVENT_COLUMNS)}) "
            f"VALUES ({', '.join('?' for _ in EVENT_COLUMNS)}) "
            "ON CONFLICT (event_id) DO NOTHING",
            tuple(json.dumps(event.evidence) if c == "evidence" and event.evidence is not None
                  else getattr(event, c) for c in EVENT_COLUMNS),
        )
        return cursor.rowcount == 1

    # --------------------------------------------------------------- events

    def recent_events(
        self,
        *,
        limit: int = 20,
        event_types: Sequence[str] = (),
        unprocessed_only: bool = False,
    ) -> list[EventRow]:
        sql = "SELECT * FROM detected_events"
        clauses: list[str] = []
        params: list[Any] = []
        if event_types:
            clauses.append(f"event_type IN ({', '.join('?' for _ in event_types)})")
            params.extend(event_types)
        if unprocessed_only:
            clauses.append("processed_at IS NULL")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY detected_at DESC, rowid DESC LIMIT ?"
        params.append(limit)
        return [_event_from_row(r) for r in self._conn.execute(sql, params).fetchall()]

    def count_rows(self, table: str) -> int:
        if table not in STATE_TABLES:
            raise ValueError(table)
        return self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def actionable_events(self, *, limit: int = 100) -> list[EventRow]:
        """Events the future Rules Engine may act on (same rule as v_actionable_events)."""
        types = sorted(ACTIONABLE_EVENT_TYPES)
        rows = self._conn.execute(
            "SELECT * FROM detected_events WHERE processing_status = 'pending' "
            f"AND event_type IN ({', '.join('?' for _ in types)}) "
            "ORDER BY detected_at, rowid LIMIT ?",
            (*types, limit),
        ).fetchall()
        return [_event_from_row(r) for r in rows]

    # ------------------------------------------------------------ migration

    def import_run(self, conn: sqlite3.Connection, row: dict[str, Any]) -> bool:
        cols = [c for c in RUN_COLUMNS if c in row]
        cursor = conn.execute(
            f"INSERT INTO poll_runs ({', '.join(cols)}) "
            f"VALUES ({', '.join('?' for _ in cols)}) ON CONFLICT (run_id) DO NOTHING",
            tuple(json.dumps(row[c]) if c in ("reasons", "scope") and row[c] is not None
                  else row[c] for c in cols),
        )
        return cursor.rowcount == 1

    def import_state(self, conn: sqlite3.Connection, row: dict[str, Any]) -> bool:
        cols = [c for c in STATE_COLUMNS if c in row]
        cursor = conn.execute(
            f"INSERT INTO scorecard_state ({', '.join(cols)}) "
            f"VALUES ({', '.join('?' for _ in cols)}) "
            "ON CONFLICT (application_id, user_scorecard_id) DO NOTHING",
            tuple(row[c] for c in cols),
        )
        return cursor.rowcount == 1

    def export_all(self) -> dict[str, list[dict[str, Any]]]:
        return _export_sqlite(self._conn)


# ------------------------------------------------------------- legacy reading


def read_sqlite_version(path: Path) -> int:
    conn = _connect_read_only(path)
    try:
        return conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()


def read_sqlite_export(path: Path) -> dict[str, list[dict[str, Any]]]:
    """Read every table from a v1 or v2 SQLite state file without modifying it."""
    if not Path(path).exists():
        raise FileNotFoundError(path)
    conn = _connect_read_only(path)
    try:
        return _export_sqlite(conn)
    finally:
        conn.close()


def _connect_read_only(path: Path) -> sqlite3.Connection:
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _export_sqlite(conn: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    def rows(table: str, order: str) -> list[dict[str, Any]]:
        return [dict(r) for r in conn.execute(f"SELECT * FROM {table} ORDER BY {order}")]

    runs = rows("poll_runs", "started_at, run_id")
    for run in runs:
        reasons = run.get("reasons")
        run["reasons"] = json.loads(reasons) if isinstance(reasons, str) else reasons
        run.setdefault("source", "poller")
    return {
        "poll_runs": runs,
        "scorecard_state": rows("scorecard_state", "application_id, user_scorecard_id"),
        "detected_events": rows("detected_events", "detected_at, event_id"),
    }


def _event_from_row(row: sqlite3.Row) -> EventRow:
    data = dict(row)
    evidence = data.get("evidence")
    data["evidence"] = json.loads(evidence) if isinstance(evidence, str) else evidence
    return EventRow(**{k: data.get(k) for k in EventRow.__dataclass_fields__})


def _split_sql(script: str) -> list[str]:
    return [s.strip() for s in script.split(";") if s.strip()]
