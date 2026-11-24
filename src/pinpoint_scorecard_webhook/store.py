"""Local SQLite state for the polling prototype.

Holds the last observed state of every user scorecard, the detected events and
one row per poll run. Only technical metadata is stored (ids, timestamps,
status, sentiment): no personal data.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS poll_runs (
    run_id                  TEXT PRIMARY KEY,
    started_at              TEXT NOT NULL,
    finished_at             TEXT,
    status                  TEXT NOT NULL CHECK (status IN ('running', 'success', 'partial', 'failed')),
    coverage                TEXT,
    end_reason              TEXT,
    pages_fetched           INTEGER,
    applications_inspected  INTEGER,
    scorecards_observed     INTEGER,
    scorecards_synced       INTEGER,
    scorecards_skipped      INTEGER,
    scorecards_not_seen     INTEGER,
    events_created          INTEGER,
    reasons                 TEXT,
    error                   TEXT
);

CREATE TABLE IF NOT EXISTS scorecard_state (
    application_id      TEXT NOT NULL,
    user_scorecard_id   TEXT NOT NULL,
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
    run_id              TEXT NOT NULL REFERENCES poll_runs (run_id)
);

CREATE INDEX IF NOT EXISTS ix_events_detected_at ON detected_events (detected_at);
CREATE INDEX IF NOT EXISTS ix_events_unprocessed ON detected_events (processed_at)
    WHERE processed_at IS NULL;
"""


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


class StateStore:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        if str(path) != ":memory:":
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

    # -------------------------------------------------------------- poll runs

    def start_run(self, run_id: str, started_at: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO poll_runs (run_id, started_at, status) VALUES (?, ?, 'running')",
                (run_id, started_at),
            )

    def fail_run(self, run_id: str, finished_at: str, error: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE poll_runs SET status = 'failed', finished_at = ?, error = ? WHERE run_id = ?",
                (finished_at, error, run_id),
            )

    def finish_run(self, conn: sqlite3.Connection, run_id: str, values: dict[str, Any]) -> None:
        """Called inside the observation transaction so run and state commit together."""
        columns = ", ".join(f"{k} = ?" for k in values)
        conn.execute(f"UPDATE poll_runs SET {columns} WHERE run_id = ?", (*values.values(), run_id))

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM poll_runs WHERE run_id = ?", (run_id,)).fetchone()
        return dict(row) if row else None

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
                application_id, user_scorecard_id, status, sentiment, updated_at, completed_at,
                observation_hash, first_seen_at, last_seen_at, last_changed_at, last_run_id
            ) VALUES (
                :application_id, :user_scorecard_id, :status, :sentiment, :updated_at,
                :completed_at, :observation_hash, :seen_at, :seen_at, :seen_at, :run_id
            )
            ON CONFLICT (application_id, user_scorecard_id) DO UPDATE SET
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
            row,
        )

    @staticmethod
    def insert_event(conn: sqlite3.Connection, event: EventRow) -> bool:
        """INSERT OR IGNORE on the deterministic event_id. Returns True if inserted."""
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO detected_events (
                event_id, application_id, user_scorecard_id, event_type, previous_status,
                new_status, previous_sentiment, new_sentiment, detected_at, processed_at, run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id, event.application_id, event.user_scorecard_id, event.event_type,
                event.previous_status, event.new_status, event.previous_sentiment,
                event.new_sentiment, event.detected_at, event.processed_at, event.run_id,
            ),
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
        return [EventRow(**dict(r)) for r in self._conn.execute(sql, params).fetchall()]

    def count_rows(self, table: str) -> int:
        if table not in {"scorecard_state", "detected_events", "poll_runs"}:
            raise ValueError(table)
        return self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _split_sql(script: str) -> list[str]:
    return [s.strip() for s in script.split(";") if s.strip()]
