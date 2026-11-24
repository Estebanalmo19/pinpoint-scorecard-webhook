"""PostgreSQL backend (VM1). Same interface as the SQLite `StateStore`.

Isolation: every statement names its table as `pinpoint_scorecard_webhook.<table>`
(composed with `psycopg.sql.Identifier`). `search_path` is also pinned to the
schema, but only as defence in depth; it is not a security boundary. Real
isolation must come from the role's privileges (see docs/postgres.md).

Concurrency: a session-level advisory lock (`pg_try_advisory_lock`) taken on the
store's single connection guarantees one poller instance. The same connection
runs every statement of the pass, and the lock is re-verified (pg_locks for
pg_backend_pid()) inside the write transaction before it commits.
"""

from __future__ import annotations

import json
import logging
import zlib
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, Self

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .config import AUTHORIZED_SCHEMA, Settings
from .event_policy import ACTIONABLE_EVENT_TYPES
from .store import (
    EVENT_COLUMNS,
    INTERRUPTED_ERROR,
    RUN_COLUMNS,
    STATE_TABLES,
    EventRow,
    StoredScorecard,
)

logger = logging.getLogger(__name__)

# pg_try_advisory_lock(classid, objid): 'PS' + crc32 of the schema name.
POLLER_LOCK = (0x5053, zlib.crc32(AUTHORIZED_SCHEMA.encode()) & 0x7FFFFFFF)


def table(name: str) -> sql.Identifier:
    """Schema-qualified identifier for one of the service's own tables/views."""
    if name not in STATE_TABLES | {"schema_migrations", "v_actionable_events",
                                   "monitoring_baselines", "v_events_needing_review"}:
        raise ValueError(f"unknown table {name!r}")
    return sql.Identifier(AUTHORIZED_SCHEMA, name)


def _cols(names: Sequence[str]) -> sql.Composed:
    return sql.SQL(", ").join(sql.Identifier(n) for n in names)


def _placeholders(n: int) -> sql.Composed:
    return sql.SQL(", ").join(sql.Placeholder() for _ in range(n))


def connect(settings: Settings) -> psycopg.Connection:
    settings.require_postgres()
    options = (
        f"-c search_path={settings.db_schema} -c TimeZone=UTC "
        f"-c statement_timeout={settings.db_statement_timeout_ms} -c lock_timeout=10000"
    )
    return psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password.get_secret_value() if settings.db_password else None,
        connect_timeout=settings.db_connect_timeout_seconds,
        application_name="pinpoint-scorecard-webhook",
        options=options,
        autocommit=True,
        row_factory=dict_row,
    )


def _iso(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return value


def _ts(value: Any) -> datetime | None:
    """ISO-8601 string -> aware datetime (UTC assumed when no offset)."""
    if value is None or isinstance(value, datetime):
        return value
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _int(value: Any) -> int | None:
    return None if value is None else int(value)


class PostgresStateStore:
    name = "postgres"

    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn
        self._lock_held = False
        self.lock_backend_pid: int | None = None
        exists = conn.execute(
            "SELECT to_regclass(%s) IS NOT NULL AS ok", (f"{AUTHORIZED_SCHEMA}.poll_runs",)
        ).fetchone()["ok"]
        if not exists:
            raise RuntimeError(
                f"{AUTHORIZED_SCHEMA}.poll_runs not found; run `db-migrate` first"
            )

    @classmethod
    def from_settings(cls, settings: Settings) -> PostgresStateStore:
        return cls(connect(settings))

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        try:
            self.release_poller_lock()
        finally:
            self._conn.close()

    def describe(self) -> dict[str, Any]:
        info = self._conn.execute(
            "SELECT current_database() AS database, current_user AS db_user, "
            "inet_server_addr()::text AS server, inet_server_port() AS port, "
            "pg_backend_pid() AS backend_pid"
        ).fetchone()
        return {"backend": "postgres", "schema": AUTHORIZED_SCHEMA, **info}

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Connection]:
        with self._conn.transaction():
            yield self._conn

    # ---------------------------------------------------------------- locking

    def try_acquire_poller_lock(self) -> bool:
        row = self._conn.execute(
            "SELECT pg_try_advisory_lock(%s, %s) AS ok, pg_backend_pid() AS pid", POLLER_LOCK
        ).fetchone()
        self._lock_held = bool(row["ok"])
        self.lock_backend_pid = row["pid"] if self._lock_held else None
        return self._lock_held

    def verify_poller_lock(self) -> bool:
        """True if THIS session (same backend pid) still holds the poller lock."""
        if not self._lock_held:
            return False
        row = self._conn.execute(
            "SELECT pg_backend_pid() AS pid, EXISTS ("
            " SELECT 1 FROM pg_catalog.pg_locks"
            " WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted"
            " AND classid = %s AND objid = %s AND objsubid = 2) AS held",
            POLLER_LOCK,
        ).fetchone()
        return bool(row["held"]) and row["pid"] == self.lock_backend_pid

    def release_poller_lock(self) -> None:
        if self._lock_held and not self._conn.closed:
            self._conn.execute("SELECT pg_advisory_unlock(%s, %s)", POLLER_LOCK)
        self._lock_held = False

    # -------------------------------------------------------------- poll runs

    def mark_interrupted_runs(self, finished_at: str) -> int:
        """Only valid while holding the poller lock: any 'running' row is then stale."""
        with self.transaction() as conn:
            cursor = conn.execute(
                sql.SQL(
                    "UPDATE {t} SET status = 'interrupted', "
                    "finished_at = GREATEST(%s::timestamptz, started_at), error = %s, "
                    "duration_ms = (EXTRACT(EPOCH FROM (GREATEST(%s::timestamptz, started_at) "
                    "- started_at)) * 1000)::bigint "
                    "WHERE status = 'running'"
                ).format(t=table("poll_runs")),
                (finished_at, INTERRUPTED_ERROR, finished_at),
            )
            return cursor.rowcount

    def start_run(self, run_id: str, started_at: str, *, hostname: str | None = None,
                  pid: int | None = None, scope_key: str | None = None,
                  scope: dict[str, Any] | None = None) -> None:
        with self.transaction() as conn:
            conn.execute(
                sql.SQL("INSERT INTO {t} (run_id, started_at, status, hostname, pid, scope_key, scope) "
                        "VALUES (%s, %s, 'running', %s, %s, %s, %s)").format(t=table("poll_runs")),
                (run_id, _ts(started_at), hostname, pid, scope_key,
                 Jsonb(scope) if scope is not None else None),
            )

    # ------------------------------------------------------ baseline/watermark

    def detection_context(self, scope_key: str, exclude_run_id: str) -> dict[str, Any]:
        baseline = self._conn.execute(
            sql.SQL("SELECT baseline_run_id FROM {t} WHERE scope_key = %s").format(
                t=table("monitoring_baselines")), (scope_key,)
        ).fetchone()
        any_baseline = self._conn.execute(
            sql.SQL("SELECT EXISTS (SELECT 1 FROM {t}) AS ok").format(
                t=table("monitoring_baselines"))
        ).fetchone()["ok"]
        watermark = self._conn.execute(
            sql.SQL("SELECT run_id, started_at FROM {t} WHERE scope_key = %s "
                    "AND status = 'success' AND run_id <> %s "
                    "ORDER BY started_at DESC LIMIT 1").format(t=table("poll_runs")),
            (scope_key, exclude_run_id),
        ).fetchone()
        return {
            "baseline_run_id": str(baseline["baseline_run_id"]).replace("-", "") if baseline else None,
            "any_baseline": bool(any_baseline),
            "watermark_run_id": str(watermark["run_id"]).replace("-", "") if watermark else None,
            "watermark_at": _iso(watermark["started_at"]) if watermark else None,
        }

    def establish_baseline(self, conn: psycopg.Connection, scope_key: str, scope: dict[str, Any],
                           run_id: str, established_at: str) -> bool:
        cursor = conn.execute(
            sql.SQL("INSERT INTO {t} (scope_key, scope, baseline_run_id, established_at) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT (scope_key) DO NOTHING").format(
                t=table("monitoring_baselines")),
            (scope_key, Jsonb(scope), run_id, _ts(established_at)),
        )
        return cursor.rowcount == 1

    def events_needing_review(self, *, limit: int = 100) -> list[EventRow]:
        rows = self._conn.execute(
            sql.SQL("SELECT e.* FROM {v} v JOIN {t} e USING (event_id) "
                    "ORDER BY e.detected_at, e.created_at LIMIT %s").format(
                v=table("v_events_needing_review"), t=table("detected_events")),
            (limit,),
        ).fetchall()
        return [EventRow(**_normalize_event(r)) for r in rows]

    def fail_run(self, run_id: str, finished_at: str, error: str,
                 extra: dict[str, Any] | None = None) -> None:
        values = {"status": "failed", "finished_at": finished_at, "error": error, **(extra or {})}
        with self.transaction() as conn:
            self.finish_run(conn, run_id, values)

    def finish_run(self, conn: psycopg.Connection, run_id: str, values: dict[str, Any]) -> None:
        unknown = set(values) - set(RUN_COLUMNS)
        if unknown:
            raise ValueError(f"unknown poll_runs columns: {sorted(unknown)}")
        params: list[Any] = []
        for key, value in values.items():
            if key == "reasons":
                params.append(Jsonb(value or []))
            elif key == "scope":
                params.append(Jsonb(value) if value is not None else None)
            elif key in ("started_at", "finished_at"):
                params.append(_ts(value))
            else:
                params.append(value)
        sets = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(k)) for k in values
        )
        conn.execute(
            sql.SQL("UPDATE {t} SET {sets} WHERE run_id = %s").format(
                t=table("poll_runs"), sets=sets),
            (*params, run_id),
        )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            sql.SQL("SELECT * FROM {t} WHERE run_id = %s").format(t=table("poll_runs")), (run_id,)
        ).fetchone()
        return _normalize_run(row) if row else None

    # ---------------------------------------------------------------- state

    def load_state(self) -> dict[tuple[str, str], StoredScorecard]:
        rows = self._conn.execute(
            sql.SQL(
                "SELECT application_id, user_scorecard_id, status, sentiment, pinpoint_updated_at, "
                "pinpoint_completed_at, observation_hash, last_changed_at FROM {t}"
            ).format(t=table("scorecard_state"))
        ).fetchall()
        state = {}
        for r in rows:
            key = (str(r["application_id"]), str(r["user_scorecard_id"]))
            state[key] = StoredScorecard(
                application_id=key[0],
                user_scorecard_id=key[1],
                status=r["status"],
                sentiment=r["sentiment"],
                updated_at=_iso(r["pinpoint_updated_at"]),
                completed_at=_iso(r["pinpoint_completed_at"]),
                observation_hash=r["observation_hash"].strip(),
                last_changed_at=_iso(r["last_changed_at"]),
            )
        return state

    @staticmethod
    def upsert_state(conn: psycopg.Connection, row: dict[str, Any]) -> None:
        conn.execute(
            sql.SQL("""
            INSERT INTO {t} AS s (
                application_id, user_scorecard_id, stage_id, status, sentiment,
                pinpoint_updated_at, pinpoint_completed_at, observation_hash,
                first_seen_at, last_seen_at, last_changed_at, last_run_id
            ) VALUES (
                %(application_id)s, %(user_scorecard_id)s, %(stage_id)s, %(status)s, %(sentiment)s,
                %(updated_at)s, %(completed_at)s, %(observation_hash)s,
                %(seen_at)s, %(seen_at)s, %(seen_at)s, %(run_id)s
            )
            ON CONFLICT (application_id, user_scorecard_id) DO UPDATE SET
                stage_id = EXCLUDED.stage_id,
                status = EXCLUDED.status,
                sentiment = EXCLUDED.sentiment,
                pinpoint_updated_at = EXCLUDED.pinpoint_updated_at,
                pinpoint_completed_at = EXCLUDED.pinpoint_completed_at,
                last_changed_at = CASE
                    WHEN s.observation_hash = EXCLUDED.observation_hash
                    THEN s.last_changed_at ELSE EXCLUDED.last_seen_at END,
                observation_hash = EXCLUDED.observation_hash,
                last_seen_at = EXCLUDED.last_seen_at,
                last_run_id = EXCLUDED.last_run_id
            """).format(t=table("scorecard_state")),
            {
                **row,
                "application_id": _int(row["application_id"]),
                "user_scorecard_id": _int(row["user_scorecard_id"]),
                "stage_id": _int(row.get("stage_id")),
                "updated_at": _ts(row.get("updated_at")),
                "completed_at": _ts(row.get("completed_at")),
                "seen_at": _ts(row["seen_at"]),
            },
        )

    @staticmethod
    def insert_event(conn: psycopg.Connection, event: EventRow) -> bool:
        status = event.processing_status or ("processed" if event.processed_at else "pending")
        values = [
            event.event_id, _int(event.application_id), _int(event.user_scorecard_id),
            event.event_type, event.previous_status, event.new_status,
            event.previous_sentiment, event.new_sentiment, _ts(event.detected_at),
            _ts(event.processed_at), event.run_id, status, event.processing_reason,
            Jsonb(event.evidence) if event.evidence is not None else None,
        ]
        columns = [*EVENT_COLUMNS, "processing_updated_at"]
        values.append(_ts(event.detected_at) if status != "pending" else None)
        cursor = conn.execute(
            sql.SQL("INSERT INTO {t} ({cols}) VALUES ({vals}) ON CONFLICT (event_id) DO NOTHING")
            .format(t=table("detected_events"), cols=_cols(columns),
                    vals=_placeholders(len(columns))),
            values,
        )
        return cursor.rowcount == 1

    # --------------------------------------------------------------- events

    def recent_events(self, *, limit: int = 20, event_types: Sequence[str] = (),
                      unprocessed_only: bool = False) -> list[EventRow]:
        clauses: list[sql.Composable] = []
        params: list[Any] = []
        if event_types:
            clauses.append(sql.SQL("event_type = ANY(%s)"))
            params.append(list(event_types))
        if unprocessed_only:
            clauses.append(sql.SQL("processed_at IS NULL"))
        where = sql.SQL("WHERE ") + sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("")
        rows = self._conn.execute(
            sql.SQL("SELECT {cols} FROM {t} {where} ORDER BY detected_at DESC, created_at DESC "
                    "LIMIT %s").format(cols=_cols(EVENT_COLUMNS), t=table("detected_events"),
                                       where=where),
            (*params, limit),
        ).fetchall()
        return [EventRow(**_normalize_event(r)) for r in rows]

    def actionable_events(self, *, limit: int = 100) -> list[EventRow]:
        """Pending events of actionable types (the v_actionable_events view)."""
        rows = self._conn.execute(
            sql.SQL("SELECT e.* FROM {v} v JOIN {t} e USING (event_id) "
                    "ORDER BY e.detected_at, e.created_at LIMIT %s").format(
                v=table("v_actionable_events"), t=table("detected_events")),
            (limit,),
        ).fetchall()
        events = [EventRow(**_normalize_event(r)) for r in rows]
        # Defence in depth: never trust the view alone.
        return [e for e in events
                if e.processing_status == "pending" and e.event_type in ACTIONABLE_EVENT_TYPES]

    def count_rows(self, table_name: str) -> int:
        if table_name not in STATE_TABLES:
            raise ValueError(table_name)
        return self._conn.execute(
            sql.SQL("SELECT COUNT(*) AS n FROM {t}").format(t=table(table_name))
        ).fetchone()["n"]

    # ------------------------------------------------------------ migration

    def import_run(self, conn: psycopg.Connection, row: dict[str, Any]) -> bool:
        cols = [c for c in RUN_COLUMNS if c in row]
        values = []
        for c in cols:
            v = row[c]
            if c == "reasons":
                v = Jsonb(v or [])
            elif c == "scope":
                v = Jsonb(v) if v is not None else None
            elif c in ("started_at", "finished_at"):
                v = _ts(v)
            values.append(v)
        cursor = conn.execute(
            sql.SQL("INSERT INTO {t} ({cols}) VALUES ({vals}) ON CONFLICT (run_id) DO NOTHING")
            .format(t=table("poll_runs"), cols=_cols(cols), vals=_placeholders(len(cols))),
            values,
        )
        return cursor.rowcount == 1

    def import_state(self, conn: psycopg.Connection, row: dict[str, Any]) -> bool:
        cursor = conn.execute(
            sql.SQL("""
            INSERT INTO {t} (
                application_id, user_scorecard_id, stage_id, status, sentiment,
                pinpoint_updated_at, pinpoint_completed_at, observation_hash,
                first_seen_at, last_seen_at, last_changed_at, last_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (application_id, user_scorecard_id) DO NOTHING
            """).format(t=table("scorecard_state")),
            (
                _int(row["application_id"]), _int(row["user_scorecard_id"]),
                _int(row.get("stage_id")), row.get("status"), row.get("sentiment"),
                _ts(row.get("updated_at")), _ts(row.get("completed_at")), row["observation_hash"],
                _ts(row["first_seen_at"]), _ts(row["last_seen_at"]), _ts(row["last_changed_at"]),
                row["last_run_id"],
            ),
        )
        return cursor.rowcount == 1

    def export_all(self) -> dict[str, list[dict[str, Any]]]:
        runs = [
            _normalize_run(r) for r in self._conn.execute(
                sql.SQL("SELECT * FROM {t} ORDER BY started_at, run_id").format(
                    t=table("poll_runs"))).fetchall()
        ]
        state = []
        for r in self._conn.execute(
            sql.SQL("SELECT * FROM {t} ORDER BY application_id, user_scorecard_id").format(
                t=table("scorecard_state"))
        ).fetchall():
            state.append({
                "application_id": str(r["application_id"]),
                "user_scorecard_id": str(r["user_scorecard_id"]),
                "stage_id": None if r["stage_id"] is None else str(r["stage_id"]),
                "status": r["status"],
                "sentiment": r["sentiment"],
                "updated_at": _iso(r["pinpoint_updated_at"]),
                "completed_at": _iso(r["pinpoint_completed_at"]),
                "observation_hash": r["observation_hash"].strip(),
                "first_seen_at": _iso(r["first_seen_at"]),
                "last_seen_at": _iso(r["last_seen_at"]),
                "last_changed_at": _iso(r["last_changed_at"]),
                "last_run_id": str(r["last_run_id"]).replace("-", ""),
            })
        events = [
            _normalize_event(r) for r in self._conn.execute(
                sql.SQL("SELECT {cols} FROM {t} ORDER BY detected_at, event_id").format(
                    cols=_cols(EVENT_COLUMNS), t=table("detected_events"))).fetchall()
        ]
        return {"poll_runs": runs, "scorecard_state": state, "detected_events": events}


def _normalize_run(row: dict[str, Any]) -> dict[str, Any]:
    out = {k: _iso(row.get(k)) for k in RUN_COLUMNS}
    out["run_id"] = str(row["run_id"]).replace("-", "")
    out["scope_key"] = row["scope_key"].strip() if row.get("scope_key") else None
    reasons = row.get("reasons")
    out["reasons"] = json.loads(reasons) if isinstance(reasons, str) else reasons
    return out


def _normalize_event(row: dict[str, Any]) -> dict[str, Any]:
    out = {k: _iso(row.get(k)) for k in EVENT_COLUMNS}
    out["event_id"] = row["event_id"].strip()
    out["application_id"] = str(row["application_id"])
    out["user_scorecard_id"] = str(row["user_scorecard_id"])
    out["run_id"] = str(row["run_id"]).replace("-", "")
    out["evidence"] = row.get("evidence")
    return out
