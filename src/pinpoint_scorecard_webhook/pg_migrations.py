"""Versioned, forward-only PostgreSQL migrations for the authorised schema.

Expects a connection with a dict row factory (see `pg_store.connect`).

Rules: migrations are append-only (never edit an applied one; its checksum is
verified), contain no DROP/TRUNCATE, and only touch objects inside
`pinpoint_scorecard_webhook`. Each migration runs in its own transaction, under
a transaction-scoped advisory lock so two processes cannot migrate concurrently.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Any

from psycopg import sql

from .config import AUTHORIZED_SCHEMA

logger = logging.getLogger(__name__)

# pg_advisory_xact_lock(classid, objid) key reserved for migrations.
MIGRATION_LOCK = (0x5053, 1)


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "initial_schema", """
CREATE TABLE poll_runs (
    run_id                  UUID PRIMARY KEY,
    started_at              TIMESTAMPTZ NOT NULL,
    finished_at             TIMESTAMPTZ,
    duration_ms             BIGINT CHECK (duration_ms IS NULL OR duration_ms >= 0),
    status                  TEXT NOT NULL
        CHECK (status IN ('running', 'success', 'partial', 'failed', 'interrupted')),
    coverage                TEXT CHECK (coverage IS NULL OR coverage IN ('complete', 'partial')),
    end_reason              TEXT,
    pages_fetched           INTEGER CHECK (pages_fetched IS NULL OR pages_fetched >= 0),
    http_requests           INTEGER CHECK (http_requests IS NULL OR http_requests >= 0),
    applications_inspected  INTEGER CHECK (applications_inspected IS NULL OR applications_inspected >= 0),
    scorecards_observed     INTEGER CHECK (scorecards_observed IS NULL OR scorecards_observed >= 0),
    scorecards_synced       INTEGER CHECK (scorecards_synced IS NULL OR scorecards_synced >= 0),
    scorecards_skipped      INTEGER CHECK (scorecards_skipped IS NULL OR scorecards_skipped >= 0),
    scorecards_not_seen     INTEGER CHECK (scorecards_not_seen IS NULL OR scorecards_not_seen >= 0),
    events_created          INTEGER CHECK (events_created IS NULL OR events_created >= 0),
    reasons                 JSONB NOT NULL DEFAULT '[]'::jsonb,
    error                   TEXT CHECK (error IS NULL OR length(error) <= 1000),
    hostname                TEXT,
    pid                     INTEGER,
    source                  TEXT NOT NULL DEFAULT 'poller'
        CHECK (source IN ('poller', 'sqlite_migration')),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);
CREATE INDEX ix_poll_runs_started_at ON poll_runs (started_at DESC);
CREATE INDEX ix_poll_runs_running ON poll_runs (started_at) WHERE status = 'running';

CREATE TABLE scorecard_state (
    application_id          BIGINT NOT NULL CHECK (application_id > 0),
    user_scorecard_id       BIGINT NOT NULL CHECK (user_scorecard_id > 0),
    stage_id                BIGINT,
    status                  TEXT,
    sentiment               TEXT,
    pinpoint_updated_at     TIMESTAMPTZ,
    pinpoint_completed_at   TIMESTAMPTZ,
    observation_hash        CHAR(64) NOT NULL CHECK (observation_hash ~ '^[0-9a-f]{64}$'),
    first_seen_at           TIMESTAMPTZ NOT NULL,
    last_seen_at            TIMESTAMPTZ NOT NULL,
    last_changed_at         TIMESTAMPTZ NOT NULL,
    last_run_id             UUID NOT NULL REFERENCES poll_runs (run_id),
    PRIMARY KEY (application_id, user_scorecard_id),
    CHECK (last_seen_at >= first_seen_at),
    CHECK (last_changed_at >= first_seen_at)
);
CREATE INDEX ix_scorecard_state_status ON scorecard_state (status);
CREATE INDEX ix_scorecard_state_last_seen ON scorecard_state (last_seen_at);

CREATE TABLE detected_events (
    event_id                CHAR(32) PRIMARY KEY CHECK (event_id ~ '^[0-9a-f]{32}$'),
    application_id          BIGINT NOT NULL,
    user_scorecard_id       BIGINT NOT NULL,
    event_type              TEXT NOT NULL CHECK (event_type IN (
        'scorecard.discovered', 'scorecard.completed', 'scorecard.left_complete',
        'scorecard.sentiment_changed', 'scorecard.updated')),
    previous_status         TEXT,
    new_status              TEXT,
    previous_sentiment      TEXT,
    new_sentiment           TEXT,
    detected_at             TIMESTAMPTZ NOT NULL,
    processing_status       TEXT NOT NULL DEFAULT 'pending'
        CHECK (processing_status IN ('pending', 'processing', 'processed', 'failed')),
    processed_at            TIMESTAMPTZ,
    run_id                  UUID NOT NULL REFERENCES poll_runs (run_id),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((processing_status = 'processed') = (processed_at IS NOT NULL))
);
CREATE INDEX ix_detected_events_detected_at ON detected_events (detected_at DESC);
CREATE INDEX ix_detected_events_scorecard
    ON detected_events (application_id, user_scorecard_id, detected_at);
CREATE INDEX ix_detected_events_pending ON detected_events (detected_at)
    WHERE processing_status = 'pending';
CREATE INDEX ix_detected_events_run ON detected_events (run_id);
"""),
    # v2+ reference every object with the explicit schema: search_path is not
    # relied on as an isolation mechanism.
    Migration(2, "event_processing_lifecycle", """
ALTER TABLE pinpoint_scorecard_webhook.detected_events
    ADD COLUMN processing_reason TEXT;
ALTER TABLE pinpoint_scorecard_webhook.detected_events
    ADD COLUMN processing_updated_at TIMESTAMPTZ;
ALTER TABLE pinpoint_scorecard_webhook.detected_events
    DROP CONSTRAINT detected_events_processing_status_check;
ALTER TABLE pinpoint_scorecard_webhook.detected_events
    ADD CONSTRAINT ck_detected_events_processing_status
    CHECK (processing_status IN ('pending', 'processing', 'processed', 'skipped', 'failed'));
ALTER TABLE pinpoint_scorecard_webhook.detected_events
    ADD CONSTRAINT ck_detected_events_skipped_has_reason
    CHECK (processing_status <> 'skipped' OR processing_reason IS NOT NULL);
ALTER TABLE pinpoint_scorecard_webhook.detected_events
    ADD CONSTRAINT ck_detected_events_reason_length
    CHECK (processing_reason IS NULL OR length(processing_reason) <= 200);
UPDATE pinpoint_scorecard_webhook.detected_events
    SET processing_status = 'skipped',
        processing_reason = 'historical_baseline',
        processing_updated_at = now()
    WHERE processing_status = 'pending'
      AND run_id IN (SELECT run_id FROM pinpoint_scorecard_webhook.poll_runs
                     WHERE source = 'sqlite_migration');
UPDATE pinpoint_scorecard_webhook.detected_events
    SET processing_status = 'skipped',
        processing_reason = 'baseline_discovery',
        processing_updated_at = now()
    WHERE processing_status = 'pending'
      AND event_type = 'scorecard.discovered';
CREATE VIEW pinpoint_scorecard_webhook.v_actionable_events AS
    SELECT event_id, application_id, user_scorecard_id, event_type,
           previous_status, new_status, previous_sentiment, new_sentiment,
           detected_at, run_id
    FROM pinpoint_scorecard_webhook.detected_events
    WHERE processing_status = 'pending'
      AND event_type IN ('scorecard.completed', 'scorecard.sentiment_changed',
                         'scorecard.left_complete')
"""),
    # Persistent baseline per monitoring scope + detection evidence. Existing runs
    # all used the default scope (no filters), so they are backfilled with its key
    # (sha256 of {"endpoint":"/applications","filters":{}}). No event is modified.
    Migration(3, "monitoring_baseline_and_evidence", """
ALTER TABLE pinpoint_scorecard_webhook.poll_runs
    ADD COLUMN scope_key CHAR(64) CHECK (scope_key IS NULL OR scope_key ~ '^[0-9a-f]{64}$');
ALTER TABLE pinpoint_scorecard_webhook.poll_runs
    ADD COLUMN scope JSONB;
UPDATE pinpoint_scorecard_webhook.poll_runs
    SET scope_key = '8224a2100acf0a168889d318424eb3b4ecf59d7d8747a97f44f2e00d5e747c43',
        scope = '{"endpoint": "/applications", "filters": {}}'::jsonb
    WHERE scope_key IS NULL;
CREATE INDEX ix_poll_runs_scope_success
    ON pinpoint_scorecard_webhook.poll_runs (scope_key, started_at DESC)
    WHERE status = 'success';
CREATE TABLE pinpoint_scorecard_webhook.monitoring_baselines (
    scope_key        CHAR(64) PRIMARY KEY CHECK (scope_key ~ '^[0-9a-f]{64}$'),
    scope            JSONB NOT NULL,
    baseline_run_id  UUID NOT NULL REFERENCES pinpoint_scorecard_webhook.poll_runs (run_id),
    established_at   TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO pinpoint_scorecard_webhook.monitoring_baselines
    (scope_key, scope, baseline_run_id, established_at)
    SELECT DISTINCT ON (scope_key) scope_key, scope, run_id, started_at
    FROM pinpoint_scorecard_webhook.poll_runs
    WHERE status = 'success' AND coverage = 'complete' AND scope_key IS NOT NULL
    ORDER BY scope_key, started_at
    ON CONFLICT (scope_key) DO NOTHING;
ALTER TABLE pinpoint_scorecard_webhook.detected_events
    ADD COLUMN evidence JSONB;
CREATE VIEW pinpoint_scorecard_webhook.v_events_needing_review AS
    SELECT event_id, application_id, user_scorecard_id, event_type, new_status,
           new_sentiment, processing_reason, evidence, detected_at, run_id
    FROM pinpoint_scorecard_webhook.detected_events
    WHERE processing_status = 'skipped' AND processing_reason LIKE 'review%'
"""),
)

# DROP is forbidden except `DROP CONSTRAINT` (needed to widen a CHECK; removes no data).
_FORBIDDEN = re.compile(
    r"\b(DROP(?!\s+CONSTRAINT\b)|TRUNCATE|DELETE\s+FROM|ALTER\s+(ROLE|USER|SYSTEM|SCHEMA)"
    r"|GRANT|REVOKE|CREATE\s+(ROLE|USER|EXTENSION))\b",
    re.IGNORECASE,
)
_DDL_DML = re.compile(r"^\s*(CREATE|ALTER|UPDATE|INSERT)\b", re.IGNORECASE)


def validate_migrations(migrations: tuple[Migration, ...] = MIGRATIONS) -> None:
    versions = [m.version for m in migrations]
    if versions != list(range(1, len(migrations) + 1)):
        raise ValueError("migration versions must be 1..N without gaps")
    for m in migrations:
        if _FORBIDDEN.search(m.sql):
            raise ValueError(f"migration {m.version} contains a forbidden statement")
        if m.version >= 2:
            for statement in _statements(m.sql):
                if _DDL_DML.match(statement) and f"{AUTHORIZED_SCHEMA}." not in statement:
                    first_line = statement.splitlines()[0][:60]
                    raise ValueError(
                        f"migration {m.version}: statement without explicit schema: {first_line}"
                    )


def _statements(script: str) -> list[str]:
    return [s.strip() for s in script.split(";") if s.strip()]


def apply_migrations(conn: Any, schema: str = AUTHORIZED_SCHEMA) -> list[int]:
    """Create the schema if needed and apply pending migrations. Returns applied versions."""
    if schema != AUTHORIZED_SCHEMA:
        raise ValueError(f"refusing to migrate schema {schema!r}")
    validate_migrations()
    schema_id = sql.Identifier(schema)
    applied: list[int] = []
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", MIGRATION_LOCK)
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema_id))
        conn.execute(sql.SQL(
            "CREATE TABLE IF NOT EXISTS {}.schema_migrations ("
            " version INTEGER PRIMARY KEY,"
            " name TEXT NOT NULL,"
            " checksum CHAR(64) NOT NULL,"
            " applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        ).format(schema_id))
        done = {
            row["version"]: row["checksum"]
            for row in conn.execute(
                sql.SQL("SELECT version, checksum FROM {}.schema_migrations").format(schema_id)
            ).fetchall()
        }
    for migration in MIGRATIONS:
        if migration.version in done:
            if done[migration.version].strip() != migration.checksum:
                raise RuntimeError(
                    f"migration {migration.version} ({migration.name}) was modified after being applied"
                )
            continue
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", MIGRATION_LOCK)
            # Re-check under the lock: another process may have applied it.
            already = conn.execute(
                sql.SQL("SELECT 1 FROM {}.schema_migrations WHERE version = %s").format(schema_id),
                (migration.version,),
            ).fetchone()
            if already:
                continue
            if migration.version == 1:
                # v1 predates explicit qualification and is checksum-locked.
                conn.execute(sql.SQL("SET LOCAL search_path TO {}").format(schema_id))
            for statement in _statements(migration.sql):
                conn.execute(statement)
            conn.execute(
                sql.SQL("INSERT INTO {}.schema_migrations (version, name, checksum) "
                        "VALUES (%s, %s, %s)").format(schema_id),
                (migration.version, migration.name, migration.checksum),
            )
        applied.append(migration.version)
        logger.info("migration applied", extra={
            "event": "migration_applied", "version": migration.version, "migration": migration.name})
    return applied


def migration_status(conn: Any, schema: str = AUTHORIZED_SCHEMA) -> dict[str, Any]:
    exists = conn.execute(
        "SELECT to_regclass(%s) IS NOT NULL AS present", (f"{schema}.schema_migrations",)
    ).fetchone()["present"]
    applied: list[int] = []
    if exists:
        applied = [r["version"] for r in conn.execute(
            sql.SQL("SELECT version FROM {}.schema_migrations ORDER BY version").format(
                sql.Identifier(schema))).fetchall()]
    known = [m.version for m in MIGRATIONS]
    return {"applied": applied, "pending": [v for v in known if v not in applied]}
