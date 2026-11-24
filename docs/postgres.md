# PostgreSQL persistence: isolation, lifecycle and operations

All objects live in the schema `pinpoint_scorecard_webhook`. Identifiers in this
document are fictitious.

## Schema isolation

- Every statement the service runs names its table explicitly
  (`pinpoint_scorecard_webhook.<table>`, composed with `psycopg.sql.Identifier`).
  A test fails if `pg_store.py` contains an unqualified table reference.
- Migrations v2 and later must qualify every `CREATE`/`ALTER`/`UPDATE`/`INSERT`;
  the validator rejects them otherwise. Migration v1 predates this rule and is
  checksum-locked, so it keeps a `SET LOCAL search_path` inside its own transaction.
- The session `search_path` is pinned to the schema as **defence in depth only**.
  It is not a security boundary. Isolation must come from role privileges.
- `DB_SCHEMA` is validated to be exactly `pinpoint_scorecard_webhook`.
- Migrations reject `DROP` (except `DROP CONSTRAINT`, used to widen a CHECK),
  `TRUNCATE`, `DELETE FROM`, `GRANT`/`REVOKE`, `ALTER ROLE/USER/SYSTEM/SCHEMA`
  and `CREATE ROLE/USER/EXTENSION`.

### Minimum privileges for a future dedicated service user (documentation only)

Today the service connects as a personal user that owns the schema. For VM1, use
two roles: an **owner** that runs migrations, and a **runtime** role for the
poller. Nothing below has been executed.

```sql
-- Run by a DBA. Role names are examples.
CREATE ROLE pinpoint_webhook_owner NOLOGIN;
CREATE ROLE pinpoint_webhook_svc LOGIN PASSWORD '<from secret manager>';

ALTER SCHEMA pinpoint_scorecard_webhook OWNER TO pinpoint_webhook_owner;   -- migrations

GRANT CONNECT ON DATABASE <database> TO pinpoint_webhook_svc;
GRANT USAGE ON SCHEMA pinpoint_scorecard_webhook TO pinpoint_webhook_svc;  -- no CREATE
GRANT SELECT, INSERT, UPDATE ON
      pinpoint_scorecard_webhook.poll_runs,
      pinpoint_scorecard_webhook.scorecard_state,
      pinpoint_scorecard_webhook.detected_events
   TO pinpoint_webhook_svc;                                                 -- no DELETE/TRUNCATE
GRANT SELECT ON pinpoint_scorecard_webhook.schema_migrations,
                pinpoint_scorecard_webhook.v_actionable_events
   TO pinpoint_webhook_svc;
REVOKE ALL ON SCHEMA public FROM pinpoint_webhook_svc;                      -- if PUBLIC grants exist
```

- Advisory locks need no grant.
- `db-migrate` must run as the owner role. `poll-once` and `events` need only the
  runtime role.
- Future Rules Engine: it needs `UPDATE (processing_status, processing_reason,
  processing_updated_at, processed_at)` on `detected_events`. Column-level grants
  can enforce that.

## Event lifecycle (`detected_events.processing_status`)

| Status | Meaning |
|---|---|
| `pending` | Waiting for the Rules Engine. Only actionable event types are inserted as pending. |
| `processing` | Claimed by a consumer. |
| `processed` | An action ran successfully (`processed_at` set; enforced by CHECK). |
| `skipped` | Intentionally excluded; `processing_reason` is mandatory (CHECK). It is never "processed" because no action ran. |
| `failed` | The consumer tried and failed. |

- Actionable types: `scorecard.completed`, `scorecard.sentiment_changed`, `scorecard.left_complete`.
- `scorecard.discovered` is always `skipped`. Its reason is `baseline_discovery`,
  `discovery_incomplete` or `review_*`; see the next section. A first sighting that
  is a proven new completion is emitted as `scorecard.completed` instead.
- `scorecard.updated` (timestamps only) is inserted as `skipped` / `not_actionable_event_type`.
- History imported from the SQLite PoC is `skipped` / `historical_baseline`. Migration
  v2 converted the existing rows, and `migrate-sqlite` inserts future imports that way.
  These rows stay available for audit.
- `v_actionable_events` = `pending` AND actionable type. The future Rules Engine must
  read from it, and the code applies the same filter again.

## First sightings: baseline vs. new completion (migration v3)

Pinpoint may only expose a user scorecard once it is completed. A scorecard seen
for the first time with `status=complete` can therefore be a genuinely new
evaluation, or an old one we could not see before.

- **Monitoring scope:** the filter set, hashed into `poll_runs.scope_key`. Page size
  and page limits are not part of it. Example: default scope = no filters.
- **Baseline** (`monitoring_baselines`, one row per scope): established in the same
  transaction as the first run of that scope with status `success` (complete coverage,
  no incomplete responses). Partial, failed and interrupted runs never establish it.
- **Watermark:** `started_at` of the last `success` run of the same scope, excluding
  the current one. It is the last time the whole scope was observed. Partial, failed
  and interrupted runs never move it.

| First sighting | Condition | Event | Processing |
|---|---|---|---|
| incomplete | no baseline for the scope | `discovered` | skipped / `baseline_discovery` |
| incomplete | baseline exists | `discovered` | skipped / `discovery_incomplete` (a later transition to complete is actionable) |
| complete | no baseline anywhere (initial) | `discovered` | skipped / `baseline_discovery` |
| complete | other scopes have a baseline, this one not (filters widened) | `discovered` | skipped / `review_scope_changed` |
| complete | gap since watermark > `MAX_OBSERVATION_GAP_HOURS` (24 h) | `discovered` | skipped / `review_observation_gap` |
| complete | `completed_at` missing | `discovered` | skipped / `review_unknown_completion_time` |
| complete | `completed_at` < watermark − `NEW_COMPLETION_TOLERANCE_SECONDS` (300 s) | `discovered` | skipped / `review_completed_before_last_full_observation` |
| complete | otherwise (completed after the last full observation) | **`completed`** | **pending**, with `evidence` |

- Every first-sighting event stores `evidence` (JSONB): scope, baseline run,
  watermark run and time, `completed_at`, tolerance, and the decision.
- `v_events_needing_review` lists every `review_*` case. They are never actionable.
- Existing scorecards are untouched by this rule: their transitions (`completed`,
  `left_complete`, `sentiment_changed`) keep the existing logic.
- Migration v3 backfilled `scope_key` on the existing runs (all used the default
  scope) and set the default-scope baseline to the first PoC run. It modified no
  event.

## Concurrency and recovery

- `pg_try_advisory_lock(0x5053, crc32(schema))` on the store's single connection. A
  second instance exits with code 3 and writes nothing.
- The same session runs the whole pass. Before committing, the poller re-checks
  `pg_locks` for `pg_backend_pid()` and rolls back if the lock is not held
  (`PollerLockLostError`).
- If a process dies, its `poll_runs` row stays `running`. The next run, which holds
  the lock, marks it `interrupted`.

## Read-only verification queries (DBeaver)

```sql
-- Migrations
SELECT version, name, applied_at FROM pinpoint_scorecard_webhook.schema_migrations ORDER BY version;

-- Event lifecycle overview
SELECT event_type, processing_status, processing_reason, count(*)
FROM pinpoint_scorecard_webhook.detected_events GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;

-- Historical baseline still available for audit
SELECT event_id, event_type, previous_sentiment, new_sentiment, detected_at,
       processing_status, processing_reason, processed_at
FROM pinpoint_scorecard_webhook.detected_events
WHERE processing_reason = 'historical_baseline' ORDER BY detected_at;

-- Automation queue (must be empty until a real transition happens)
SELECT * FROM pinpoint_scorecard_webhook.v_actionable_events ORDER BY detected_at;

-- Safety invariants: every query below must return 0
SELECT count(*) FROM pinpoint_scorecard_webhook.detected_events
 WHERE event_type = 'scorecard.discovered' AND processing_status <> 'skipped';
SELECT count(*) FROM pinpoint_scorecard_webhook.detected_events e
  JOIN pinpoint_scorecard_webhook.poll_runs r USING (run_id)
 WHERE r.source = 'sqlite_migration' AND e.processing_status <> 'skipped';
SELECT count(*) FROM pinpoint_scorecard_webhook.detected_events
 WHERE processing_status = 'processed' AND processed_at IS NULL;

-- Runs, newest first; stale or interrupted runs
SELECT run_id, source, status, coverage, started_at, duration_ms, http_requests, events_created, error
FROM pinpoint_scorecard_webhook.poll_runs ORDER BY started_at DESC LIMIT 20;
SELECT run_id, started_at, hostname, pid, status, error
FROM pinpoint_scorecard_webhook.poll_runs WHERE status IN ('running', 'interrupted');

-- Is a poller running right now? (advisory lock holder)
SELECT l.pid, a.application_name, a.backend_start
FROM pg_locks l JOIN pg_stat_activity a USING (pid)
WHERE l.locktype = 'advisory' AND l.classid = 20563 AND l.granted;

-- Constraints on detected_events
SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
WHERE conrelid = 'pinpoint_scorecard_webhook.detected_events'::regclass ORDER BY conname;
```

```sql
-- Baselines and the current watermark per scope
SELECT b.scope_key, b.scope, b.baseline_run_id, b.established_at,
       (SELECT max(started_at) FROM pinpoint_scorecard_webhook.poll_runs r
         WHERE r.scope_key = b.scope_key AND r.status = 'success') AS watermark
FROM pinpoint_scorecard_webhook.monitoring_baselines b;

-- First sightings recorded for review (never actionable)
SELECT detected_at, application_id, user_scorecard_id, processing_reason, evidence
FROM pinpoint_scorecard_webhook.v_events_needing_review ORDER BY detected_at DESC;

-- New completions detected on first sighting, with their evidence
SELECT detected_at, application_id, user_scorecard_id, processing_status,
       evidence->>'watermark_at' AS watermark, evidence->>'completed_at' AS completed_at
FROM pinpoint_scorecard_webhook.detected_events
WHERE event_type = 'scorecard.completed' AND previous_status IS NULL ORDER BY detected_at DESC;
```
