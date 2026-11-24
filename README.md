# Pinpoint Scorecard Webhook

Detects changes to **user scorecards** in Pinpoint ATS (completed, sentiment changed, edited) and
records them as internal events. Pinpoint has no webhook for scorecards, so the service simulates one
by polling the API and diffing against local state.

**Status: v0.2: PostgreSQL persistence (VM1) and structured observability.**
Read-only against Pinpoint. State in PostgreSQL schema `pinpoint_scorecard_webhook` (SQLite kept
for local tests). Not yet included: scheduler, rules engine, stage updates, deployment.

## How it works

```
poll-once ──► advisory lock (single instance) ──► mark stale 'running' runs as 'interrupted'
          ──► PinpointClient (httpx, GET only, retries, page cap, request counting)
                 │  GET /api/v1/applications?include=user_scorecards&sort=id&stats[total]=count
                 ▼
              discovery: per application -> user scorecards (ids, status, sentiment, timestamps)
                 ▼
              poller: diff vs scorecard_state ──► detected_events + poll_runs (ONE transaction)
                 ▼
              JSON logs on stderr (journald-ready)
```

- Each pass reads the **whole** application scope. Incremental `filter[updated_at][gt]` is not used:
  live tests showed scorecard edits do **not** change `application.updated_at`.
- Completion is decided by `status == "complete"`. `completed_at` stays set when a scorecard is
  edited, so it is never trusted on its own.
- Tracking key: `(application_id, user_scorecard_id)`. Event idempotency: a deterministic
  `event_id` built from the transition and the stored state version it starts from
  (`INSERT OR IGNORE`). Unchanged data never produces events.
- See [docs/pinpoint-api-discovery.md](docs/pinpoint-api-discovery.md) for endpoints, field
  semantics and open questions.

## PoC results (live, read-only)

| Check | Result |
|---|---|
| Discovery without a fixed `APPLICATION_ID` | 5 applications, 2 user scorecards, complete coverage in 1 GET |
| Baseline `poll-once` | 2 × `scorecard.discovered`, **no** retroactive `scorecard.completed` |
| Repeat with unchanged data | 0 events |
| Real edit `positive → negative` on one test scorecard | exactly 1 `scorecard.sentiment_changed` |
| Repeat after the edit | 0 events |
| Observed values | `status`: `complete`, `incomplete`; `sentiment`: `positive`, `negative`, null (also on complete scorecards) |

## PostgreSQL (VM1)

```powershell
python -m pinpoint_scorecard_webhook db-check                       # read-only: identity, version, privileges
python -m pinpoint_scorecard_webhook db-migrate                     # apply pending versioned migrations
python -m pinpoint_scorecard_webhook migrate-sqlite --backend postgres   # copy + verify SQLite PoC history
python -m pinpoint_scorecard_webhook poll-once --backend postgres
python -m pinpoint_scorecard_webhook events --backend postgres
python -m pinpoint_scorecard_webhook events --backend postgres --actionable   # automation queue
python -m pinpoint_scorecard_webhook events --backend postgres --review       # unproven first sightings
```
Setting `STATE_BACKEND=postgres` in the environment makes `--backend` unnecessary.

- Every statement names `pinpoint_scorecard_webhook.<table>` explicitly. The pinned `search_path` is
  defence in depth only, not a security boundary. Migrations are forward-only and checksummed. The only
  DROP they allow is `DROP CONSTRAINT`; TRUNCATE, DELETE, GRANT and REVOKE are rejected.
- Tables: `poll_runs`, `scorecard_state`, `detected_events`, `schema_migrations`; view `v_actionable_events`.
- Event lifecycle: `pending | processing | processed | skipped | failed`, and `skipped` requires a
  `processing_reason`. `scorecard.discovered` and imported history (`historical_baseline`) are always
  `skipped`, so they can never trigger automation.
- First sightings: Pinpoint may expose a scorecard only once it is complete. After a scope's baseline
  is established, a new scorecard that appears already complete becomes an actionable
  `scorecard.completed` only if its `completed_at` is after the last full observation of that scope
  (the watermark). Otherwise it is recorded for review (`review_*`, `events --review`). See
  [docs/postgres.md](docs/postgres.md) for the
  rules, the minimum privileges for a dedicated service user, and verification SQL.
- Single instance: `pg_try_advisory_lock` on the store's single session. A second poller exits with
  code 3 (`skipped_locked`). The lock is re-verified in `pg_locks` before every commit.
- Interrupted runs: a `poll_runs` row left in `running` (process killed, connection lost) is marked
  `interrupted` by the next run, which can only start while holding the lock.
- `migrate-sqlite` opens the SQLite file read-only, inserts with `ON CONFLICT DO NOTHING` in one
  transaction (idempotent), and verifies ids, hashes and timestamps row by row.
- Exit codes: 0 ok, 1 error, 2 config error, 3 another instance running.

## Local setup (Windows)

Requires Python 3.11+ (developed with 3.12).

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
Copy-Item .env.example .env   # then fill in values locally; never commit .env
```

Linux / Ubuntu VM:

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

## Discovery (account-wide, bounded)

```powershell
python -m pinpoint_scorecard_webhook discover --max-pages 2 --page-size 10   # bounded sample
python -m pinpoint_scorecard_webhook discover --all --summary-only           # to the end, capped by PINPOINT_MAX_PAGES
python -m pinpoint_scorecard_webhook discover-diff                           # compare the two latest runs (no API call)
```

- One of `--max-pages` or `--all` is required. A run that stops before the end prints `Coverage PARTIAL`.
- `--include-confidential` / `--include-sourced` add the documented filters for records the API excludes by default.
- Runs are saved to `snapshots/discovery/` (technical metadata only). The diff tracks `(application_id, user_scorecard_id)`.
- Completion is decided by `status` (`complete`). `completed_at` alone is not trusted; mismatches are flagged.

## Polling prototype (SQLite state, simulated webhook)

```powershell
python -m pinpoint_scorecard_webhook poll-once                 # one pass; bounded by PINPOINT_MAX_PAGES
python -m pinpoint_scorecard_webhook events --limit 20         # recent events (no API call)
python -m pinpoint_scorecard_webhook events --type scorecard.completed --unprocessed
```

- Backend: `STATE_BACKEND` / `--backend` (`sqlite` locally, `postgres` on VM1). SQLite state lives in
  `STATE_DB_PATH` (git-ignored). The v0.1 SQLite file (schema v1) is kept read-only as a migration source.
- Events: `scorecard.discovered`, `scorecard.completed`, `scorecard.left_complete`,
  `scorecard.sentiment_changed`, `scorecard.updated`. A scorecard first seen as complete emits only
  `discovered`; `completed` requires an observed transition.
- Each pass commits state, events and the run record in one transaction; a fetch error writes nothing
  except a `failed` run row. Scorecards not seen are kept, never treated as deleted.

## Single-application commands

```powershell
pinpoint-scorecards inspect                 # show metadata of APPLICATION_ID
pinpoint-scorecards snapshot-before         # save snapshots/application-<id>-before.json
#   ...complete the scorecard in the Pinpoint UI...
pinpoint-scorecards snapshot-after          # save -after.json, print diff, save -diff.json
pinpoint-scorecards compare                 # re-diff saved files, no API call
pinpoint-scorecards probe-user-scorecards   # optional: is GET /user_scorecards/{id} available?
```

Every command accepts `--application-id <id>` to override `.env`. Each command makes one or two GET requests.

## Tests

```powershell
python -m pytest -q
python -m ruff check src tests
```

## Layout

```
src/pinpoint_scorecard_webhook/
  config.py      settings from .env (READ_ONLY and DB_SCHEMA enforced)
  logging_setup.py  JSON/text logging to stderr with secret redaction
  client.py      read-only Pinpoint client: retries, 429 handling, bounded pagination
  snapshot.py    single-application metadata extraction and diff
  discovery.py   account-wide discovery, classification, run diff
  store.py       backend interface + SQLite backend
  pg_store.py    PostgreSQL backend (advisory lock, pinned search_path)
  pg_migrations.py  versioned, checksummed PostgreSQL migrations
  history_migration.py  SQLite -> PostgreSQL history copy and verification
  db_commands.py db-check, db-migrate, migrate-sqlite
  detection.py   first-sighting classification (baseline, watermark, review)
  event_policy.py  which event types may reach automation
  poller.py      event rules and poll_once
  cli.py         command-line entry point
tests/           pytest suite (HTTP mocked with respx, SQLite in temp dirs)
docs/            API discovery notes and live observations
```

Local, git-ignored: `.env`, `.venv/`, `state/` (SQLite), `snapshots/`, `data/`, `samples/`, logs.

## Security

- The API key goes only in `.env` or a secret manager. Never put it in code, logs or commits.
- Use a Pinpoint API key that has **read-only** permissions.
- Requests use sparse fieldsets, so names, emails, CVs and scorecard free text are never requested.
- Snapshots and SQLite store only ids, timestamps, status and sentiment. All local data is git-ignored.
- `READ_ONLY=true` is enforced at startup, and a request hook blocks any non-GET method.
- Logs and stored errors pass through a redaction filter (API key, DB password, credential patterns);
  HTTP logs carry path, page, status and duration only, never bodies or query values.
