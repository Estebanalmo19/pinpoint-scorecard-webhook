# Pinpoint Scorecard Webhook

Detects changes to **user scorecards** in Pinpoint ATS (completed, sentiment changed, edited) and
records them as internal events. Pinpoint has no webhook for scorecards, so the service simulates one
by polling the API and diffing against local state.

**Status: v0.1 proof of concept (validated against the live API, 2026-11-24).**
Read-only against Pinpoint. Local SQLite state. Not yet included: scheduler, rules engine, stage
updates, PostgreSQL, deployment.

## How it works

```
poll-once ──► PinpointClient (httpx, GET only, retries, page cap)
                 │  GET /api/v1/applications?include=user_scorecards&sort=id&stats[total]=count
                 ▼
              discovery: per application -> user scorecards (ids, status, sentiment, timestamps)
                 ▼
              poller: diff vs SQLite scorecard_state ──► detected_events (one transaction)
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

- State lives in `STATE_DB_PATH` (default `state/pinpoint_state.sqlite3`, git-ignored).
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
  config.py      settings from .env (READ_ONLY enforced)
  client.py      read-only Pinpoint client: retries, 429 handling, bounded pagination
  snapshot.py    single-application metadata extraction and diff
  discovery.py   account-wide discovery, classification, run diff
  store.py       SQLite schema and transactional state/event storage
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
