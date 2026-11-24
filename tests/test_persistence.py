from __future__ import annotations

import hashlib
import json
import logging
import sqlite3

import httpx
import pytest
import respx
from pydantic import ValidationError

from pinpoint_scorecard_webhook.cli import EXIT_LOCKED, main
from pinpoint_scorecard_webhook.client import PinpointClient
from pinpoint_scorecard_webhook.config import Settings
from pinpoint_scorecard_webhook.history_migration import migrate_history, verify_history
from pinpoint_scorecard_webhook.logging_setup import (
    JsonFormatter,
    RedactingFilter,
    configure_logging,
    sanitize,
)
from pinpoint_scorecard_webhook.pg_migrations import (
    MIGRATIONS,
    Migration,
    apply_migrations,
    validate_migrations,
)
from pinpoint_scorecard_webhook.pg_store import _iso, _ts
from pinpoint_scorecard_webhook.poller import poll_once
from pinpoint_scorecard_webhook.store import (
    LegacySchemaError,
    StateStore,
    read_sqlite_export,
)

from .conftest import API_KEY, BASE, user_scorecard

DONE = "2026-11-24T16:03:10+00:00"

# Schema of the v0.1 PoC SQLite file (user_version = 1), used as migration source.
V1_SCHEMA = """
CREATE TABLE poll_runs (run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
  status TEXT NOT NULL CHECK (status IN ('running','success','partial','failed')), coverage TEXT,
  end_reason TEXT, pages_fetched INTEGER, applications_inspected INTEGER,
  scorecards_observed INTEGER, scorecards_synced INTEGER, scorecards_skipped INTEGER,
  scorecards_not_seen INTEGER, events_created INTEGER, reasons TEXT, error TEXT);
CREATE TABLE scorecard_state (application_id TEXT NOT NULL, user_scorecard_id TEXT NOT NULL,
  status TEXT, sentiment TEXT, updated_at TEXT, completed_at TEXT, observation_hash TEXT NOT NULL,
  first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, last_changed_at TEXT NOT NULL,
  last_run_id TEXT NOT NULL REFERENCES poll_runs (run_id),
  PRIMARY KEY (application_id, user_scorecard_id));
CREATE TABLE detected_events (event_id TEXT PRIMARY KEY, application_id TEXT NOT NULL,
  user_scorecard_id TEXT NOT NULL, event_type TEXT NOT NULL, previous_status TEXT,
  new_status TEXT, previous_sentiment TEXT, new_sentiment TEXT, detected_at TEXT NOT NULL,
  processed_at TEXT, run_id TEXT NOT NULL REFERENCES poll_runs (run_id));
PRAGMA user_version = 1;
"""
R1, R2 = "a" * 32, "b" * 32
H1, H2 = "1" * 64, "2" * 64


@pytest.fixture
def legacy_db(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(V1_SCHEMA)
    conn.executemany(
        "INSERT INTO poll_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (R1, "2026-11-24T17:55:15.591294+00:00", "2026-11-24T17:55:16.100000+00:00",
             "success", "complete", "total_reached", 1, 5, 2, 2, 0, 0, 2, "[]", None),
            (R2, "2026-11-24T18:01:22.811659+00:00", "2026-11-24T18:01:23.000000+00:00",
             "success", "complete", "total_reached", 1, 5, 2, 2, 0, 0, 1, "[]", None),
        ],
    )
    conn.execute(
        "INSERT INTO scorecard_state VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("100001", "500001", "complete", "negative", "2026-11-24T18:00:56+00:00",
         "2026-11-24T18:00:48+00:00", H2, "2026-11-24T17:55:15.591294+00:00",
         "2026-11-24T18:01:22.811659+00:00", "2026-11-24T18:01:22.811659+00:00", R2),
    )
    conn.executemany(
        "INSERT INTO detected_events VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("c" * 32, "100001", "500001", "scorecard.discovered", None, "complete", None,
             "positive", "2026-11-24T17:55:15.591294+00:00", None, R1),
            ("d" * 32, "100001", "500001", "scorecard.sentiment_changed", "complete", "complete",
             "positive", "negative", "2026-11-24T18:01:22.811659+00:00", None, R2),
        ],
    )
    conn.commit()
    conn.close()
    return path


def file_digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ------------------------------------------------------------ legacy SQLite


def test_legacy_db_is_refused_and_left_unchanged(legacy_db):
    before = file_digest(legacy_db)
    with pytest.raises(LegacySchemaError):
        StateStore(legacy_db)
    data = read_sqlite_export(legacy_db)
    assert file_digest(legacy_db) == before
    assert {k: len(v) for k, v in data.items()} == {
        "poll_runs": 2, "scorecard_state": 1, "detected_events": 2}
    assert data["poll_runs"][0]["reasons"] == []


def test_history_migration_is_idempotent_and_verified(legacy_db, tmp_path):
    before = file_digest(legacy_db)
    with StateStore(tmp_path / "target.sqlite3") as target:
        first = migrate_history(legacy_db, target)
        assert first.inserted == {"poll_runs": 2, "scorecard_state": 1, "detected_events": 2}
        second = migrate_history(legacy_db, target)
        assert second.inserted == {"poll_runs": 0, "scorecard_state": 0, "detected_events": 0}
        assert second.already_present == first.inserted
        assert verify_history(legacy_db, target) == []
        run = target.get_run(R1)
        assert run["source"] == "sqlite_migration" and run["duration_ms"] == 509
        events = {e.event_id: e for e in target.recent_events(limit=10)}
        assert events["d" * 32].previous_sentiment == "positive"
    assert file_digest(legacy_db) == before


def test_verification_detects_mismatches(legacy_db, tmp_path):
    with StateStore(tmp_path / "target.sqlite3") as target:
        migrate_history(legacy_db, target)
        with target.transaction() as conn:
            conn.execute("UPDATE detected_events SET new_sentiment = 'positive' "
                         "WHERE event_id = ?", ("d" * 32,))
        problems = verify_history(legacy_db, target)
    assert problems == [f"detected_events ('{'d' * 32}',): new_sentiment differs"]


def test_verification_tolerates_newer_target_state(legacy_db, tmp_path):
    with StateStore(tmp_path / "target.sqlite3") as target:
        migrate_history(legacy_db, target)
        target.start_run("e" * 32, "2026-11-25T09:00:00+00:00")
        with target.transaction() as conn:
            target.upsert_state(conn, {
                "application_id": "100001", "user_scorecard_id": "500001", "stage_id": "9",
                "status": "complete", "sentiment": "positive", "updated_at": DONE,
                "completed_at": DONE, "observation_hash": H1,
                "seen_at": "2026-11-25T09:00:00+00:00", "run_id": "e" * 32})
        assert verify_history(legacy_db, target) == []


# ----------------------------------------------------------- poll behaviour


def doc(scs, total=1):
    app = {"id": "100001", "type": "applications",
           "attributes": {"stage_id": 4242, "updated_at": DONE},
           "relationships": {"user_scorecards": {
               "data": [{"type": "user_scorecards", "id": s["id"]} for s in scs]}}}
    return {"data": [app], "included": scs, "meta": {"stats": {"total": {"count": total}}}}


@pytest.fixture
def store(tmp_path):
    with StateStore(tmp_path / "state.sqlite3") as s:
        yield s


def run_poll(settings, store, responses, **kwargs):
    respx.get(f"{BASE}/applications").mock(
        side_effect=[r if isinstance(r, httpx.Response) else httpx.Response(200, json=r)
                     for r in responses])
    with PinpointClient(settings, sleep=lambda _: None) as client:
        return poll_once(client, store, page_size=10, max_pages=5,
                         secrets=tuple(settings.secret_values()), **kwargs)


@respx.mock
def test_poll_records_http_requests_duration_host_and_stage(settings, store):
    sc = user_scorecard("500001", status="complete", completed_at=DONE, sentiment="positive")
    result = run_poll(settings, store, [httpx.Response(503), doc([sc])])
    assert result.status == "success" and result.http_requests == 2
    run = store.get_run(result.run_id)
    assert run["http_requests"] == 2 and run["duration_ms"] >= 0
    assert run["hostname"] and run["pid"] and run["source"] == "poller"
    stage = store._conn.execute("SELECT stage_id FROM scorecard_state").fetchone()[0]
    assert stage == "4242"


@respx.mock
def test_lock_held_by_another_instance_skips_the_run(settings, store, monkeypatch):
    monkeypatch.setattr(StateStore, "try_acquire_poller_lock", lambda self: False)
    result = run_poll(settings, store, [doc([])])
    assert result.status == "skipped_locked"
    assert store.count_rows("poll_runs") == 0
    assert not respx.calls  # no API traffic when locked


@respx.mock
def test_stale_running_runs_are_marked_interrupted(settings, store):
    store.start_run("f" * 32, "2026-11-24T10:00:00+00:00")  # process died mid-run
    result = run_poll(settings, store, [doc([])])
    assert result.interrupted_runs_recovered == 1
    stale = store.get_run("f" * 32)
    assert stale["status"] == "interrupted" and "ended before" in stale["error"]
    assert store.get_run(result.run_id)["status"] == "success"


@respx.mock
def test_failed_run_records_sanitized_error(settings, store):
    result = run_poll(settings, store, [httpx.Response(401, json={"errors": [{"code": "x"}]})])
    run = store.get_run(result.run_id)
    assert run["status"] == "failed" and run["http_requests"] == 1
    assert API_KEY not in run["error"]


@respx.mock
def test_cli_exit_code_when_locked(settings, capsys, monkeypatch, tmp_path):
    monkeypatch.setattr(StateStore, "try_acquire_poller_lock", lambda self: False)
    settings = settings.model_copy(update={"state_db_path": tmp_path / "s.sqlite3"})
    assert main(["poll-once"], settings=settings) == EXIT_LOCKED
    assert "skipped_locked" in capsys.readouterr().out


# ------------------------------------------------------------------ logging


def test_sanitize_removes_secrets_and_credentials():
    text = ("failed password=hunter2 x-api-key: abc123 "
            "postgresql://user:pw@host/db token SECRETVALUE\n\n more")
    out = sanitize(text, ["SECRETVALUE"])
    assert "hunter2" not in out and "abc123" not in out and ":pw@" not in out
    assert "SECRETVALUE" not in out and "\n" not in out
    assert len(sanitize("x" * 5000)) == 500


def test_json_formatter_and_redaction():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "key %s", (API_KEY,), None)
    record.run_id = "r1"
    record.detail = f"leak {API_KEY}"
    RedactingFilter([API_KEY]).filter(record)
    payload = json.loads(JsonFormatter().format(record))
    assert payload["msg"] == "key ***" and payload["run_id"] == "r1"
    assert payload["detail"] == "leak ***" and payload["level"] == "INFO"


@respx.mock
def test_poll_emits_structured_logs_without_secrets(settings, store, capsys):
    configure_logging("INFO", "json", settings.secret_values())
    sc = user_scorecard("500001", status="complete", completed_at=DONE, sentiment="positive")
    sc["attributes"]["summary"] = "INTERVIEW NOTES"
    try:
        result = run_poll(settings, store, [httpx.Response(503), doc([sc])])
    finally:
        logging.getLogger().handlers.clear()
    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines() if line.strip()]
    events = [line.get("event") for line in lines]
    assert events[:1] == ["poll_started"] and "poll_finished" in events
    assert "http_retry" in events and "http_request" in events and "events_detected" in events
    assert all(line.get("run_id") == result.run_id for line in lines if "run_id" in line)
    finished = next(line for line in lines if line.get("event") == "poll_finished")
    assert finished["http_requests"] == 2 and finished["coverage"] == "complete"
    raw = json.dumps(lines)
    assert API_KEY not in raw and "INTERVIEW NOTES" not in raw


# --------------------------------------------------------------- migrations


def test_migrations_are_valid_and_contain_no_destructive_sql():
    import re

    validate_migrations()
    for migration in MIGRATIONS:
        upper = migration.sql.upper()
        assert "TRUNCATE" not in upper and "DELETE FROM" not in upper
        # The only DROP allowed is DROP CONSTRAINT (widening a CHECK; no data removed).
        assert not re.search(r"\bDROP\s+(?!CONSTRAINT\b)", upper)


def test_destructive_migration_is_rejected():
    with pytest.raises(ValueError, match="forbidden"):
        validate_migrations((Migration(1, "bad", "DROP TABLE x"),))
    with pytest.raises(ValueError, match="gaps"):
        validate_migrations((Migration(2, "gap", "SELECT 1"),))


def test_apply_migrations_refuses_other_schemas():
    with pytest.raises(ValueError, match="refusing"):
        apply_migrations(object(), "public")


def test_timestamp_round_trip_preserves_instant():
    original = "2026-11-24T18:01:22.811659+00:00"
    assert _iso(_ts(original)) == original
    assert _ts("2026-11-24T18:00:56Z").isoformat() == "2026-11-24T18:00:56+00:00"


# ------------------------------------------------------------------- config


def test_db_schema_must_be_the_authorised_one():
    with pytest.raises(ValidationError):
        Settings(_env_file=None, pinpoint_subdomain="acme", pinpoint_api_key="k", db_schema="public")


def test_require_postgres_lists_missing_settings(settings):
    with pytest.raises(ValueError, match="DB_HOST"):
        settings.require_postgres()


def test_secret_values_include_db_password():
    s = Settings(_env_file=None, pinpoint_subdomain="acme", pinpoint_api_key=API_KEY,
                 db_password="pw-123")
    assert set(s.secret_values()) == {API_KEY, "pw-123"}
    assert "pw-123" not in repr(s)


def test_db_connection_error_is_logged_sanitized(settings, capsys, monkeypatch):
    import psycopg

    from pinpoint_scorecard_webhook import db_commands

    def refuse(*_a, **_k):
        raise psycopg.OperationalError("connection failed: password=topsecret host=127.0.0.1")

    monkeypatch.setattr(db_commands, "open_store", refuse)
    monkeypatch.setattr("pinpoint_scorecard_webhook.cli.open_store", refuse)
    code = main(["events", "--backend", "postgres"], settings=settings)
    err = capsys.readouterr().err
    logging.getLogger().handlers.clear()
    assert code == 1
    assert "db_connection_error" in err and "topsecret" not in err
