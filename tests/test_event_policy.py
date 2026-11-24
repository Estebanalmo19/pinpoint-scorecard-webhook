from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import httpx
import pytest
import respx

from pinpoint_scorecard_webhook import db_commands
from pinpoint_scorecard_webhook.client import PinpointClient
from pinpoint_scorecard_webhook.event_policy import (
    ACTIONABLE_EVENT_TYPES,
    REASON_BASELINE_DISCOVERY,
    REASON_HISTORICAL_BASELINE,
    REASON_NOT_ACTIONABLE,
    initial_processing,
)
from pinpoint_scorecard_webhook.history_migration import migrate_history, verify_history
from pinpoint_scorecard_webhook.pg_migrations import MIGRATIONS, Migration, validate_migrations
from pinpoint_scorecard_webhook.pg_store import table
from pinpoint_scorecard_webhook.poller import PollerLockLostError, poll_once
from pinpoint_scorecard_webhook.store import EventRow, StateStore

from .conftest import BASE, user_scorecard
from .test_persistence import legacy_db  # noqa: F401  (fixture)

DONE = "2026-11-24T16:03:10+00:00"
T1 = "2026-11-24T18:00:00+00:00"
SRC = Path(__file__).resolve().parents[1] / "src" / "pinpoint_scorecard_webhook"


def doc(scs):
    app = {"id": "100001", "type": "applications",
           "attributes": {"stage_id": 7, "updated_at": DONE},
           "relationships": {"user_scorecards": {
               "data": [{"type": "user_scorecards", "id": s["id"]} for s in scs]}}}
    return {"data": [app], "included": scs, "meta": {"stats": {"total": {"count": 1}}}}


@pytest.fixture
def store(tmp_path):
    with StateStore(tmp_path / "state.sqlite3") as s:
        yield s


def poll(settings, store, body):
    respx.reset()
    respx.get(f"{BASE}/applications").mock(return_value=httpx.Response(200, json=body))
    with PinpointClient(settings, sleep=lambda _: None) as client:
        return poll_once(client, store, page_size=10, max_pages=5)


# ------------------------------------------------------------------ policy


@pytest.mark.parametrize(
    ("event_type", "expected"),
    [
        ("scorecard.discovered", ("skipped", REASON_BASELINE_DISCOVERY)),
        ("scorecard.updated", ("skipped", REASON_NOT_ACTIONABLE)),
        ("scorecard.completed", ("pending", None)),
        ("scorecard.sentiment_changed", ("pending", None)),
        ("scorecard.left_complete", ("pending", None)),
    ],
)
def test_initial_processing(event_type, expected):
    assert initial_processing(event_type) == expected


def test_discovered_is_never_actionable():
    assert "scorecard.discovered" not in ACTIONABLE_EVENT_TYPES
    assert "scorecard.updated" not in ACTIONABLE_EVENT_TYPES


@respx.mock
def test_baseline_cannot_feed_automation_but_real_transitions_can(settings, store):
    incomplete = user_scorecard("500001", status="incomplete")
    complete_pos = user_scorecard("500002", status="complete", completed_at=DONE,
                                  sentiment="positive")
    poll(settings, store, doc([incomplete, complete_pos]))

    baseline = store.recent_events(limit=10)
    assert {(e.processing_status, e.processing_reason) for e in baseline} == {
        ("skipped", REASON_BASELINE_DISCOVERY)}
    assert store.actionable_events() == []

    done = user_scorecard("500001", status="complete", completed_at=T1, sentiment="negative",
                          updated_at=T1)
    touched = user_scorecard("500002", status="complete", completed_at=DONE,
                             sentiment="positive", updated_at=T1)
    poll(settings, store, doc([done, touched]))
    actionable = store.actionable_events()
    assert [(e.event_type, e.user_scorecard_id) for e in actionable] == [
        ("scorecard.completed", "500001")]
    updated = store.recent_events(limit=10, event_types=["scorecard.updated"])
    assert [(e.processing_status, e.processing_reason) for e in updated] == [
        ("skipped", REASON_NOT_ACTIONABLE)]


def test_skipped_requires_reason(store):
    store.start_run("a" * 32, DONE)
    row = EventRow("b" * 32, "1", "2", "scorecard.discovered", None, "complete", None, None,
                   DONE, None, "a" * 32, "skipped", None)
    with pytest.raises(sqlite3.IntegrityError), store.transaction() as conn:
        store.insert_event(conn, row)


def test_unknown_processing_status_rejected(store):
    store.start_run("a" * 32, DONE)
    row = EventRow("b" * 32, "1", "2", "scorecard.completed", None, "complete", None, None,
                   DONE, None, "a" * 32, "done", None)
    with pytest.raises(sqlite3.IntegrityError), store.transaction() as conn:
        store.insert_event(conn, row)


# ------------------------------------------------------- historical baseline


def test_imported_history_is_skipped_kept_and_not_processed(legacy_db, tmp_path):  # noqa: F811
    with StateStore(tmp_path / "target.sqlite3") as target:
        migrate_history(legacy_db, target)
        events = target.recent_events(limit=10)
        assert len(events) == 2  # still available for audit
        assert {(e.processing_status, e.processing_reason, e.processed_at) for e in events} == {
            ("skipped", REASON_HISTORICAL_BASELINE, None)}
        assert target.actionable_events() == []
        assert verify_history(legacy_db, target) == []


# --------------------------------------------------------------- lock safety


@respx.mock
def test_lost_lock_rolls_back_everything(settings, store, monkeypatch):
    monkeypatch.setattr(StateStore, "verify_poller_lock", lambda self: False)
    with pytest.raises(PollerLockLostError):
        poll(settings, store, doc([user_scorecard("500001")]))
    assert store.count_rows("detected_events") == 0
    assert store.count_rows("scorecard_state") == 0
    run = store._conn.execute("SELECT status, error FROM poll_runs").fetchone()
    assert run["status"] == "failed" and "PollerLockLostError" in run["error"]


# ------------------------------------------------------- schema isolation


def test_table_identifiers_are_schema_qualified():
    assert table("poll_runs").as_string(None) == '"pinpoint_scorecard_webhook"."poll_runs"'
    with pytest.raises(ValueError):
        table("pg_authid")


def test_pg_store_has_no_unqualified_table_references():
    source = (SRC / "pg_store.py").read_text()
    pattern = re.compile(
        r"\b(FROM|INTO|UPDATE|JOIN)\s+(poll_runs|scorecard_state|detected_events|"
        r"schema_migrations|v_actionable_events)\b", re.IGNORECASE)
    assert pattern.findall(source) == []


def test_v2_migration_qualifies_every_statement_and_widens_check_only():
    v2 = MIGRATIONS[1]
    assert v2.name == "event_processing_lifecycle"
    assert "'skipped'" in v2.sql and "historical_baseline" in v2.sql
    assert "DROP CONSTRAINT detected_events_processing_status_check" in v2.sql
    assert "v_actionable_events" in v2.sql and "scorecard.discovered'" not in v2.sql.split(
        "CREATE VIEW")[1]


@pytest.mark.parametrize(
    ("sql_text", "message"),
    [
        ("CREATE TABLE foo (a int)", "explicit schema"),
        ("DROP TABLE pinpoint_scorecard_webhook.poll_runs", "forbidden"),
        ("DELETE FROM pinpoint_scorecard_webhook.detected_events", "forbidden"),
        ("GRANT SELECT ON pinpoint_scorecard_webhook.poll_runs TO x", "forbidden"),
    ],
)
def test_validator_rejects_unsafe_v2_migrations(sql_text, message):
    with pytest.raises(ValueError, match=message):
        validate_migrations((MIGRATIONS[0], Migration(2, "bad", sql_text)))


def test_validator_allows_qualified_drop_constraint():
    validate_migrations((MIGRATIONS[0], Migration(
        2, "ok", "ALTER TABLE pinpoint_scorecard_webhook.t DROP CONSTRAINT c")))


# ---------------------------------------------------------- backend choice


def test_state_backend_postgres_selects_postgres_store(settings, monkeypatch):
    sentinel = object()
    from pinpoint_scorecard_webhook import pg_store

    monkeypatch.setattr(pg_store.PostgresStateStore, "from_settings",
                        classmethod(lambda cls, s: sentinel))
    pg_settings = settings.model_copy(update={"state_backend": "postgres"})
    assert db_commands.open_store(pg_settings, db_commands.backend_name(object(), pg_settings)) \
        is sentinel


def test_sqlite_describe_has_no_credentials(store):
    info = store.describe()
    assert info["backend"] == "sqlite" and set(info) == {"backend", "path"}
