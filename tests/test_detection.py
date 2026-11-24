from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from pinpoint_scorecard_webhook.client import PinpointClient
from pinpoint_scorecard_webhook.detection import (
    DETECTION_NEW_AFTER_BASELINE,
    REASON_BASELINE_DISCOVERY,
    REASON_DISCOVERY_INCOMPLETE,
    REASON_REVIEW_COMPLETED_BEFORE_WATERMARK,
    REASON_REVIEW_NO_WATERMARK,
    REASON_REVIEW_OBSERVATION_GAP,
    REASON_REVIEW_SCOPE_CHANGED,
    REASON_REVIEW_UNKNOWN_COMPLETION_TIME,
    DetectionContext,
    DetectionPolicy,
    canonical_scope,
    classify_first_sighting,
    scope_key,
)
from pinpoint_scorecard_webhook.discovery import ScorecardRecord, build_filters
from pinpoint_scorecard_webhook.history_migration import migrate_history
from pinpoint_scorecard_webhook.pg_migrations import MIGRATIONS
from pinpoint_scorecard_webhook.poller import poll_once
from pinpoint_scorecard_webhook.store import StateStore

from .conftest import BASE, user_scorecard
from .test_persistence import legacy_db  # noqa: F401  (fixture)

T0 = datetime(2026, 12, 1, 9, 0, tzinfo=UTC)
DEFAULT_KEY = scope_key(canonical_scope({}))


def iso(dt: datetime) -> str:
    return dt.isoformat()


def ctx(*, baseline=True, any_baseline=True, watermark: datetime | None = T0) -> DetectionContext:
    return DetectionContext(
        scope_key=DEFAULT_KEY,
        baseline_run_id="b" * 32 if baseline else None,
        any_baseline=any_baseline,
        watermark_run_id="w" * 32 if watermark else None,
        watermark_at=watermark,
    )


def rec(status="complete", completed: datetime | None = T0 + timedelta(minutes=3)):
    return ScorecardRecord("100001", "500001", status, iso(T0), iso(completed) if completed else None,
                           "positive")


# --------------------------------------------------------------- classifier


@pytest.mark.parametrize(
    ("record", "context", "expected"),
    [
        (rec(), ctx(baseline=False, any_baseline=False), ("scorecard.discovered", REASON_BASELINE_DISCOVERY)),
        (rec("incomplete", None), ctx(baseline=False, any_baseline=False),
         ("scorecard.discovered", REASON_BASELINE_DISCOVERY)),
        (rec("incomplete", None), ctx(), ("scorecard.discovered", REASON_DISCOVERY_INCOMPLETE)),
        (rec(), ctx(baseline=False, any_baseline=True), ("scorecard.discovered", REASON_REVIEW_SCOPE_CHANGED)),
        (rec(), ctx(watermark=None), ("scorecard.discovered", REASON_REVIEW_NO_WATERMARK)),
        (rec(completed=None), ctx(), ("scorecard.discovered", REASON_REVIEW_UNKNOWN_COMPLETION_TIME)),
        (rec(completed=T0 - timedelta(hours=2)), ctx(),
         ("scorecard.discovered", REASON_REVIEW_COMPLETED_BEFORE_WATERMARK)),
        (rec(completed=T0 - timedelta(minutes=2)), ctx(), ("scorecard.completed", None)),  # tolerance
        (rec(), ctx(), ("scorecard.completed", None)),
    ],
)
def test_classify_first_sighting(record, context, expected):
    result = classify_first_sighting(record, context, iso(T0 + timedelta(minutes=10)))
    assert (result.event_type, result.processing_reason) == expected
    assert result.processing_status == ("pending" if expected[0] == "scorecard.completed" else "skipped")
    assert result.evidence["scope_key"] == DEFAULT_KEY and "decision" in result.evidence


def test_observation_gap_forces_review():
    result = classify_first_sighting(rec(completed=T0 + timedelta(hours=29)), ctx(),
                                     iso(T0 + timedelta(hours=30)))
    assert result.processing_reason == REASON_REVIEW_OBSERVATION_GAP
    custom = DetectionPolicy(max_observation_gap=timedelta(hours=48))
    assert classify_first_sighting(rec(completed=T0 + timedelta(hours=29)), ctx(),
                                   iso(T0 + timedelta(hours=30)), custom).event_type == "scorecard.completed"


def test_scope_key_ignores_order_and_paging():
    a = canonical_scope({"job_visibility": "x", "stage_basic_type": "y"})
    b = canonical_scope({"stage_basic_type": "y", "job_visibility": "x"})
    assert scope_key(a) == scope_key(b) != DEFAULT_KEY


# --------------------------------------------------------------- end to end


def app(app_id, scs):
    return {"id": app_id, "type": "applications",
            "attributes": {"stage_id": 7, "updated_at": iso(T0)},
            "relationships": {"user_scorecards": {
                "data": [{"type": "user_scorecards", "id": s["id"]} for s in scs]}}}


def doc(*apps, total=None):
    data, included = [], []
    for app_id, scs in apps:
        data.append(app(app_id, scs))
        included.extend(scs)
    return {"data": data, "included": included,
            "meta": {"stats": {"total": {"count": len(data) if total is None else total}}}}


def sc(sc_id, status="complete", completed: datetime | None = None, sentiment="positive"):
    return user_scorecard(sc_id, status=status, completed_at=iso(completed) if completed else None,
                          sentiment=sentiment if status == "complete" else None,
                          updated_at=iso(completed or T0))


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "state.sqlite3"


@pytest.fixture
def store(db_path):
    with StateStore(db_path) as s:
        yield s


def poll(settings, store, responses, at: datetime, *, max_pages=5, **kwargs):
    respx.reset()
    if not isinstance(responses, list):
        responses = [responses]
    respx.get(f"{BASE}/applications").mock(side_effect=[
        r if isinstance(r, httpx.Response) else httpx.Response(200, json=r) for r in responses])
    with PinpointClient(settings, sleep=lambda _: None) as client:
        return poll_once(client, store, page_size=10, max_pages=max_pages,
                         now=lambda: iso(at), **kwargs)


def events_of(store, sc_id):
    return [e for e in store.recent_events(limit=100) if e.user_scorecard_id == sc_id]


def baselines(store):
    return [dict(r) for r in store._conn.execute("SELECT * FROM monitoring_baselines")]


@respx.mock
def test_initial_baseline_is_never_actionable(settings, store):
    r = poll(settings, store, doc(("1", [sc("10", completed=T0)]), ("2", [sc("20", "incomplete")])), T0)
    assert r.baseline_established
    assert {(e.event_type, e.processing_reason) for e in store.recent_events(limit=10)} == {
        ("scorecard.discovered", REASON_BASELINE_DISCOVERY)}
    assert store.actionable_events() == []
    assert baselines(store)[0]["baseline_run_id"] == r.run_id


@respx.mock
def test_new_completed_scorecard_after_baseline_is_actionable_once(settings, store):
    poll(settings, store, doc(("1", [])), T0)  # app 1 had no scorecards
    body = doc(("1", [sc("10", completed=T0 + timedelta(minutes=5))]))
    r = poll(settings, store, body, T0 + timedelta(minutes=10))
    assert r.events_by_type == {"scorecard.completed": 1}
    [event] = store.actionable_events()
    assert (event.user_scorecard_id, event.previous_status, event.new_status) == ("10", None, "complete")
    assert event.evidence["decision"] == DETECTION_NEW_AFTER_BASELINE
    assert event.evidence["watermark_run_id"] is not None
    # Idempotent: same data, no duplicate.
    assert poll(settings, store, body, T0 + timedelta(minutes=20)).events_created == 0
    assert len(store.actionable_events()) == 1


@respx.mock
def test_old_completion_first_seen_after_baseline_goes_to_review(settings, store):
    poll(settings, store, doc(("1", [])), T0)
    r = poll(settings, store, doc(("1", [sc("10", completed=T0 - timedelta(days=3))])),
             T0 + timedelta(minutes=10))
    assert r.skipped_by_reason == {REASON_REVIEW_COMPLETED_BEFORE_WATERMARK: 1}
    assert store.actionable_events() == []
    [review] = store.events_needing_review()
    assert review.user_scorecard_id == "10" and review.evidence["completed_at"]
    assert any("review" in reason for reason in r.reasons)


@respx.mock
def test_unknown_completion_time_goes_to_review(settings, store):
    poll(settings, store, doc(("1", [])), T0)
    r = poll(settings, store, doc(("1", [sc("10", completed=None)])), T0 + timedelta(minutes=10))
    assert r.skipped_by_reason == {REASON_REVIEW_UNKNOWN_COMPLETION_TIME: 1}


@respx.mock
def test_new_incomplete_then_completed(settings, store):
    poll(settings, store, doc(("1", [])), T0)
    r1 = poll(settings, store, doc(("1", [sc("10", "incomplete")])), T0 + timedelta(minutes=10))
    assert r1.skipped_by_reason == {REASON_DISCOVERY_INCOMPLETE: 1}
    assert store.actionable_events() == []
    r2 = poll(settings, store, doc(("1", [sc("10", completed=T0 + timedelta(minutes=15))])),
              T0 + timedelta(minutes=20))
    assert r2.events_by_type == {"scorecard.completed": 1}
    [event] = store.actionable_events()
    assert event.previous_status == "incomplete" and event.evidence is None  # a transition


@respx.mock
def test_baseline_scorecard_stays_excluded_but_real_transitions_count(settings, store):
    poll(settings, store, doc(("1", [sc("10", "incomplete")])), T0)
    assert store.actionable_events() == []
    poll(settings, store, doc(("1", [sc("10", completed=T0 + timedelta(hours=1))])),
         T0 + timedelta(hours=2))
    assert [e.event_type for e in store.actionable_events()] == ["scorecard.completed"]
    discovered = [e for e in events_of(store, "10") if e.event_type == "scorecard.discovered"]
    assert [(e.processing_status, e.processing_reason) for e in discovered] == [
        ("skipped", REASON_BASELINE_DISCOVERY)]


@respx.mock
def test_widened_scope_requires_its_own_baseline(settings, store):
    poll(settings, store, doc(("1", [])), T0)  # default scope baseline
    widened = {"include_confidential": True}
    r = poll(settings, store, doc(("1", []), ("9", [sc("90", completed=T0 + timedelta(minutes=1)),
                                                    sc("91", "incomplete")])),
             T0 + timedelta(minutes=10), **widened)
    assert r.skipped_by_reason == {REASON_REVIEW_SCOPE_CHANGED: 1, REASON_BASELINE_DISCOVERY: 1}
    assert store.actionable_events() == [] and r.baseline_established
    assert len(baselines(store)) == 2
    r2 = poll(settings, store, doc(("1", []), ("9", [sc("90", completed=T0 + timedelta(minutes=1)),
                                                     sc("91", "incomplete")]),
                                   ("8", [sc("80", completed=T0 + timedelta(minutes=15))])),
              T0 + timedelta(minutes=20), **widened)
    assert r2.events_by_type == {"scorecard.completed": 1}
    assert r2.scope_key == scope_key(canonical_scope(build_filters(True, False)))


@respx.mock
def test_partial_first_run_does_not_establish_baseline(settings, store):
    full_page = doc(*[(str(i), [sc(str(i * 10), completed=T0)]) for i in range(1, 11)], total=20)
    r1 = poll(settings, store, full_page, T0, max_pages=1)
    assert r1.status == "partial" and not r1.baseline_established and baselines(store) == []
    # Next complete run is still baseline-building: a new complete scorecard is NOT actionable.
    r2 = poll(settings, store, doc(("50", [sc("500", completed=T0 + timedelta(minutes=5))])),
              T0 + timedelta(minutes=10))
    assert r2.baseline_established
    assert r2.skipped_by_reason == {REASON_BASELINE_DISCOVERY: 1}
    assert store.actionable_events() == []


@respx.mock
def test_partial_run_after_baseline_does_not_move_watermark(settings, store):
    poll(settings, store, doc(("1", [])), T0)  # watermark = T0
    full_page = doc(*[(str(i), []) for i in range(1, 11)], total=20)
    assert poll(settings, store, full_page, T0 + timedelta(hours=1), max_pages=1).status == "partial"
    # Completed between the full run (T0) and the partial run (T0+1h): still after our last FULL look.
    r = poll(settings, store, doc(("1", [sc("10", completed=T0 + timedelta(minutes=30))])),
             T0 + timedelta(hours=2))
    assert r.events_by_type == {"scorecard.completed": 1}
    assert store.actionable_events()[0].evidence["watermark_at"] == iso(T0)


@respx.mock
def test_interrupted_and_failed_runs_do_not_count_as_observations(settings, store, db_path):
    poll(settings, store, doc(("1", [])), T0)
    store.start_run("c" * 32, iso(T0 + timedelta(hours=1)), scope_key=DEFAULT_KEY,
                    scope=canonical_scope({}))  # crashed mid-run
    failed = poll(settings, store, [httpx.Response(503)] * 3, T0 + timedelta(hours=2))
    assert failed.status == "failed" and failed.interrupted_runs_recovered == 1
    store.close()
    # Service restart: new process, same database.
    with StateStore(db_path) as restarted:
        r = poll(settings, restarted, doc(("1", [sc("10", completed=T0 + timedelta(minutes=30))])),
                 T0 + timedelta(hours=3))
        assert r.events_by_type == {"scorecard.completed": 1}
        assert restarted.actionable_events()[0].evidence["watermark_at"] == iso(T0)
        assert len(baselines(restarted)) == 1


@respx.mock
def test_failed_first_run_establishes_nothing(settings, store):
    assert poll(settings, store, [httpx.Response(503)] * 3, T0).status == "failed"
    assert baselines(store) == []
    r = poll(settings, store, doc(("1", [sc("10", completed=T0)])), T0 + timedelta(minutes=5))
    assert r.baseline_established and r.skipped_by_reason == {REASON_BASELINE_DISCOVERY: 1}


@respx.mock
def test_long_downtime_sends_new_completions_to_review(settings, store):
    poll(settings, store, doc(("1", [])), T0)
    r = poll(settings, store, doc(("1", [sc("10", completed=T0 + timedelta(hours=29))])),
             T0 + timedelta(hours=30))
    assert r.skipped_by_reason == {REASON_REVIEW_OBSERVATION_GAP: 1}
    assert store.actionable_events() == []


@respx.mock
def test_review_event_is_not_reclassified_later(settings, store):
    poll(settings, store, doc(("1", [])), T0)
    body = doc(("1", [sc("10", completed=T0 - timedelta(days=1))]))
    poll(settings, store, body, T0 + timedelta(minutes=10))
    assert poll(settings, store, body, T0 + timedelta(minutes=20)).events_created == 0
    assert len(store.events_needing_review()) == 1 and store.actionable_events() == []


# ---------------------------------------------------------------- migration


def test_v3_migration_backfills_the_default_scope_key_and_touches_no_event():
    v3 = MIGRATIONS[2]
    assert v3.name == "monitoring_baseline_and_evidence"
    assert DEFAULT_KEY in v3.sql
    assert "UPDATE pinpoint_scorecard_webhook.detected_events" not in v3.sql


def test_imported_runs_get_the_default_scope(legacy_db, tmp_path):  # noqa: F811
    with StateStore(tmp_path / "target.sqlite3") as target:
        migrate_history(legacy_db, target)
        keys = {r[0] for r in target._conn.execute("SELECT scope_key FROM poll_runs")}
        assert keys == {DEFAULT_KEY}
