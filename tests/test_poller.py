from __future__ import annotations

import json

import httpx
import pytest
import respx

from pinpoint_scorecard_webhook.cli import EXIT_ERROR, EXIT_OK, main
from pinpoint_scorecard_webhook.client import PinpointClient
from pinpoint_scorecard_webhook.discovery import ScorecardRecord
from pinpoint_scorecard_webhook.poller import (
    EVENT_COMPLETED,
    EVENT_DISCOVERED,
    EVENT_LEFT_COMPLETE,
    EVENT_SENTIMENT_CHANGED,
    EVENT_UPDATED,
    derive_event_types,
    observation_hash,
    poll_once,
)
from pinpoint_scorecard_webhook.store import StateStore, StoredScorecard

from .conftest import API_KEY, BASE, user_scorecard

T0 = "2026-11-24T10:00:00+00:00"
T1 = "2026-11-24T11:00:00+00:00"
DONE = "2026-11-24T16:03:10+00:00"


# ------------------------------------------------------------------ helpers


def app(app_id, scorecards=(), *, loaded=True, extra_refs=()):
    rels = {}
    if loaded:
        refs = [{"type": "user_scorecards", "id": s["id"]} for s in scorecards]
        refs += [{"type": "user_scorecards", "id": i} for i in extra_refs]
        rels["user_scorecards"] = {"data": refs}
    return {"id": app_id, "type": "applications",
            "attributes": {"stage_id": 7, "updated_at": T0}, "relationships": rels}


def doc(*apps_with_scorecards, total=None):
    data, included = [], []
    for app_id, scs in apps_with_scorecards:
        data.append(app(app_id, scs))
        included.extend(scs)
    body = {"data": data, "included": included}
    if total is not None:
        body["meta"] = {"stats": {"total": {"count": total}}}
    return body


@pytest.fixture
def store(settings):
    with StateStore(settings.state_db_path) as s:
        yield s


@pytest.fixture
def settings(settings, tmp_path):
    return settings.model_copy(update={
        "state_db_path": tmp_path / "state" / "test.sqlite3",
        "pinpoint_page_size": 10,
    })


def poll(settings, store, responses, *, max_pages=5, page_size=10):
    side_effect = [r if isinstance(r, (httpx.Response, Exception)) else httpx.Response(200, json=r)
                   for r in responses]
    respx.get(f"{BASE}/applications").mock(side_effect=side_effect)
    with PinpointClient(settings, sleep=lambda _: None) as client:
        return poll_once(client, store, page_size=page_size, max_pages=max_pages)


def events(store, **kwargs):
    return store.recent_events(limit=100, **kwargs)


def state(store):
    return store.load_state()


INCOMPLETE = user_scorecard("10", status="incomplete", updated_at=T0)
COMPLETE_POS = user_scorecard("20", status="complete", completed_at=DONE, sentiment="positive",
                              updated_at=T0)


# --------------------------------------------------------------- pure rules


def rec(status, sentiment=None, updated=T0, completed=None):
    return ScorecardRecord("1", "10", status, updated, completed, sentiment)


def stored(status, sentiment=None, updated=T0, completed=None):
    r = rec(status, sentiment, updated, completed)
    return StoredScorecard("1", "10", status, sentiment, updated, completed, observation_hash(r))


@pytest.mark.parametrize(
    ("prev", "obs", "expected"),
    [
        (None, rec("complete", "positive", completed=DONE), [EVENT_DISCOVERED]),
        (None, rec("incomplete"), [EVENT_DISCOVERED]),
        (stored("incomplete"), rec("incomplete"), []),
        (stored("incomplete"), rec("complete", "positive", T1, DONE), [EVENT_COMPLETED]),
        (stored("incomplete"), rec("complete", None, T1, DONE), [EVENT_COMPLETED]),
        (stored("complete", "positive", completed=DONE), rec("incomplete", None, T1, DONE),
         [EVENT_LEFT_COMPLETE]),
        (stored("complete", "positive", completed=DONE), rec("complete", "negative", T1, DONE),
         [EVENT_SENTIMENT_CHANGED]),
        (stored("complete", None, completed=DONE), rec("complete", "positive", T1, DONE),
         [EVENT_SENTIMENT_CHANGED]),
        (stored("incomplete"), rec("incomplete", None, T1), [EVENT_UPDATED]),
        (stored("complete", "positive", completed=DONE), rec("complete", "positive", T1, T1),
         [EVENT_UPDATED]),
    ],
)
def test_derive_event_types(prev, obs, expected):
    assert derive_event_types(prev, obs) == expected


# ------------------------------------------------------------------ polling


@respx.mock
def test_baseline_emits_discovered_but_never_retroactive_completed(settings, store):
    null_sentiment = user_scorecard("30", status="complete", completed_at=DONE, sentiment=None)
    result = poll(settings, store, [doc(("1", [INCOMPLETE]), ("2", [COMPLETE_POS]),
                                        ("3", [null_sentiment]), ("4", []), total=4)])

    assert result.status == "success" and result.coverage == "complete"
    assert result.applications_inspected == 4 and result.scorecards_synced == 3
    assert result.events_by_type == {EVENT_DISCOVERED: 3}
    assert not events(store, event_types=[EVENT_COMPLETED])
    s = state(store)
    assert s[("3", "30")].status == "complete" and s[("3", "30")].sentiment is None


@respx.mock
def test_repeated_poll_is_idempotent(settings, store):
    body = doc(("1", [INCOMPLETE]), ("2", [COMPLETE_POS]), total=2)
    first = poll(settings, store, [body])
    respx.reset()
    second = poll(settings, store, [body])
    assert first.events_created == 2
    assert second.events_created == 0 and second.status == "success"
    assert store.count_rows("detected_events") == 2
    assert store.count_rows("scorecard_state") == 2
    assert store.count_rows("poll_runs") == 2


@respx.mock
def test_transition_to_complete_emits_completed_once(settings, store):
    poll(settings, store, [doc(("1", [INCOMPLETE]), total=1)])
    done = user_scorecard("10", status="complete", completed_at=DONE, sentiment="negative",
                          updated_at=T1)
    respx.reset()
    result = poll(settings, store, [doc(("1", [done]), total=1)])
    assert result.events_by_type == {EVENT_COMPLETED: 1}
    ev = events(store, event_types=[EVENT_COMPLETED])[0]
    assert (ev.previous_status, ev.new_status, ev.previous_sentiment, ev.new_sentiment) == (
        "incomplete", "complete", None, "negative")
    assert ev.processed_at is None
    respx.reset()
    assert poll(settings, store, [doc(("1", [done]), total=1)]).events_created == 0


@respx.mock
def test_sentiment_change_while_complete(settings, store):
    poll(settings, store, [doc(("2", [COMPLETE_POS]), total=1)])
    neg = user_scorecard("20", status="complete", completed_at=DONE, sentiment="negative",
                         updated_at=T1)
    respx.reset()
    result = poll(settings, store, [doc(("2", [neg]), total=1)])
    assert result.events_by_type == {EVENT_SENTIMENT_CHANGED: 1}
    ev = events(store, event_types=[EVENT_SENTIMENT_CHANGED])[0]
    assert (ev.previous_sentiment, ev.new_sentiment) == ("positive", "negative")


@respx.mock
def test_left_complete_then_completed_again(settings, store):
    poll(settings, store, [doc(("2", [COMPLETE_POS]), total=1)])
    edited = user_scorecard("20", status="incomplete", completed_at=DONE, sentiment=None,
                            updated_at=T1)
    respx.reset()
    assert poll(settings, store, [doc(("2", [edited]), total=1)]).events_by_type == {
        EVENT_LEFT_COMPLETE: 1}
    again = user_scorecard("20", status="complete", completed_at="2026-11-24T17:45:01+00:00",
                           sentiment="negative", updated_at="2026-11-24T17:45:10+00:00")
    respx.reset()
    assert poll(settings, store, [doc(("2", [again]), total=1)]).events_by_type == {
        EVENT_COMPLETED: 1}


@respx.mock
def test_disappearance_is_not_deletion(settings, store):
    poll(settings, store, [doc(("1", [INCOMPLETE]), ("2", [COMPLETE_POS]), total=2)])
    respx.reset()
    result = poll(settings, store, [doc(("1", [INCOMPLETE]), total=1)])
    assert result.events_created == 0
    assert result.scorecards_not_seen == 1
    assert any("not deleted" in r for r in result.reasons)
    assert ("2", "20") in state(store)  # kept
    # If it reappears unchanged, no event: it was never removed.
    respx.reset()
    assert poll(settings, store, [doc(("1", [INCOMPLETE]), ("2", [COMPLETE_POS]),
                                      total=2)]).events_created == 0


@respx.mock
def test_partial_coverage_is_flagged_and_keeps_unseen_state(settings, store):
    poll(settings, store, [doc(("1", [INCOMPLETE]), ("2", [COMPLETE_POS]), total=2)])
    respx.reset()
    full_page = doc(*[(str(i), []) for i in range(100, 110)])  # 10 rows, no total
    result = poll(settings, store, [full_page], max_pages=1)
    assert result.status == "partial" and result.coverage == "partial"
    assert any("partial coverage" in r for r in result.reasons)
    assert len(state(store)) == 2 and result.events_created == 0
    run = store.get_run(result.run_id)
    assert run["status"] == "partial" and "partial coverage" in run["reasons"]


@respx.mock
def test_relationship_unavailable_and_missing_included(settings, store):
    poll(settings, store, [doc(("1", [INCOMPLETE]), total=1)])
    body = {
        "data": [app("1", loaded=False), app("2", extra_refs=["99"])],
        "included": [],
        "meta": {"stats": {"total": {"count": 2}}},
    }
    respx.reset()
    result = poll(settings, store, [body])
    assert result.status == "partial"
    assert result.scorecards_skipped == 1 and result.events_created == 0
    assert ("2", "99") not in state(store)  # unknown attributes are not stored
    assert state(store)[("1", "10")].status == "incomplete"  # untouched
    assert result.scorecards_not_seen == 1  # app 1's scorecard: relationship unavailable
    assert [r.split(" ", 1)[1] for r in result.reasons] == [
        "application(s) without user_scorecards relationship",
        "application(s) with scorecards missing from included",
        "stored scorecard(s) not seen in this run (kept, not deleted)",
    ]


@respx.mock
def test_transient_failure_is_retried(settings, store):
    result = poll(settings, store, [httpx.Response(503), httpx.ConnectTimeout("t"),
                                    doc(("1", [INCOMPLETE]), total=1)])
    assert result.status == "success" and result.events_by_type == {EVENT_DISCOVERED: 1}


@respx.mock
def test_persistent_failure_writes_nothing_but_the_failed_run(settings, store):
    poll(settings, store, [doc(("1", [INCOMPLETE]), total=1)])
    respx.reset()
    result = poll(settings, store, [httpx.Response(503)] * 3)
    assert result.status == "failed" and "503" in result.error
    assert store.count_rows("detected_events") == 1
    assert state(store)[("1", "10")].status == "incomplete"
    assert store.get_run(result.run_id)["status"] == "failed"
    assert API_KEY not in result.error


@respx.mock
def test_failure_on_a_later_page_applies_nothing(settings, store):
    page1 = doc(*[(str(i), [user_scorecard(str(i + 1000))]) for i in range(10)])
    result = poll(settings, store, [page1, httpx.Response(401)])
    assert result.status == "failed"
    assert store.count_rows("scorecard_state") == 0 and store.count_rows("detected_events") == 0


@respx.mock
def test_apply_error_rolls_back_state_and_events(settings, store, monkeypatch):
    def boom(*_args, **_kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(StateStore, "upsert_state", staticmethod(boom))
    with pytest.raises(RuntimeError):
        poll(settings, store, [doc(("1", [INCOMPLETE]), total=1)])
    assert store.count_rows("detected_events") == 0
    runs = store._conn.execute("SELECT status, error FROM poll_runs").fetchall()
    assert [tuple(r) for r in runs] == [("failed", "apply failed: RuntimeError")]


def test_event_insert_is_idempotent_on_event_id(store):
    from pinpoint_scorecard_webhook.store import EventRow

    store.start_run("r1", T0)
    row = EventRow("abc", "1", "10", EVENT_COMPLETED, "incomplete", "complete", None, None,
                   T0, None, "r1")
    with store.transaction() as conn:
        assert store.insert_event(conn, row) is True
        assert store.insert_event(conn, row) is False
    assert store.count_rows("detected_events") == 1


def test_schema_contains_required_columns(store):
    cols = {r[1] for r in store._conn.execute("PRAGMA table_info(detected_events)")}
    assert {"event_id", "application_id", "user_scorecard_id", "event_type", "previous_status",
            "new_status", "previous_sentiment", "new_sentiment", "detected_at",
            "processed_at"} <= cols
    cols = {r[1] for r in store._conn.execute("PRAGMA table_info(scorecard_state)")}
    assert {"application_id", "user_scorecard_id", "status", "sentiment", "updated_at",
            "completed_at", "last_seen_at", "observation_hash"} <= cols


# ---------------------------------------------------------------------- CLI


def cli(argv, settings, capsys):
    code = main(argv, settings=settings)
    out = capsys.readouterr()
    assert API_KEY not in out.out + out.err
    return code, out.out, out.err


@respx.mock
def test_poll_once_and_events_cli(settings, capsys):
    respx.get(f"{BASE}/applications").respond(json=doc(("1", [INCOMPLETE]), total=1))
    code, out, _ = cli(["poll-once"], settings, capsys)
    assert code == EXIT_OK
    assert "Applications inspected   : 1" in out and "scorecard.discovered=1" in out

    code, out, _ = cli(["events", "--type", EVENT_DISCOVERED], settings, capsys)
    assert code == EXIT_OK and "scorecard.discovered" in out and "10" in out
    code, out, _ = cli(["events", "--type", EVENT_COMPLETED], settings, capsys)
    assert "No events." in out


@respx.mock
def test_poll_once_cli_failure_exit_code(settings, capsys):
    respx.get(f"{BASE}/applications").respond(
        403, json={"errors": [{"code": "forbidden", "title": "Forbidden"}]})
    code, out, _ = cli(["poll-once"], settings, capsys)
    assert code == EXIT_ERROR and "failed" in out


def test_events_without_db(settings, capsys):
    code, _, err = cli(["events"], settings, capsys)
    assert code == EXIT_ERROR and "poll-once" in err


@respx.mock
def test_state_db_contains_no_personal_data(settings, store):
    sc = user_scorecard("10", status="complete", completed_at=DONE, sentiment="positive")
    sc["attributes"]["summary"] = "INTERVIEW NOTES"
    body = doc(("1", [sc]), total=1)
    body["data"][0]["attributes"]["email"] = "person@example.com"
    poll(settings, store, [body])
    dump = "\n".join(store._conn.iterdump())
    assert "INTERVIEW NOTES" not in dump and "person@example.com" not in dump
    assert json.dumps(dump)  # sanity


# ------------------------------------------------- repeated-change idempotency


def _sc_edited(sentiment, updated_at):
    return user_scorecard("500001", status="complete", completed_at=DONE,
                          sentiment=sentiment, updated_at=updated_at)


def _poll_sequence(settings, store, observations):
    results = []
    for obs in observations:
        respx.reset()
        results.append(poll(settings, store, [doc(("100001", [obs]), total=1)]))
    return results


@respx.mock
def test_flip_flop_with_new_updated_at_each_edit(settings, store):
    """Realistic case: every edit in Pinpoint bumps updated_at."""
    seq = [
        _sc_edited("positive", "2026-11-24T17:55:00+00:00"),
        _sc_edited("negative", "2026-11-24T18:00:56+00:00"),
        _sc_edited("negative", "2026-11-24T18:00:56+00:00"),  # repeat, no change
        _sc_edited("positive", "2026-11-24T18:10:00+00:00"),
        _sc_edited("positive", "2026-11-24T18:10:00+00:00"),  # repeat, no change
        _sc_edited("negative", "2026-11-24T18:20:00+00:00"),
        _sc_edited("negative", "2026-11-24T18:20:00+00:00"),  # repeat, no change
    ]
    results = _poll_sequence(settings, store, seq)
    assert [r.events_created for r in results] == [1, 1, 0, 1, 0, 1, 0]
    changes = events(store, event_types=[EVENT_SENTIMENT_CHANGED])
    assert len(changes) == 3 and len({e.event_id for e in changes}) == 3
    assert [(e.previous_sentiment, e.new_sentiment) for e in reversed(changes)] == [
        ("positive", "negative"), ("negative", "positive"), ("positive", "negative")]


@respx.mock
def test_flip_flop_with_identical_observations_is_not_dropped(settings, store):
    """Worst case: the API returns byte-identical observations again (same updated_at).

    The 3rd transition repeats the exact (previous, new) pair of the 1st one and
    must still be recorded as a distinct event.
    """
    pos = _sc_edited("positive", T0)
    neg = _sc_edited("negative", T0)
    results = _poll_sequence(settings, store, [pos, neg, neg, pos, pos, neg, neg])
    assert [r.events_created for r in results] == [1, 1, 0, 1, 0, 1, 0]
    changes = events(store, event_types=[EVENT_SENTIMENT_CHANGED])
    assert len(changes) == 3 and len({e.event_id for e in changes}) == 3
    assert state(store)[("100001", "500001")].sentiment == "negative"


def test_event_id_is_stable_for_replays_and_unique_per_transition():
    from pinpoint_scorecard_webhook.poller import event_id

    a = event_id("1", "10", EVENT_SENTIMENT_CHANGED, "hA", "hB", "2026-11-24T17:55:00+00:00")
    assert a == event_id("1", "10", EVENT_SENTIMENT_CHANGED, "hA", "hB", "2026-11-24T17:55:00+00:00")
    assert a != event_id("1", "10", EVENT_SENTIMENT_CHANGED, "hA", "hB", "2026-11-24T18:10:00+00:00")
    assert len(a) == 32
