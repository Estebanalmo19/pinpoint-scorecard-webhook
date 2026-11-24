from __future__ import annotations

import dataclasses
import json

import httpx
import pytest
import respx

from pinpoint_scorecard_webhook.cli import EXIT_ERROR, EXIT_OK, main
from pinpoint_scorecard_webhook.client import PinpointClient
from pinpoint_scorecard_webhook.discovery import (
    APP_ALL_COMPLETED,
    APP_HAS_PENDING,
    APP_HAS_UNKNOWN_STATUS,
    APP_INCOMPLETE_RESPONSE,
    APP_NO_SCORECARDS,
    APP_RELATIONSHIP_UNAVAILABLE,
    DiscoveryRun,
    diff_runs,
    list_runs,
    load_run,
    run_discovery,
    save_run,
    summarize,
)

from .conftest import API_KEY, BASE, user_scorecard

DONE = "2026-11-24T16:03:10+00:00"


def app(app_id, scorecards=None, *, loaded=True, extra_refs=(), stage=7,
        updated="2026-11-24T10:00:00+00:00"):
    resource = {
        "id": app_id,
        "type": "applications",
        "attributes": {"stage_id": stage, "updated_at": updated},
        "relationships": {},
    }
    if loaded:
        refs = [{"type": "user_scorecards", "id": s["id"]} for s in scorecards or []]
        refs += [{"type": "user_scorecards", "id": i} for i in extra_refs]
        resource["relationships"]["user_scorecards"] = {"data": refs}
    else:
        resource["relationships"]["user_scorecards"] = {"meta": {"included": False}}
    return resource


def page(apps, scorecards=(), **extra):
    return {"data": apps, "included": list(scorecards), **extra}


def run_with(settings, responses, **kwargs):
    respx.get(f"{BASE}/applications").mock(side_effect=[httpx.Response(200, json=r) for r in responses])
    with PinpointClient(settings, sleep=lambda _: None) as client:
        return run_discovery(client, **{"page_size": 2, "max_pages": 5, **kwargs})


def make_run(scorecards, apps=None, coverage="complete", scope=None):
    from pinpoint_scorecard_webhook.discovery import ApplicationRecord, ScorecardRecord

    sc_records = [ScorecardRecord(*s) if isinstance(s, tuple) else s for s in scorecards]
    if apps is None:
        ids = sorted({s.application_id for s in sc_records})
        apps = [
            ApplicationRecord(i, "7", "2026-11-24T10:00:00+00:00", APP_HAS_PENDING,
                              tuple(s.user_scorecard_id for s in sc_records if s.application_id == i))
            for i in ids
        ]
    return DiscoveryRun(scope=scope or {"page_size": 10}, applications=apps,
                        scorecards=sc_records, coverage=coverage, end_reason="short_page")


# ---------------------------------------------------------------- discovery


@respx.mock
def test_discovery_pages_classifies_and_requests_minimal_fields(settings):
    sc_pending = user_scorecard("11", status="incomplete")
    sc_done = user_scorecard("12", status="complete", completed_at=DONE, sentiment="positive")
    sc_edited = user_scorecard("13", status="incomplete", completed_at=DONE)
    sc_weird = user_scorecard("14", status="draft")
    responses = [
        page([app("1"), app("2", [sc_pending])], [sc_pending]),
        page([app("3", [sc_done]), app("4", loaded=False)], [sc_done]),
        page([app("5", extra_refs=["99"]), app("6", [sc_edited, sc_weird])], [sc_edited, sc_weird]),
        page([]),
    ]
    run = run_with(settings, responses)

    classes = {a.application_id: a.classification for a in run.applications}
    assert classes == {
        "1": APP_NO_SCORECARDS,
        "2": APP_HAS_PENDING,
        "3": APP_ALL_COMPLETED,
        "4": APP_RELATIONSHIP_UNAVAILABLE,
        "5": APP_INCOMPLETE_RESPONSE,
        "6": APP_HAS_PENDING,
    }
    assert run.coverage == "complete" and run.end_reason == "empty_page" and run.pages_fetched == 4

    params = respx.calls[0].request.url.params
    assert params["include"] == "user_scorecards"
    assert params["sort"] == "id"
    assert params["stats[total]"] == "count"
    assert params["fields[applications]"] == "stage_id,updated_at,user_scorecards"
    assert params["fields[user_scorecards]"] == "status,completed_at,updated_at,sentiment"
    assert "filter[job_visibility]" not in params

    edited = next(s for s in run.scorecards if s.user_scorecard_id == "13")
    assert edited.state == "pending" and edited.discrepancy == "not_complete_with_completed_at"
    weird = next(s for s in run.scorecards if s.user_scorecard_id == "14")
    assert weird.state == "unknown"


@respx.mock
def test_unknown_status_only_classification(settings):
    sc = user_scorecard("1", status="draft")
    run = run_with(settings, [page([app("1", [sc])], [sc])])
    assert run.applications[0].classification == APP_HAS_UNKNOWN_STATUS


@respx.mock
def test_partial_coverage_when_max_pages_reached(settings):
    full = page([app("1"), app("2")])
    run = run_with(settings, [full, page([app("3"), app("4")])], max_pages=2)
    assert run.coverage == "partial" and run.end_reason is None and run.pages_fetched == 2


@respx.mock
def test_duplicate_rows_across_pages_are_deduplicated(settings):
    run = run_with(settings, [page([app("1"), app("2")]), page([app("2")])])
    assert [a.application_id for a in run.applications] == ["1", "2"]
    assert run.duplicate_application_rows == 1


@respx.mock
def test_include_flags_add_documented_filters(settings):
    run_with(settings, [page([])], include_confidential=True, include_sourced=True)
    params = respx.calls[0].request.url.params
    assert params["filter[job_visibility]"] == "internal,external,confidential,private_job"
    assert "sourced" in params["filter[stage_basic_type]"].split(",")


@respx.mock
def test_summary_and_saved_file_contain_no_personal_data(settings):
    sc = user_scorecard("12", status="complete", completed_at=DONE, sentiment="positive")
    sc["attributes"]["summary"] = "INTERVIEW NOTES"
    resource = app("3", [sc])
    resource["attributes"]["email"] = "person@example.com"
    run = run_with(settings, [page([resource], [sc], meta={"stats": {"total": {"count": 1}}})])

    summary = summarize(run)
    assert summary["applications_reviewed"] == 1
    assert summary["user_scorecards_found"] == 1
    assert summary["scorecards_by_status"] == {"complete": 1}
    assert summary["scorecards_by_sentiment"] == {"positive": 1}
    assert summary["total_reported_by_api"] == 1

    path = save_run(run, settings.snapshot_dir)
    text = path.read_text()
    assert "INTERVIEW NOTES" not in text and "person@example.com" not in text
    assert load_run(path).to_dict() == run.to_dict()
    assert list_runs(settings.snapshot_dir) == [path]


# --------------------------------------------------------------------- diff


def sc(app_id, sc_id, status="incomplete", updated="2026-11-24T10:00:00+00:00",
       completed=None, sentiment=None, loaded=True):
    from pinpoint_scorecard_webhook.discovery import ScorecardRecord

    return ScorecardRecord(app_id, sc_id, status, updated, completed, sentiment, loaded)


def kinds(changes):
    return [(c.kind, c.application_id, c.user_scorecard_id) for c in changes]


def test_diff_new_scorecard_and_completion():
    before = make_run([sc("1", "10")])
    after = make_run([
        sc("1", "10", "complete", "2026-11-24T11:00:00+00:00", DONE, "positive"),
        sc("1", "20"),
    ])
    assert kinds(diff_runs(before, after)) == [
        ("scorecard_new", "1", "20"),
        ("became_complete", "1", "10"),
        ("status_changed", "1", "10"),
        ("sentiment_changed", "1", "10"),
        ("updated_at_changed", "1", "10"),
        ("completed_at_changed", "1", "10"),
    ]


def test_diff_edited_scorecard_observed_live():
    before = make_run([sc("100001", "500001", "complete", "2026-11-24T16:03:17+00:00",
                          DONE, "positive")])
    after = make_run([sc("100001", "500001", "incomplete", "2026-11-24T17:32:03+00:00",
                         DONE, None)])
    changes = diff_runs(before, after)
    assert [c.kind for c in changes] == [
        "left_complete", "status_changed", "sentiment_changed", "updated_at_changed",
        "status_completed_at_discrepancy",
    ]
    assert "not inferable" in changes[0].note
    assert not any(c.kind == "completed_at_changed" for c in changes)


def test_diff_not_seen_is_never_reported_as_deleted():
    from pinpoint_scorecard_webhook.discovery import ApplicationRecord

    before = make_run([sc("1", "10"), sc("2", "20"), sc("3", "30")])
    after_apps = [
        ApplicationRecord("1", "7", "2026-11-24T10:00:00+00:00", APP_NO_SCORECARDS, ()),
        ApplicationRecord("3", "7", "2026-11-24T10:00:00+00:00", APP_RELATIONSHIP_UNAVAILABLE, ()),
    ]
    after = make_run([], apps=after_apps)
    changes = diff_runs(before, after)
    notes = {(c.kind, c.application_id): c.note for c in changes}
    assert ("application_not_seen", "2") in notes
    assert "not assumed deleted" in notes[("scorecard_not_seen", "1")]
    assert "not present" in notes[("scorecard_not_seen", "2")]
    assert "unavailable" in notes[("scorecard_not_seen", "3")]
    assert not any("deleted" == c.kind for c in changes)


def test_diff_application_level_changes():
    from pinpoint_scorecard_webhook.discovery import ApplicationRecord

    b_app = ApplicationRecord("1", "7", "2026-11-24T10:00:00+00:00", APP_NO_SCORECARDS, ())
    a_app = dataclasses.replace(b_app, stage_id="8", updated_at="2026-11-24T12:00:00+00:00")
    new_app = ApplicationRecord("2", "7", None, APP_NO_SCORECARDS, ())
    changes = diff_runs(make_run([], apps=[b_app]), make_run([], apps=[a_app, new_app]))
    assert [c.kind for c in changes] == [
        "application_new", "application_updated_at_changed", "application_stage_changed",
    ]


def test_diff_warns_on_partial_coverage_and_scope_mismatch():
    before = make_run([sc("1", "10")], coverage="partial", scope={"page_size": 10})
    after = make_run([sc("1", "10")], scope={"page_size": 20})
    assert [c.kind for c in diff_runs(before, after)] == [
        "warning_scope_mismatch", "warning_partial_coverage",
    ]


def test_diff_unverifiable_when_attributes_missing():
    before = make_run([sc("1", "10")])
    after = make_run([sc("1", "10", None, None, None, None, loaded=False)])
    assert [c.kind for c in diff_runs(before, after)] == ["scorecard_unverifiable"]


def test_diff_identical_runs():
    run = make_run([sc("1", "10", "complete", completed=DONE)])
    assert diff_runs(run, run) == []


# ---------------------------------------------------------------------- CLI


def cli(argv, settings, capsys):
    code = main(argv, settings=settings)
    out = capsys.readouterr()
    assert API_KEY not in out.out + out.err
    return code, out.out, out.err


def test_discover_requires_explicit_limit(settings, capsys):
    with pytest.raises(SystemExit):
        main(["discover"], settings=settings)
    capsys.readouterr()


def test_discover_rejects_limit_above_ceiling(settings, capsys):
    code, _, err = cli(["discover", "--max-pages", "50"], settings, capsys)
    assert code == EXIT_ERROR and "PINPOINT_MAX_PAGES" in err


@respx.mock
def test_discover_cli_prints_rows_summary_and_partial_warning(settings, capsys):
    sc_done = user_scorecard("12", status="complete", completed_at=DONE, sentiment="positive")
    respx.get(f"{BASE}/applications").mock(side_effect=[
        httpx.Response(200, json=page([app("1"), app("3", [sc_done])], [sc_done])),
        httpx.Response(200, json=page([app("4"), app("5")])),
    ])
    code, out, _ = cli(["discover", "--max-pages", "2", "--page-size", "2"], settings, capsys)
    assert code == EXIT_OK
    assert "application_id" in out and "12" in out and "positive" in out
    assert "applications_reviewed" in out and "Coverage PARTIAL" in out
    assert len(list_runs(settings.snapshot_dir)) == 1


@respx.mock
def test_discover_diff_cli(settings, capsys):
    before_sc = user_scorecard("12", status="incomplete")
    after_sc = user_scorecard("12", status="complete", completed_at=DONE, sentiment="positive")
    respx.get(f"{BASE}/applications").mock(side_effect=[
        httpx.Response(200, json=page([app("3", [before_sc])], [before_sc])),
        httpx.Response(200, json=page([app("3", [after_sc])], [after_sc])),
    ])
    assert cli(["discover", "--all", "--page-size", "2"], settings, capsys)[0] == EXIT_OK
    assert cli(["discover", "--all", "--page-size", "2"], settings, capsys)[0] == EXIT_OK
    code, out, _ = cli(["discover-diff"], settings, capsys)
    assert code == EXIT_OK
    assert "[became_complete] app 3 / user_scorecard 12" in out


def test_discover_diff_needs_two_runs(settings, capsys):
    code, _, err = cli(["discover-diff"], settings, capsys)
    assert code == EXIT_ERROR and "two saved discovery runs" in err


def test_discover_does_not_need_application_id(settings):
    settings = settings.model_copy(update={"application_id": None})
    assert settings.application_id is None
    json.dumps(summarize(make_run([])))  # summary is JSON-serialisable
