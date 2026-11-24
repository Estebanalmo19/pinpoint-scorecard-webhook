from __future__ import annotations

import json

import httpx
import respx

from pinpoint_scorecard_webhook.cli import EXIT_CONFIG, EXIT_ERROR, EXIT_OK, main

from .conftest import API_KEY, BASE, application_doc, user_scorecard

DONE = "2026-11-24T12:00:00+00:00"


def run(argv, settings, capsys):
    code = main(argv, settings=settings)
    out = capsys.readouterr()
    assert API_KEY not in out.out + out.err
    return code, out.out, out.err


@respx.mock
def test_inspect_prints_only_requested_metadata(settings, capsys):
    doc = application_doc(scorecards=[user_scorecard("1", sentiment="yes")])
    doc["included"][0]["attributes"]["summary"] = "SECRET INTERVIEW NOTES"
    respx.get(f"{BASE}/applications/101").respond(json=doc)

    code, out, _ = run(["inspect"], settings, capsys)
    assert code == EXIT_OK
    for expected in ("Application ID", "101", "Candidate ID", "55", "Stage ID", "7", "User Scorecard ID"):
        assert expected in out
    assert "SECRET INTERVIEW NOTES" not in out


@respx.mock
def test_before_after_experiment_end_to_end(settings, capsys):
    route = respx.get(f"{BASE}/applications/101").mock(
        side_effect=[
            httpx.Response(200, json=application_doc(scorecards=[user_scorecard("1")])),
            httpx.Response(
                200,
                json=application_doc(
                    updated_at="2026-11-24T12:00:05+00:00",
                    scorecards=[user_scorecard("1", status="complete", completed_at=DONE)],
                ),
            ),
        ]
    )
    assert run(["snapshot-before"], settings, capsys)[0] == EXIT_OK
    code, out, _ = run(["snapshot-after"], settings, capsys)
    assert code == EXIT_OK and route.call_count == 2
    assert "[completed] user_scorecard 1" in out
    assert "[application_updated_at_changed]" in out

    snap_dir = settings.snapshot_dir
    diff = json.loads((snap_dir / "application-101-diff.json").read_text())
    assert {c["kind"] for c in diff["changes"]} >= {"completed", "application_updated_at_changed"}

    # Offline re-comparison works without network.
    code, out, _ = run(["compare"], settings, capsys)
    assert code == EXIT_OK and "[completed]" in out


@respx.mock
def test_snapshot_before_refuses_overwrite_without_force(settings, capsys):
    respx.get(f"{BASE}/applications/101").respond(json=application_doc())
    assert run(["snapshot-before"], settings, capsys)[0] == EXIT_OK
    assert run(["snapshot-before"], settings, capsys)[0] == EXIT_ERROR
    assert run(["snapshot-before", "--force"], settings, capsys)[0] == EXIT_OK


def test_snapshot_after_requires_before(settings, capsys):
    code, _, err = run(["snapshot-after"], settings, capsys)
    assert code == EXIT_ERROR and "snapshot-before" in err


@respx.mock
def test_api_errors_are_reported_without_secrets(settings, capsys):
    respx.get(f"{BASE}/applications/101").respond(
        401, json={"errors": [{"code": "unauthorized", "title": "Unauthorized"}]}
    )
    code, _, err = run(["inspect"], settings, capsys)
    assert code == EXIT_ERROR and "HTTP 401" in err


@respx.mock
def test_probe_handles_404_and_403(settings, capsys):
    respx.get(f"{BASE}/applications/101").respond(json=application_doc(scorecards=[user_scorecard("1")]))
    probe = respx.get(f"{BASE}/user_scorecards/1")

    probe.respond(404, json={"errors": [{"code": "not_found"}]})
    code, out, _ = run(["probe-user-scorecards"], settings, capsys)
    assert code == EXIT_OK and "not available" in out

    probe.respond(403, json={"errors": [{"code": "forbidden"}]})
    code, out, _ = run(["probe-user-scorecards"], settings, capsys)
    assert code == EXIT_OK and "forbidden" in out


@respx.mock
def test_application_id_override(settings, capsys):
    route = respx.get(f"{BASE}/applications/202").respond(json=application_doc(app_id="202"))
    assert run(["inspect", "--application-id", "202"], settings, capsys)[0] == EXIT_OK
    assert route.called


def test_config_errors_do_not_echo_values(monkeypatch, capsys, tmp_path):
    monkeypatch.chdir(tmp_path)  # no .env here
    monkeypatch.setenv("PINPOINT_SUBDOMAIN", "acme")
    monkeypatch.setenv("PINPOINT_API_KEY", API_KEY)
    monkeypatch.setenv("READ_ONLY", "false")
    code = main(["inspect"])
    out = capsys.readouterr()
    assert code == EXIT_CONFIG
    assert "READ_ONLY" in out.err and API_KEY not in out.err
