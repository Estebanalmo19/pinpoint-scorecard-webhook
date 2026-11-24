from __future__ import annotations

import dataclasses

import pytest

from pinpoint_scorecard_webhook.snapshot import (
    diff_snapshots,
    load_snapshot,
    save_snapshot,
    snapshot_from_document,
)

from .conftest import application_doc, user_scorecard

DONE = "2026-11-24T12:00:00+00:00"


def kinds(changes):
    return [(c.kind, c.user_scorecard_id) for c in changes]


# --------------------------------------------------------------- extraction


def test_extracts_only_technical_metadata():
    doc = application_doc(scorecards=[user_scorecard("2"), user_scorecard("1", status="complete", completed_at=DONE)])
    doc["included"][0]["attributes"]["summary"] = "free text that must not be stored"
    snap = snapshot_from_document(doc)

    assert (snap.application_id, snap.candidate_id, snap.stage_id) == ("101", "55", "7")
    assert snap.user_scorecards_loaded
    assert [s.id for s in snap.user_scorecards] == ["1", "2"]
    assert snap.user_scorecards[0].is_completed and not snap.user_scorecards[1].is_completed
    assert "free text" not in str(snap.to_dict())


def test_incomplete_scorecard_has_null_completed_at():
    snap = snapshot_from_document(application_doc(scorecards=[user_scorecard("1", status=None)]))
    sc = snap.user_scorecards[0]
    assert sc.completed_at is None and sc.status is None and not sc.is_completed


def test_missing_relationship_is_unknown_not_empty():
    snap = snapshot_from_document(application_doc(include_relationship=False))
    assert not snap.user_scorecards_loaded and snap.user_scorecards == ()


def test_relationships_key_absent():
    doc = application_doc()
    del doc["data"]["relationships"]
    snap = snapshot_from_document(doc)
    assert not snap.user_scorecards_loaded and snap.candidate_id == "55"


def test_reference_without_included_resource():
    snap = snapshot_from_document(application_doc(extra_refs=["9"]))
    assert snap.user_scorecards[0].id == "9" and not snap.user_scorecards[0].attributes_loaded


def test_candidate_id_falls_back_to_relationship():
    doc = application_doc()
    doc["data"]["attributes"]["candidate_id"] = None
    doc["data"]["relationships"]["candidate"]["data"]["id"] = "77"
    assert snapshot_from_document(doc).candidate_id == "77"


def test_concealed_candidate_is_none():
    doc = application_doc(candidate_id=None)
    doc["data"]["relationships"]["candidate"] = {"data": None}
    assert snapshot_from_document(doc).candidate_id is None


def test_rejects_non_application_document():
    with pytest.raises(ValueError):
        snapshot_from_document({"data": [{"type": "applications"}]})


def test_save_and_load_roundtrip(tmp_path):
    snap = snapshot_from_document(application_doc(scorecards=[user_scorecard("1")]))
    path = tmp_path / "s.json"
    save_snapshot(snap, path)
    assert load_snapshot(path) == snap


# --------------------------------------------------------------------- diff


def test_no_changes():
    doc = application_doc(scorecards=[user_scorecard("1")])
    assert diff_snapshots(snapshot_from_document(doc), snapshot_from_document(doc)) == []


def test_incomplete_to_completed_transition():
    before = snapshot_from_document(application_doc(scorecards=[user_scorecard("1")]))
    after = snapshot_from_document(
        application_doc(
            updated_at="2026-11-24T12:00:01+00:00",
            scorecards=[
                user_scorecard("1", status="complete", completed_at=DONE,
                               updated_at=DONE, sentiment="positive")
            ],
        )
    )
    changes = diff_snapshots(before, after)
    assert kinds(changes) == [
        ("application_updated_at_changed", None),
        ("completed", "1"),
        ("status_changed", "1"),
        ("sentiment_changed", "1"),
        ("completed_at_changed", "1"),
        ("updated_at_changed", "1"),
    ]


def test_completion_without_application_updated_at_change_is_visible():
    before = snapshot_from_document(application_doc(scorecards=[user_scorecard("1")]))
    after = snapshot_from_document(
        application_doc(scorecards=[user_scorecard("1", status="complete", completed_at=DONE)])
    )
    changes = diff_snapshots(before, after)
    assert ("completed", "1") in kinds(changes)
    assert not any(c.kind == "application_updated_at_changed" for c in changes)


def test_new_scorecard_already_completed_and_removed_scorecard():
    before = snapshot_from_document(application_doc(scorecards=[user_scorecard("1")]))
    after = snapshot_from_document(
        application_doc(scorecards=[user_scorecard("2", status="complete", completed_at=DONE)])
    )
    assert kinds(diff_snapshots(before, after)) == [
        ("user_scorecard_added", "2"),
        ("completed", "2"),
        ("user_scorecard_removed", "1"),
    ]


def test_edited_scorecard_leaves_complete_but_keeps_completed_at():
    # Observed live: complete/positive -> incomplete/null with completed_at unchanged.
    before = snapshot_from_document(application_doc(scorecards=[
        user_scorecard("1", status="complete", completed_at=DONE, sentiment="positive")]))
    after = snapshot_from_document(application_doc(scorecards=[
        user_scorecard("1", status="incomplete", completed_at=DONE,
                       updated_at="2026-11-24T17:32:03+00:00", sentiment=None)]))
    changes = diff_snapshots(before, after)
    assert kinds(changes) == [
        ("left_complete", "1"),
        ("status_changed", "1"),
        ("sentiment_changed", "1"),
        ("updated_at_changed", "1"),
    ]
    assert after.user_scorecards[0].discrepancy == "not_complete_with_completed_at"


def test_completed_at_alone_does_not_mean_complete():
    sc = snapshot_from_document(application_doc(scorecards=[
        user_scorecard("1", status="incomplete", completed_at=DONE)])).user_scorecards[0]
    assert not sc.is_completed and sc.state == "pending"


def test_state_and_discrepancy_classification():
    doc = application_doc(scorecards=[
        user_scorecard("1", status="complete", completed_at=None),
        user_scorecard("2", status="weird"),
        user_scorecard("3", status=None),
    ], extra_refs=["4"])
    scs = {s.id: s for s in snapshot_from_document(doc).user_scorecards}
    assert scs["1"].state == "complete" and scs["1"].discrepancy == "complete_without_completed_at"
    assert scs["2"].state == "unknown" and scs["3"].state == "unknown"
    assert scs["4"].state == "unknown" and scs["4"].discrepancy is None


def test_stage_change_is_reported():
    before = snapshot_from_document(application_doc(stage_id=7))
    after = snapshot_from_document(application_doc(stage_id=8))
    assert kinds(diff_snapshots(before, after)) == [("application_stage_id_changed", None)]


def test_unloaded_relationship_does_not_report_removals():
    before = snapshot_from_document(application_doc(scorecards=[user_scorecard("1")]))
    after = snapshot_from_document(application_doc(include_relationship=False))
    assert kinds(diff_snapshots(before, after)) == [("user_scorecards_relationship_unavailable", None)]


def test_unloaded_scorecard_attributes_are_flagged():
    before = snapshot_from_document(application_doc(scorecards=[user_scorecard("1")]))
    after = snapshot_from_document(application_doc(extra_refs=["1"]))
    assert kinds(diff_snapshots(before, after)) == [("user_scorecard_attributes_unavailable", "1")]


def test_different_applications_rejected():
    a = snapshot_from_document(application_doc(app_id="1"))
    b = dataclasses.replace(a, application_id="2")
    with pytest.raises(ValueError):
        diff_snapshots(a, b)
