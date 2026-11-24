from __future__ import annotations

from typing import Any

import pytest

from pinpoint_scorecard_webhook.config import Settings

API_KEY = "test-secret-key-do-not-print"
BASE = "https://acme.pinpointhq.com/api/v1"


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        pinpoint_subdomain="acme",
        pinpoint_api_key=API_KEY,
        pinpoint_max_retries=2,
        pinpoint_backoff_base_seconds=0,
        pinpoint_page_size=2,
        pinpoint_max_pages=5,
        pinpoint_page_delay_seconds=0,
        application_id="101",
        snapshot_dir=tmp_path / "snapshots",
    )


def user_scorecard(
    sc_id: str,
    *,
    status: str | None = "incomplete",
    completed_at: str | None = None,
    updated_at: str = "2026-11-20T10:00:00+00:00",
    sentiment: str | None = None,
) -> dict[str, Any]:
    return {
        "id": sc_id,
        "type": "user_scorecards",
        "attributes": {
            "status": status,
            "completed_at": completed_at,
            "updated_at": updated_at,
            "sentiment": sentiment,
        },
    }


def application_doc(
    *,
    app_id: str = "101",
    updated_at: str = "2026-11-20T10:00:00+00:00",
    stage_id: int | None = 7,
    candidate_id: str | None = "55",
    scorecards: list[dict[str, Any]] | None = None,
    include_relationship: bool = True,
    extra_refs: list[str] | None = None,
) -> dict[str, Any]:
    scorecards = scorecards or []
    resource: dict[str, Any] = {
        "id": app_id,
        "type": "applications",
        "attributes": {"candidate_id": candidate_id, "stage_id": stage_id, "updated_at": updated_at},
        "relationships": {"candidate": {"data": {"type": "candidates", "id": candidate_id}}},
    }
    if include_relationship:
        refs = [{"type": "user_scorecards", "id": s["id"]} for s in scorecards]
        refs += [{"type": "user_scorecards", "id": i} for i in extra_refs or []]
        resource["relationships"]["user_scorecards"] = {"data": refs}
    else:
        resource["relationships"]["user_scorecards"] = {"meta": {"included": False}}
    return {"data": resource, "included": scorecards}
