from __future__ import annotations

import httpx
import pytest
import respx
from pydantic import ValidationError

from pinpoint_scorecard_webhook.client import PinpointClient, extract_total, filter_params
from pinpoint_scorecard_webhook.config import Settings
from pinpoint_scorecard_webhook.errors import (
    PinpointAuthError,
    PinpointNotFoundError,
    PinpointPermissionError,
    PinpointRateLimitError,
    PinpointTransportError,
    ReadOnlyViolationError,
)

from .conftest import API_KEY, BASE, application_doc


def make_client(settings, sleeps=None):
    sleeps = [] if sleeps is None else sleeps
    return PinpointClient(settings, sleep=sleeps.append)


def err_body(status: int, code: str, title: str) -> dict:
    return {"errors": [{"status": str(status), "code": code, "title": title, "detail": "x"}]}


# ----------------------------------------------------------------- requests


@respx.mock
def test_get_application_sends_auth_include_and_sparse_fields(settings):
    route = respx.get(f"{BASE}/applications/101").respond(json=application_doc())
    with make_client(settings) as client:
        client.get_application("101")

    request = route.calls.last.request
    assert request.headers["X-API-KEY"] == API_KEY
    assert request.headers["Accept"] == "application/vnd.api+json"
    params = request.url.params
    assert params["include"] == "user_scorecards"
    assert params["fields[user_scorecards]"] == "status,completed_at,updated_at,sentiment"
    assert "user_scorecards" in params["fields[applications]"].split(",")
    # PII-bearing fields are never requested.
    assert "summary" not in params["fields[user_scorecards]"]
    assert "email" not in params["fields[applications]"]


def test_rejects_non_numeric_ids(settings):
    with make_client(settings) as client, pytest.raises(ValueError):
        client.get_application("../jobs")


@respx.mock
def test_get_scorecard_uses_documented_include(settings):
    route = respx.get(f"{BASE}/scorecards/9").respond(json={"data": {"id": "9", "type": "scorecards"}})
    with make_client(settings) as client:
        client.get_scorecard("9", include=("user_scorecards", "stages"))
    assert route.calls.last.request.url.params["include"] == "user_scorecards,stages"


def test_filter_params_builds_nested_brackets():
    assert filter_params({"job_id": "1", "updated_at.gt": "2026-01-01T00:00:00+00:00"}) == {
        "filter[job_id]": "1",
        "filter[updated_at][gt]": "2026-01-01T00:00:00+00:00",
    }


# --------------------------------------------------------------- pagination


@respx.mock
def test_pagination_stops_on_short_page(settings):
    def page(request):
        n = int(request.url.params["page[number]"])
        data = {1: [{"id": "1"}, {"id": "2"}], 2: [{"id": "3"}]}[n]
        return httpx.Response(200, json={"data": data})

    route = respx.get(f"{BASE}/applications").mock(side_effect=page)
    with make_client(settings) as client:
        pages = list(client.iter_applications(filters={"job_id": "4"}, sort="updated_at"))

    assert [len(p.data) for p in pages] == [2, 1]
    assert pages[-1].is_last and pages[-1].end_reason == "short_page"
    assert route.call_count == 2
    first = route.calls[0].request.url.params
    assert first["page[size]"] == "2" and first["filter[job_id]"] == "4" and first["sort"] == "updated_at"


@respx.mock
def test_pagination_follows_links_next_when_present(settings):
    responses = [
        httpx.Response(200, json={"data": [{"id": "1"}], "links": {"next": "p2"}}),
        httpx.Response(200, json={"data": [{"id": "2"}], "links": {"next": None}}),
    ]
    route = respx.get(f"{BASE}/scorecards").mock(side_effect=responses)
    with make_client(settings) as client:
        pages = list(client.iter_scorecards())
    assert len(pages) == 2 and route.call_count == 2
    assert pages[-1].end_reason == "no_next_link"


@respx.mock
def test_pagination_uses_reported_total(settings):
    route = respx.get(f"{BASE}/applications").respond(
        json={"data": [{"id": "1"}, {"id": "2"}], "meta": {"stats": {"total": {"count": 4}}}}
    )
    with make_client(settings) as client:
        pages = list(client.iter_applications(with_total=True))
    assert len(pages) == 2 and pages[-1].end_reason == "total_reached"
    assert pages[0].total == 4
    assert route.calls[0].request.url.params["stats[total]"] == "count"


@respx.mock
def test_full_pages_until_limit_report_partial_not_silent(settings, caplog):
    route = respx.get(f"{BASE}/applications").respond(json={"data": [{"id": "1"}, {"id": "2"}]})
    with make_client(settings) as client:
        pages = list(client.iter_applications(max_pages=3))
    assert len(pages) == 3 and route.call_count == 3
    assert not pages[-1].is_last
    assert "coverage is partial" in caplog.text


def test_max_pages_above_ceiling_is_an_explicit_error(settings):
    with make_client(settings) as client, pytest.raises(ValueError, match="PINPOINT_MAX_PAGES"):
        list(client.iter_applications(max_pages=50))


@respx.mock
def test_page_delay_between_pages(settings):
    settings = settings.model_copy(update={"pinpoint_page_delay_seconds": 0.5})
    respx.get(f"{BASE}/applications").mock(side_effect=[
        httpx.Response(200, json={"data": [{"id": "1"}, {"id": "2"}]}),
        httpx.Response(200, json={"data": []}),
    ])
    sleeps: list[float] = []
    with make_client(settings, sleeps) as client:
        pages = list(client.iter_applications())
    assert sleeps == [0.5] and pages[-1].end_reason == "empty_page"


@respx.mock
def test_empty_first_page(settings):
    respx.get(f"{BASE}/applications").respond(json={"data": []})
    with make_client(settings) as client:
        pages = list(client.iter_applications())
    assert len(pages) == 1 and pages[0].end_reason == "empty_page"


@pytest.mark.parametrize(
    ("meta", "expected"),
    [
        ({"stats": {"total": {"count": 12}}}, 12),
        ({"total_count": 7}, 7),
        ({}, None),
        (None, None),
        ({"total": 1, "count": 2}, None),  # ambiguous
        ({"applications_truncated": True}, None),
    ],
)
def test_extract_total(meta, expected):
    assert extract_total(meta) == expected


# ------------------------------------------------------------------- errors


@pytest.mark.parametrize(
    ("status", "code", "exc_type"),
    [
        (401, "unauthorized", PinpointAuthError),
        (403, "forbidden", PinpointPermissionError),
        (404, "not_found", PinpointNotFoundError),
    ],
)
@respx.mock
def test_error_mapping_is_not_retried(settings, status, code, exc_type):
    route = respx.get(f"{BASE}/applications/101").respond(status, json=err_body(status, code, "T"))
    sleeps: list[float] = []
    with make_client(settings, sleeps) as client, pytest.raises(exc_type) as info:
        client.get_application("101")
    assert route.call_count == 1 and sleeps == []
    assert info.value.status_code == status
    assert code in str(info.value)
    assert API_KEY not in str(info.value)


@respx.mock
def test_429_is_retried_then_succeeds(settings):
    route = respx.get(f"{BASE}/applications/101").mock(
        side_effect=[
            httpx.Response(429, json=err_body(429, "too_many_requests", "Too Many Requests")),
            httpx.Response(200, json=application_doc()),
        ]
    )
    sleeps: list[float] = []
    with make_client(settings, sleeps) as client:
        doc = client.get_application("101")
    assert doc["data"]["id"] == "101"
    assert route.call_count == 2 and len(sleeps) == 1


@respx.mock
def test_429_exhausted_raises_rate_limit_error(settings):
    route = respx.get(f"{BASE}/applications/101").respond(
        429, json=err_body(429, "too_many_requests", "Too Many Requests")
    )
    sleeps: list[float] = []
    with make_client(settings, sleeps) as client, pytest.raises(PinpointRateLimitError):
        client.get_application("101")
    assert route.call_count == settings.pinpoint_max_retries + 1
    assert len(sleeps) == settings.pinpoint_max_retries


@respx.mock
def test_retry_after_header_is_honoured_and_capped(settings):
    respx.get(f"{BASE}/applications/101").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "999"}),
            httpx.Response(200, json=application_doc()),
        ]
    )
    sleeps: list[float] = []
    with make_client(settings, sleeps) as client:
        client.get_application("101")
    assert sleeps == [settings.pinpoint_backoff_max_seconds]


@respx.mock
def test_503_and_transport_errors_are_retried(settings):
    respx.get(f"{BASE}/applications/101").mock(
        side_effect=[
            httpx.Response(503),
            httpx.ConnectTimeout("timeout"),
            httpx.Response(200, json=application_doc()),
        ]
    )
    with make_client(settings) as client:
        assert client.get_application("101")["data"]["id"] == "101"


@respx.mock
def test_transport_error_exhausted(settings):
    respx.get(f"{BASE}/applications/101").mock(side_effect=httpx.ConnectError("down"))
    with make_client(settings) as client, pytest.raises(PinpointTransportError):
        client.get_application("101")


@respx.mock
def test_500_is_not_retried(settings):
    route = respx.get(f"{BASE}/applications/101").respond(500, text="<html>oops</html>")
    with make_client(settings) as client, pytest.raises(Exception) as info:
        client.get_application("101")
    assert route.call_count == 1
    assert "oops" not in str(info.value)


@respx.mock
def test_user_scorecard_probe_handles_404(settings):
    respx.get(f"{BASE}/user_scorecards/5").respond(404, json=err_body(404, "not_found", "Not Found"))
    with make_client(settings) as client, pytest.raises(PinpointNotFoundError):
        client.get_user_scorecard("5")


# --------------------------------------------------------------- read-only


@respx.mock
def test_non_get_requests_are_blocked(settings):
    with make_client(settings) as client:
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            with pytest.raises(ReadOnlyViolationError):
                client._http.request(method, "/applications/101")


def test_client_has_no_write_methods():
    public = {name for name in dir(PinpointClient) if not name.startswith("_")}
    assert not any(word in name for name in public for word in ("create", "update", "delete", "put", "post", "patch"))


def test_read_only_false_is_rejected():
    with pytest.raises(ValidationError):
        Settings(_env_file=None, pinpoint_subdomain="acme", pinpoint_api_key="k", read_only=False)


@pytest.mark.parametrize("bad", ["evil.com/x", "acme.evil", "-acme", "a b"])
def test_subdomain_must_be_a_label(bad):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, pinpoint_subdomain=bad, pinpoint_api_key="k")


def test_api_key_not_in_settings_repr(settings):
    assert API_KEY not in repr(settings)
