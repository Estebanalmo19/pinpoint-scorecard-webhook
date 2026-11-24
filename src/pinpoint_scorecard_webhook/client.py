"""Read-only HTTP client for the Pinpoint ATS JSON:API (v1).

Only GET is implemented, and an httpx request hook rejects any other method as a
second line of defence. Endpoints and parameters are taken from the OpenAPI spec
(v1.0.27) published at developers.pinpointhq.com.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Self

import httpx

from .config import Settings
from .errors import (
    PinpointAPIError,
    PinpointAuthError,
    PinpointNotFoundError,
    PinpointPermissionError,
    PinpointRateLimitError,
    PinpointTransportError,
    ReadOnlyViolationError,
)

logger = logging.getLogger(__name__)

JsonDoc = dict[str, Any]

RETRYABLE_STATUS = frozenset({429, 502, 503, 504})
JSONAPI_MEDIA_TYPE = "application/vnd.api+json"

# Minimal sparse fieldsets: only technical metadata is requested from the API, so
# names, emails, CVs and scorecard free text (`summary`) never leave Pinpoint.
APPLICATION_FIELDS = ("candidate_id", "stage_id", "updated_at", "candidate", "user_scorecards")
USER_SCORECARD_FIELDS = ("status", "completed_at", "updated_at", "sentiment")
SCORECARD_FIELDS = ("name", "is_active", "updated_at", "user_scorecards", "stages")


@dataclass(frozen=True)
class Page:
    number: int
    doc: JsonDoc
    size: int
    total: int | None
    # Why this page is the last one, or None if more pages may exist.
    end_reason: str | None

    @property
    def is_last(self) -> bool:
        return self.end_reason is not None

    @property
    def data(self) -> list[dict[str, Any]]:
        data = self.doc.get("data")
        return data if isinstance(data, list) else []


def extract_total(meta: Any) -> int | None:
    """Find the record count returned for `stats[total]=count`.

    The exact meta key is not documented, so this accepts a single integer found
    under a key path containing "total" or "count" (e.g. meta.stats.total.count).
    Returns None when absent or ambiguous.
    """
    found: list[int] = []

    def walk(node: Any, path: tuple[str, ...]) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                walk(value, (*path, str(key).lower()))
        elif (
            isinstance(node, int)
            and not isinstance(node, bool)
            and any("total" in p or "count" in p for p in path)
        ):
            found.append(node)

    walk(meta, ())
    return found[0] if len(found) == 1 else None


def _end_reason(doc: JsonDoc, number: int, size: int, total: int | None) -> str | None:
    data = doc.get("data")
    count = len(data) if isinstance(data, list) else 0
    links = doc.get("links")
    if count == 0:
        return "empty_page"
    if isinstance(links, dict):
        return None if links.get("next") else "no_next_link"
    if total is not None:
        return "total_reached" if number * size >= total else None
    if count < size:
        return "short_page"
    return None


def _enforce_get_only(request: httpx.Request) -> None:
    if request.method != "GET":
        raise ReadOnlyViolationError(f"Blocked non-GET request: {request.method}")


def _error_reason(response: httpx.Response) -> str:
    """Extract JSON:API error code/title only; never the full body."""
    try:
        errors = response.json().get("errors") or []
        first = errors[0]
        parts = [str(first.get(k)) for k in ("code", "title") if first.get(k)]
        return " / ".join(parts)
    except Exception:  # noqa: BLE001 - body may be HTML or empty; reason is best-effort
        return ""


def _fields_params(fields: Mapping[str, Sequence[str]] | None) -> dict[str, str]:
    return {f"fields[{rtype}]": ",".join(cols) for rtype, cols in (fields or {}).items()}


class PinpointClient:
    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not settings.read_only:  # also enforced by Settings validation
            raise ReadOnlyViolationError("READ_ONLY must be true")
        self._settings = settings
        self._sleep = sleep
        self._http = httpx.Client(
            base_url=settings.base_url,
            headers={
                "X-API-KEY": settings.pinpoint_api_key.get_secret_value(),
                "Accept": JSONAPI_MEDIA_TYPE,
            },
            timeout=httpx.Timeout(settings.pinpoint_timeout_seconds),
            event_hooks={"request": [_enforce_get_only]},
            transport=transport,
            follow_redirects=False,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    # ------------------------------------------------------------------ transport

    def _backoff_delay(self, attempt: int, response: httpx.Response | None) -> float:
        cap = self._settings.pinpoint_backoff_max_seconds
        # Retry-After is not documented by Pinpoint; honour it only if present.
        if response is not None:
            retry_after = response.headers.get("Retry-After", "")
            if retry_after.isdigit():
                return min(float(retry_after), cap)
        base = self._settings.pinpoint_backoff_base_seconds * (2**attempt)
        return min(base + random.uniform(0, base / 2), cap)

    def _get(self, path: str, params: Mapping[str, Any] | None = None) -> JsonDoc:
        max_retries = self._settings.pinpoint_max_retries
        for attempt in range(max_retries + 1):
            try:
                response = self._http.get(path, params=params)
            except httpx.TransportError as exc:
                if attempt >= max_retries:
                    raise PinpointTransportError(
                        f"{type(exc).__name__} on GET {path} after {attempt + 1} attempts"
                    ) from None
                delay = self._backoff_delay(attempt, None)
                logger.warning("transport error on GET %s (%s); retry in %.1fs",
                               path, type(exc).__name__, delay)
                self._sleep(delay)
                continue

            if response.status_code in RETRYABLE_STATUS and attempt < max_retries:
                delay = self._backoff_delay(attempt, response)
                logger.warning("HTTP %s on GET %s; retry %d/%d in %.1fs",
                               response.status_code, path, attempt + 1, max_retries, delay)
                self._sleep(delay)
                continue

            return self._handle(response, path)
        raise AssertionError("unreachable")  # pragma: no cover

    @staticmethod
    def _handle(response: httpx.Response, path: str) -> JsonDoc:
        status = response.status_code
        if 200 <= status < 300:
            return response.json()
        reason = _error_reason(response)
        error_cls = {
            401: PinpointAuthError,
            403: PinpointPermissionError,
            404: PinpointNotFoundError,
            429: PinpointRateLimitError,
        }.get(status, PinpointAPIError)
        raise error_cls(status, path, reason)

    def _iter_pages(
        self,
        path: str,
        params: Mapping[str, Any],
        *,
        page_size: int | None,
        max_pages: int | None,
        with_total: bool = False,
    ) -> Iterator[Page]:
        """Yield pages until the end is detected or `max_pages` is reached.

        The last yielded page tells the caller how iteration stopped: `is_last`
        is True when the end of the collection was detected, False when the
        `max_pages` limit cut the run short (coverage is then partial).
        """
        size = page_size or self._settings.pinpoint_page_size
        ceiling = self._settings.pinpoint_max_pages
        limit = ceiling if max_pages is None else max_pages
        if not 1 <= limit <= ceiling:
            raise ValueError(
                f"max_pages={limit} is outside 1..{ceiling}; raise PINPOINT_MAX_PAGES to allow more"
            )
        if not 1 <= size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")
        base = {**params, "page[size]": size}
        if with_total:
            base["stats[total]"] = "count"

        for number in range(1, limit + 1):
            if number > 1 and self._settings.pinpoint_page_delay_seconds:
                self._sleep(self._settings.pinpoint_page_delay_seconds)
            doc = self._get(path, {**base, "page[number]": number})
            total = extract_total(doc.get("meta"))
            end_reason = _end_reason(doc, number, size, total)
            yield Page(number=number, doc=doc, size=size, total=total, end_reason=end_reason)
            if end_reason:
                return
        logger.warning("GET %s stopped at max_pages=%d before the end; coverage is partial",
                       path, limit)

    # ------------------------------------------------------------------ endpoints

    def get_application(
        self,
        application_id: str,
        *,
        include: Sequence[str] = ("user_scorecards",),
        fields: Mapping[str, Sequence[str]] | None = None,
    ) -> JsonDoc:
        """GET /applications/{id} (documented: reference/get-application)."""
        if fields is None:
            fields = {"applications": APPLICATION_FIELDS, "user_scorecards": USER_SCORECARD_FIELDS}
        params = {"include": ",".join(include), **_fields_params(fields)}
        return self._get(f"/applications/{_safe_id(application_id)}", params)

    def iter_applications(
        self,
        *,
        filters: Mapping[str, str] | None = None,
        sort: str | None = None,
        include: Sequence[str] = (),
        fields: Mapping[str, Sequence[str]] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        with_total: bool = False,
    ) -> Iterator[Page]:
        """GET /applications, paginated (documented: reference/get-applications).

        `filters` uses the dotted form of `filter_params`, e.g. {"updated_at.gt": ts}.
        `max_pages` defaults to, and may not exceed, PINPOINT_MAX_PAGES.
        """
        if fields is None:
            fields = {"applications": APPLICATION_FIELDS, "user_scorecards": USER_SCORECARD_FIELDS}
        params: dict[str, Any] = {**filter_params(filters), **_fields_params(fields)}
        if include:
            params["include"] = ",".join(include)
        if sort:
            params["sort"] = sort
        return self._iter_pages("/applications", params, page_size=page_size,
                                max_pages=max_pages, with_total=with_total)

    def get_scorecard(
        self,
        scorecard_id: str,
        *,
        include: Sequence[str] = ("user_scorecards",),
        fields: Mapping[str, Sequence[str]] | None = None,
    ) -> JsonDoc:
        """GET /scorecards/{id} (documented: reference/get-scorecard).

        Documented includes: scorecard_notes, scorecard_sections, user_scorecards, stages.
        """
        if fields is None:
            fields = {"scorecards": SCORECARD_FIELDS, "user_scorecards": USER_SCORECARD_FIELDS}
        params = {"include": ",".join(include), **_fields_params(fields)}
        return self._get(f"/scorecards/{_safe_id(scorecard_id)}", params)

    def iter_scorecards(
        self,
        *,
        filters: Mapping[str, str] | None = None,
        include: Sequence[str] = (),
        fields: Mapping[str, Sequence[str]] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
    ) -> Iterator[Page]:
        """GET /scorecards, paginated (documented: reference/get-scorecards)."""
        if fields is None:
            fields = {"scorecards": SCORECARD_FIELDS, "user_scorecards": USER_SCORECARD_FIELDS}
        params: dict[str, Any] = {**filter_params(filters), **_fields_params(fields)}
        if include:
            params["include"] = ",".join(include)
        return self._iter_pages("/scorecards", params, page_size=page_size, max_pages=max_pages)

    def get_user_scorecard(self, user_scorecard_id: str) -> JsonDoc:
        """GET /user_scorecards/{id}. UNDOCUMENTED: only named in changelog 1.0.27.

        Callers must expect PinpointNotFoundError / PinpointPermissionError.
        """
        params = _fields_params({"user_scorecards": USER_SCORECARD_FIELDS})
        return self._get(f"/user_scorecards/{_safe_id(user_scorecard_id)}", params)


def filter_params(filters: Mapping[str, str] | None) -> dict[str, str]:
    """Build JSON:API filter params.

    Keys are the filter path without the `filter` prefix; nested operators use a
    list-like dotted form: {"job_id": "1", "updated_at.gt": "2026-01-01T00:00:00+00:00"}
    -> filter[job_id]=1&filter[updated_at][gt]=...
    """
    out: dict[str, str] = {}
    for key, value in (filters or {}).items():
        parts = key.split(".")
        out["filter" + "".join(f"[{p}]" for p in parts)] = value
    return out


def _safe_id(value: str) -> str:
    value = str(value).strip()
    if not value.isdigit():
        raise ValueError("Pinpoint ids must be numeric")
    return value
