"""Account-wide discovery of applications and their user scorecards.

A discovery run pages through GET /applications?include=user_scorecards and keeps
only technical metadata (ids, timestamps, status, sentiment). Two runs can be
compared using (application_id, user_scorecard_id) as the tracking key.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .client import PinpointClient
from .snapshot import UserScorecardMeta, extract_application, index_included

DISCOVERY_VERSION = 1

DISCOVERY_FIELDS = {
    "applications": ("stage_id", "updated_at", "user_scorecards"),
    "user_scorecards": ("status", "completed_at", "updated_at", "sentiment"),
}
# Documented enums (reference/get-applications). Both are excluded by default by
# the API: confidential jobs and the "sourced" holding stage.
ALL_JOB_VISIBILITIES = "internal,external,confidential,private_job"
ALL_STAGE_BASIC_TYPES = "applied,rejected,review,interview,hired,offer,sourced"

# Application classification, in priority order.
APP_RELATIONSHIP_UNAVAILABLE = "relationship_unavailable"
APP_INCOMPLETE_RESPONSE = "incomplete_response"
APP_NO_SCORECARDS = "no_scorecards"
APP_HAS_PENDING = "has_pending"
APP_HAS_UNKNOWN_STATUS = "has_unknown_status"
APP_ALL_COMPLETED = "all_completed"


@dataclass(frozen=True)
class ScorecardRecord:
    application_id: str
    user_scorecard_id: str
    status: str | None
    updated_at: str | None
    completed_at: str | None
    sentiment: str | None
    attributes_loaded: bool = True

    @property
    def key(self) -> tuple[str, str]:
        return (self.application_id, self.user_scorecard_id)

    def _meta(self) -> UserScorecardMeta:
        return UserScorecardMeta(self.user_scorecard_id, self.status, self.completed_at,
                                 self.updated_at, self.sentiment, self.attributes_loaded)

    @property
    def state(self) -> str:
        return self._meta().state

    @property
    def is_completed(self) -> bool:
        return self._meta().is_completed

    @property
    def discrepancy(self) -> str | None:
        return self._meta().discrepancy


@dataclass(frozen=True)
class ApplicationRecord:
    application_id: str
    stage_id: str | None
    updated_at: str | None
    classification: str
    user_scorecard_ids: tuple[str, ...] = ()


@dataclass
class DiscoveryRun:
    scope: dict[str, Any]
    applications: list[ApplicationRecord] = field(default_factory=list)
    scorecards: list[ScorecardRecord] = field(default_factory=list)
    pages_fetched: int = 0
    coverage: str = "partial"  # complete | partial
    end_reason: str | None = None
    total_reported: int | None = None
    duplicate_application_rows: int = 0
    captured_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    version: int = DISCOVERY_VERSION

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["applications"] = [asdict(a) for a in self.applications]
        data["scorecards"] = [asdict(s) for s in self.scorecards]
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DiscoveryRun:
        return cls(
            scope=data.get("scope", {}),
            applications=[
                ApplicationRecord(**{**a, "user_scorecard_ids": tuple(a.get("user_scorecard_ids", ()))})
                for a in data.get("applications", [])
            ],
            scorecards=[ScorecardRecord(**s) for s in data.get("scorecards", [])],
            pages_fetched=data.get("pages_fetched", 0),
            coverage=data.get("coverage", "partial"),
            end_reason=data.get("end_reason"),
            total_reported=data.get("total_reported"),
            duplicate_application_rows=data.get("duplicate_application_rows", 0),
            captured_at=data.get("captured_at", ""),
            version=data.get("version", DISCOVERY_VERSION),
        )


def classify_application(loaded: bool, scorecards: list[ScorecardRecord]) -> str:
    if not loaded:
        return APP_RELATIONSHIP_UNAVAILABLE
    if any(not s.attributes_loaded for s in scorecards):
        return APP_INCOMPLETE_RESPONSE
    if not scorecards:
        return APP_NO_SCORECARDS
    states = {s.state for s in scorecards}
    if "pending" in states:
        return APP_HAS_PENDING
    if "unknown" in states:
        return APP_HAS_UNKNOWN_STATUS
    return APP_ALL_COMPLETED


# ----------------------------------------------------------------------- run


def build_filters(include_confidential: bool = False, include_sourced: bool = False) -> dict[str, str]:
    """API filters that define the monitoring scope."""
    filters: dict[str, str] = {}
    if include_confidential:
        filters["job_visibility"] = ALL_JOB_VISIBILITIES
    if include_sourced:
        filters["stage_basic_type"] = ALL_STAGE_BASIC_TYPES
    return filters


def run_discovery(
    client: PinpointClient,
    *,
    page_size: int,
    max_pages: int,
    include_confidential: bool = False,
    include_sourced: bool = False,
) -> DiscoveryRun:
    filters = build_filters(include_confidential, include_sourced)
    scope = {
        "endpoint": "/applications",
        "sort": "id",
        "page_size": page_size,
        "max_pages": max_pages,
        "filters": filters,
    }
    run = DiscoveryRun(scope=scope)
    apps: dict[str, ApplicationRecord] = {}
    scorecards: dict[tuple[str, str], ScorecardRecord] = {}

    for page in client.iter_applications(
        filters=filters,
        sort="id",  # stable order: records created mid-run land on later pages
        include=("user_scorecards",),
        fields=DISCOVERY_FIELDS,
        page_size=page_size,
        max_pages=max_pages,
        with_total=True,
    ):
        run.pages_fetched = page.number
        run.total_reported = page.total if page.total is not None else run.total_reported
        run.end_reason = page.end_reason
        included = index_included(page.doc)
        for resource in page.data:
            if not isinstance(resource, dict) or resource.get("type") != "applications":
                continue
            snap = extract_application(resource, included)
            records = [
                ScorecardRecord(snap.application_id, s.id, s.status, s.updated_at,
                                s.completed_at, s.sentiment, s.attributes_loaded)
                for s in snap.user_scorecards
            ]
            if snap.application_id in apps:
                run.duplicate_application_rows += 1
            apps[snap.application_id] = ApplicationRecord(
                application_id=snap.application_id,
                stage_id=snap.stage_id,
                updated_at=snap.updated_at,
                classification=classify_application(snap.user_scorecards_loaded, records),
                user_scorecard_ids=tuple(r.user_scorecard_id for r in records),
            )
            for r in records:
                scorecards[r.key] = r

    run.coverage = "complete" if run.end_reason else "partial"
    run.applications = sorted(apps.values(), key=lambda a: _num(a.application_id))
    run.scorecards = sorted(scorecards.values(),
                            key=lambda s: (_num(s.application_id), _num(s.user_scorecard_id)))
    return run


def summarize(run: DiscoveryRun) -> dict[str, Any]:
    return {
        "captured_at": run.captured_at,
        "coverage": run.coverage,
        "end_reason": run.end_reason,
        "pages_fetched": run.pages_fetched,
        "total_reported_by_api": run.total_reported,
        "applications_reviewed": len(run.applications),
        "duplicate_application_rows": run.duplicate_application_rows,
        "applications_by_classification": dict(Counter(a.classification for a in run.applications)),
        "user_scorecards_found": len(run.scorecards),
        "scorecards_by_state": dict(Counter(s.state for s in run.scorecards)),
        "scorecards_by_status": dict(Counter(str(s.status) for s in run.scorecards)),
        "scorecards_by_sentiment": dict(Counter(str(s.sentiment) for s in run.scorecards)),
        "status_completed_at_discrepancies": dict(
            Counter(s.discrepancy for s in run.scorecards if s.discrepancy)
        ),
    }


# ------------------------------------------------------------------- storage


def discovery_dir(snapshot_dir: Path) -> Path:
    return snapshot_dir / "discovery"


def save_run(run: DiscoveryRun, snapshot_dir: Path) -> Path:
    directory = discovery_dir(snapshot_dir)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(run.captured_at).strftime("%Y%m%dT%H%M%S%fZ")
    path = directory / f"discovery-{stamp}.json"
    path.write_text(json.dumps(run.to_dict(), indent=2), encoding="utf-8")
    return path


def load_run(path: Path) -> DiscoveryRun:
    return DiscoveryRun.from_dict(json.loads(path.read_text(encoding="utf-8")))


def list_runs(snapshot_dir: Path) -> list[Path]:
    return sorted(discovery_dir(snapshot_dir).glob("discovery-*.json"))


# ---------------------------------------------------------------------- diff


@dataclass(frozen=True)
class DiscoveryChange:
    kind: str
    application_id: str | None
    user_scorecard_id: str | None = None
    field: str | None = None
    before: str | None = None
    after: str | None = None
    note: str | None = None

    def describe(self) -> str:
        target = f"app {self.application_id}" if self.application_id else "run"
        if self.user_scorecard_id:
            target += f" / user_scorecard {self.user_scorecard_id}"
        text = f"[{self.kind}] {target}"
        if self.field:
            text += f": {self.field} {self.before!r} -> {self.after!r}"
        if self.note:
            text += f" ({self.note})"
        return text


TRACKED_FIELDS = ("status", "sentiment", "updated_at", "completed_at")


def diff_runs(before: DiscoveryRun, after: DiscoveryRun) -> list[DiscoveryChange]:
    changes: list[DiscoveryChange] = []
    if before.scope != after.scope:
        changes.append(DiscoveryChange("warning_scope_mismatch", None,
                                       note="runs used different filters/page size/max pages"))
    if before.coverage != "complete" or after.coverage != "complete":
        changes.append(DiscoveryChange(
            "warning_partial_coverage", None,
            note=f"before={before.coverage}, after={after.coverage}; "
                 "records outside the fetched pages are reported as not seen, not deleted",
        ))

    b_apps = {a.application_id: a for a in before.applications}
    a_apps = {a.application_id: a for a in after.applications}
    b_sc = {s.key: s for s in before.scorecards}
    a_sc = {s.key: s for s in after.scorecards}

    for app_id in sorted(a_apps.keys() - b_apps.keys(), key=_num):
        changes.append(DiscoveryChange("application_new", app_id))
    for app_id in sorted(b_apps.keys() - a_apps.keys(), key=_num):
        changes.append(DiscoveryChange("application_not_seen", app_id,
                                       note="absent from the after run; not assumed deleted"))
    for app_id in sorted(a_apps.keys() & b_apps.keys(), key=_num):
        b, a = b_apps[app_id], a_apps[app_id]
        if b.updated_at != a.updated_at:
            changes.append(DiscoveryChange("application_updated_at_changed", app_id, None,
                                           "updated_at", b.updated_at, a.updated_at))
        if b.stage_id != a.stage_id:
            changes.append(DiscoveryChange("application_stage_changed", app_id, None,
                                           "stage_id", b.stage_id, a.stage_id))

    for key in sorted(a_sc.keys() - b_sc.keys(), key=_key):
        sc = a_sc[key]
        changes.append(DiscoveryChange("scorecard_new", *key, "status", None, sc.status))
        if sc.is_completed:
            changes.append(DiscoveryChange("became_complete", *key, "status", None, sc.status))

    for key in sorted(b_sc.keys() - a_sc.keys(), key=_key):
        app_after = a_apps.get(key[0])
        if app_after is None:
            note = "application not present in the after run"
        elif app_after.classification == APP_RELATIONSHIP_UNAVAILABLE:
            note = "user_scorecards relationship unavailable in the after run"
        else:
            note = "no longer linked to the application; not assumed deleted"
        changes.append(DiscoveryChange("scorecard_not_seen", *key, note=note))

    for key in sorted(a_sc.keys() & b_sc.keys(), key=_key):
        b, a = b_sc[key], a_sc[key]
        if not (b.attributes_loaded and a.attributes_loaded):
            changes.append(DiscoveryChange("scorecard_unverifiable", *key,
                                           note="attributes missing from one of the runs"))
            continue
        if not b.is_completed and a.is_completed:
            changes.append(DiscoveryChange("became_complete", *key, "status", b.status, a.status))
        elif b.is_completed and not a.is_completed:
            changes.append(DiscoveryChange("left_complete", *key, "status", b.status, a.status,
                                           note="cause (edit/reset) not inferable from the API"))
        for name in TRACKED_FIELDS:
            bv, av = getattr(b, name), getattr(a, name)
            if bv != av:
                changes.append(DiscoveryChange(f"{name}_changed", *key, name, bv, av))
        if a.discrepancy and a.discrepancy != b.discrepancy:
            changes.append(DiscoveryChange("status_completed_at_discrepancy", *key,
                                           note=a.discrepancy))
    return changes


def _num(value: str) -> tuple[int, str]:
    return (int(value), "") if value.isdigit() else (0, value)


def _key(key: tuple[str, str]) -> tuple[tuple[int, str], tuple[int, str]]:
    return (_num(key[0]), _num(key[1]))
