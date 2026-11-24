"""Technical-metadata snapshots of one application and its user scorecards.

Only ids, timestamps, status and sentiment are kept. Nothing else from the API
response is extracted, so snapshots on disk contain no personal data.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SNAPSHOT_VERSION = 1

# `status` values are not documented. Observed live on 2026-11-24: "complete",
# "incomplete". A scorecard with status "incomplete" kept a non-null completed_at,
# so completion is decided by status, never by completed_at.
COMPLETE_STATUSES = frozenset({"complete"})
PENDING_STATUSES = frozenset({"incomplete"})


@dataclass(frozen=True)
class UserScorecardMeta:
    id: str
    status: str | None
    completed_at: str | None
    updated_at: str | None
    sentiment: str | None
    # False when the id is referenced by the application but the resource was not
    # present in `included` (attributes unknown, not empty).
    attributes_loaded: bool = True

    @property
    def is_completed(self) -> bool:
        return self.status in COMPLETE_STATUSES

    @property
    def state(self) -> str:
        """complete | pending | unknown (attributes missing or unrecognised status)."""
        if not self.attributes_loaded or self.status is None:
            return "unknown"
        if self.status in COMPLETE_STATUSES:
            return "complete"
        if self.status in PENDING_STATUSES:
            return "pending"
        return "unknown"

    @property
    def discrepancy(self) -> str | None:
        """Flag status/completed_at combinations that disagree."""
        if not self.attributes_loaded:
            return None
        if self.is_completed and self.completed_at is None:
            return "complete_without_completed_at"
        if not self.is_completed and self.status is not None and self.completed_at is not None:
            return "not_complete_with_completed_at"
        return None


@dataclass(frozen=True)
class ApplicationSnapshot:
    application_id: str
    candidate_id: str | None
    stage_id: str | None
    updated_at: str | None
    # False when the API did not return the user_scorecards relationship data, so
    # "no scorecards" and "unknown" can be told apart.
    user_scorecards_loaded: bool
    user_scorecards: tuple[UserScorecardMeta, ...] = ()
    captured_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    version: int = SNAPSHOT_VERSION

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["user_scorecards"] = [asdict(s) for s in self.user_scorecards]
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ApplicationSnapshot:
        return cls(
            application_id=str(data["application_id"]),
            candidate_id=data.get("candidate_id"),
            stage_id=data.get("stage_id"),
            updated_at=data.get("updated_at"),
            user_scorecards_loaded=bool(data.get("user_scorecards_loaded")),
            user_scorecards=tuple(UserScorecardMeta(**s) for s in data.get("user_scorecards", [])),
            captured_at=data.get("captured_at", ""),
            version=int(data.get("version", SNAPSHOT_VERSION)),
        )


def _str_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


def _rel_data(resource: dict[str, Any], name: str) -> tuple[bool, Any]:
    """Return (loaded, data) for a JSON:API relationship."""
    rel = (resource.get("relationships") or {}).get(name)
    if not isinstance(rel, dict) or "data" not in rel:
        return False, None
    return True, rel["data"]


IncludedIndex = dict[tuple[Any, str], dict[str, Any]]


def index_included(doc: dict[str, Any]) -> IncludedIndex:
    return {
        (item.get("type"), str(item.get("id"))): item
        for item in doc.get("included") or []
        if isinstance(item, dict)
    }


def snapshot_from_document(doc: dict[str, Any]) -> ApplicationSnapshot:
    """Build a snapshot from a GET /applications/{id}?include=user_scorecards response."""
    resource = doc.get("data")
    if not isinstance(resource, dict) or resource.get("type") != "applications":
        raise ValueError("response does not contain a single 'applications' resource")
    return extract_application(resource, index_included(doc))


def extract_application(resource: dict[str, Any], included: IncludedIndex) -> ApplicationSnapshot:
    """Extract technical metadata from one `applications` resource object."""
    attrs = resource.get("attributes") or {}

    candidate_id = _str_or_none(attrs.get("candidate_id"))
    if candidate_id is None:
        loaded, data = _rel_data(resource, "candidate")
        if loaded and isinstance(data, dict):
            candidate_id = _str_or_none(data.get("id"))

    loaded, rel = _rel_data(resource, "user_scorecards")
    scorecards: list[UserScorecardMeta] = []
    if loaded and isinstance(rel, list):
        for ref in rel:
            sc_id = str(ref.get("id"))
            item = included.get(("user_scorecards", sc_id))
            if item is None:
                scorecards.append(UserScorecardMeta(sc_id, None, None, None, None, False))
                continue
            a = item.get("attributes") or {}
            scorecards.append(
                UserScorecardMeta(
                    id=sc_id,
                    status=_str_or_none(a.get("status")),
                    completed_at=_str_or_none(a.get("completed_at")),
                    updated_at=_str_or_none(a.get("updated_at")),
                    sentiment=_str_or_none(a.get("sentiment")),
                )
            )
    else:
        loaded = False

    return ApplicationSnapshot(
        application_id=str(resource.get("id")),
        candidate_id=candidate_id,
        stage_id=_str_or_none(attrs.get("stage_id")),
        updated_at=_str_or_none(attrs.get("updated_at")),
        user_scorecards_loaded=loaded,
        user_scorecards=tuple(sorted(scorecards, key=lambda s: _id_key(s.id))),
    )


# ---------------------------------------------------------------------- storage


def snapshot_path(directory: Path, application_id: str, label: str) -> Path:
    return directory / f"application-{application_id}-{label}.json"


def save_snapshot(snapshot: ApplicationSnapshot, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot.to_dict(), indent=2), encoding="utf-8")


def load_snapshot(path: Path) -> ApplicationSnapshot:
    return ApplicationSnapshot.from_dict(json.loads(path.read_text(encoding="utf-8")))


# ------------------------------------------------------------------------- diff


@dataclass(frozen=True)
class Change:
    kind: str
    user_scorecard_id: str | None
    field: str | None
    before: str | None
    after: str | None

    def describe(self) -> str:
        target = f"user_scorecard {self.user_scorecard_id}" if self.user_scorecard_id else "application"
        if self.field:
            return f"[{self.kind}] {target}: {self.field} {self.before!r} -> {self.after!r}"
        return f"[{self.kind}] {target}"


TRACKED_SCORECARD_FIELDS = ("status", "sentiment", "completed_at", "updated_at")


def diff_snapshots(before: ApplicationSnapshot, after: ApplicationSnapshot) -> list[Change]:
    if before.application_id != after.application_id:
        raise ValueError("snapshots belong to different applications")

    changes: list[Change] = []
    for name in ("updated_at", "stage_id"):
        b, a = getattr(before, name), getattr(after, name)
        if b != a:
            changes.append(Change(f"application_{name}_changed", None, name, b, a))

    if not (before.user_scorecards_loaded and after.user_scorecards_loaded):
        changes.append(
            Change("user_scorecards_relationship_unavailable", None, "user_scorecards_loaded",
                   str(before.user_scorecards_loaded), str(after.user_scorecards_loaded))
        )
        return changes

    b_map = {s.id: s for s in before.user_scorecards}
    a_map = {s.id: s for s in after.user_scorecards}

    for sc_id in sorted(a_map.keys() - b_map.keys(), key=_id_key):
        sc = a_map[sc_id]
        changes.append(Change("user_scorecard_added", sc_id, None, None, None))
        if sc.is_completed:
            changes.append(Change("completed", sc_id, "status", None, sc.status))

    for sc_id in sorted(b_map.keys() - a_map.keys(), key=_id_key):
        changes.append(Change("user_scorecard_removed", sc_id, None, None, None))

    for sc_id in sorted(a_map.keys() & b_map.keys(), key=_id_key):
        b, a = b_map[sc_id], a_map[sc_id]
        if not (b.attributes_loaded and a.attributes_loaded):
            changes.append(Change("user_scorecard_attributes_unavailable", sc_id, None, None, None))
            continue
        # Transitions are keyed on status. Leaving "complete" is reported neutrally:
        # the cause (edit, reopen, reset) cannot be inferred from the API.
        if not b.is_completed and a.is_completed:
            changes.append(Change("completed", sc_id, "status", b.status, a.status))
        elif b.is_completed and not a.is_completed:
            changes.append(Change("left_complete", sc_id, "status", b.status, a.status))
        for name in TRACKED_SCORECARD_FIELDS:
            bv, av = getattr(b, name), getattr(a, name)
            if bv != av:
                changes.append(Change(f"{name}_changed", sc_id, name, bv, av))
    return changes


def _id_key(value: str) -> tuple[int, str]:
    return (int(value), "") if value.isdigit() else (0, value)
