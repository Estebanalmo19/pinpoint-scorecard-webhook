"""One polling pass: discover scorecards, diff against stored state, emit internal events.

Event rules (status values observed live: "complete", "incomplete"):

- first sighting                       -> classified by detection.classify_first_sighting:
                                          baseline / incomplete -> scorecard.discovered (skipped)
                                          complete + proven new -> scorecard.completed (pending)
                                          complete + unproven   -> scorecard.discovered (review_*)
- status non-complete -> complete      -> scorecard.completed
- status complete -> non-complete      -> scorecard.left_complete
- sentiment changed, no transition     -> scorecard.sentiment_changed
- any other observed change            -> scorecard.updated

A complete scorecard with sentiment null stays complete; no sentiment is inferred.
Missing scorecards are never treated as deleted: their state is simply not touched.
The whole pass (state + events + run row) commits in one transaction, and
nothing is written if fetching fails.

Concurrency and recovery: a pass first takes the backend's poller lock (a
PostgreSQL advisory lock). If another instance holds it, the pass is skipped.
While holding the lock, any poll_runs row still 'running' belongs to a process
that died mid-run, so it is marked 'interrupted' before the new run starts.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import socket
import time
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .client import PinpointClient
from .detection import (
    DetectionContext,
    DetectionPolicy,
    canonical_scope,
    classify_first_sighting,
    scope_key,
)
from .discovery import (
    APP_INCOMPLETE_RESPONSE,
    APP_RELATIONSHIP_UNAVAILABLE,
    DiscoveryRun,
    ScorecardRecord,
    build_filters,
    run_discovery,
)
from .errors import PinpointError
from .event_policy import initial_processing
from .logging_setup import sanitize
from .snapshot import COMPLETE_STATUSES
from .store import EventRow, StateBackend, StoredScorecard

logger = logging.getLogger(__name__)


class PollerLockLostError(RuntimeError):
    """The advisory lock is no longer held by this session; refuse to commit."""

EVENT_DISCOVERED = "scorecard.discovered"
EVENT_COMPLETED = "scorecard.completed"
EVENT_LEFT_COMPLETE = "scorecard.left_complete"
EVENT_SENTIMENT_CHANGED = "scorecard.sentiment_changed"
EVENT_UPDATED = "scorecard.updated"


def observation_hash(sc: ScorecardRecord | StoredScorecard) -> str:
    payload = json.dumps([sc.status, sc.sentiment, sc.updated_at, sc.completed_at])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def derive_event_types(prev: StoredScorecard | None, obs: ScorecardRecord) -> list[str]:
    if prev is None:
        return [EVENT_DISCOVERED]
    if prev.observation_hash == observation_hash(obs):
        return []
    was_complete = prev.status in COMPLETE_STATUSES
    is_complete = obs.status in COMPLETE_STATUSES
    if not was_complete and is_complete:
        return [EVENT_COMPLETED]
    if was_complete and not is_complete:
        return [EVENT_LEFT_COMPLETE]
    if prev.sentiment != obs.sentiment:
        return [EVENT_SENTIMENT_CHANGED]
    return [EVENT_UPDATED]


def event_id(
    app_id: str,
    sc_id: str,
    event_type: str,
    prev_hash: str | None,
    new_hash: str,
    prev_changed_at: str | None = None,
) -> str:
    """Deterministic id for one transition.

    Replaying the same transition from the same stored state yields the same id
    (the insert ignores the duplicate event_id). `prev_changed_at` distinguishes a transition
    that repeats an earlier (prev_hash, new_hash) pair, e.g. A->B->A->B with
    identical observations, so the later one is not silently dropped.
    """
    raw = f"{app_id}|{sc_id}|{event_type}|{prev_hash or '-'}|{new_hash}|{prev_changed_at or '-'}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


@dataclass
class PollResult:
    run_id: str
    status: str  # success | partial | failed | skipped_locked
    coverage: str | None = None
    end_reason: str | None = None
    pages_fetched: int = 0
    http_requests: int = 0
    duration_ms: int = 0
    applications_inspected: int = 0
    scorecards_observed: int = 0
    scorecards_synced: int = 0
    scorecards_skipped: int = 0
    scorecards_not_seen: int = 0
    scorecards_in_state: int = 0
    interrupted_runs_recovered: int = 0
    baseline_established: bool = False
    scope_key: str | None = None
    events_by_type: dict[str, int] = field(default_factory=dict)
    skipped_by_reason: dict[str, int] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def events_created(self) -> int:
        return sum(self.events_by_type.values())


def _now() -> str:
    return datetime.now(UTC).isoformat()


def poll_once(
    client: PinpointClient,
    store: StateBackend,
    *,
    page_size: int,
    max_pages: int,
    include_confidential: bool = False,
    include_sourced: bool = False,
    now: Callable[[], str] = _now,
    run_id: str | None = None,
    secrets: tuple[str, ...] = (),
    policy: DetectionPolicy | None = None,
) -> PollResult:
    run_id = run_id or uuid.uuid4().hex
    log = {"run_id": run_id, "backend": getattr(store, "name", "unknown")}

    if not store.try_acquire_poller_lock():
        logger.warning("another poller instance holds the lock; skipping this run",
                       extra={**log, "event": "poll_skipped_locked"})
        return PollResult(run_id=run_id, status="skipped_locked",
                          reasons=["another poller instance is running"])
    try:
        return _locked_poll(
            client, store, run_id, log, now, secrets, policy or DetectionPolicy(),
            {
                "page_size": page_size,
                "max_pages": max_pages,
                "include_confidential": include_confidential,
                "include_sourced": include_sourced,
            },
        )
    finally:
        store.release_poller_lock()


def _locked_poll(
    client: PinpointClient,
    store: StateBackend,
    run_id: str,
    log: dict[str, Any],
    now: Callable[[], str],
    secrets: tuple[str, ...],
    policy: DetectionPolicy,
    discovery_kwargs: dict[str, Any],
) -> PollResult:
    started = time.monotonic()
    requests_before = client.request_count

    def elapsed_ms() -> int:
        return round((time.monotonic() - started) * 1000)

    recovered = store.mark_interrupted_runs(now())
    if recovered:
        logger.warning("marked stale runs as interrupted",
                       extra={**log, "event": "runs_interrupted", "count": recovered})
    scope = canonical_scope(build_filters(discovery_kwargs["include_confidential"],
                                          discovery_kwargs["include_sourced"]))
    key = scope_key(scope)
    log["scope_key"] = key[:12]
    store.start_run(run_id, now(), hostname=socket.gethostname(), pid=os.getpid(),
                    scope_key=key, scope=scope)
    logger.info("poll started", extra={
        **log, "event": "poll_started", "lock_backend_pid": getattr(store, "lock_backend_pid", None),
        **discovery_kwargs})

    try:
        run = run_discovery(client, **discovery_kwargs)
    except (PinpointError, ValueError) as exc:
        # Nothing was applied: state and events stay exactly as before.
        message = sanitize(f"{type(exc).__name__}: {exc}", secrets)
        http_requests = client.request_count - requests_before
        store.fail_run(run_id, now(), message,
                       {"duration_ms": elapsed_ms(), "http_requests": http_requests})
        logger.error("poll failed before applying changes", extra={
            **log, "event": "poll_failed", "error": message, "duration_ms": elapsed_ms(),
            "http_requests": http_requests})
        return PollResult(run_id=run_id, status="failed", error=message,
                          http_requests=http_requests, duration_ms=elapsed_ms(),
                          interrupted_runs_recovered=recovered,
                          scorecards_in_state=store.count_rows("scorecard_state"))

    http_requests = client.request_count - requests_before
    try:
        result = _apply(store, run, run_id, now(), http_requests=http_requests,
                        elapsed_ms=elapsed_ms, scope=scope, key=key, policy=policy)
    except Exception as exc:
        # The transaction was rolled back; record a sanitized failure and re-raise.
        message = sanitize(f"apply failed: {type(exc).__name__}", secrets)
        logger.error("persistence error; transaction rolled back", extra={
            **log, "event": "persistence_error", "error": message,
            "detail": sanitize(str(exc), secrets), "duration_ms": elapsed_ms()})
        try:
            store.fail_run(run_id, now(), message,
                           {"duration_ms": elapsed_ms(), "http_requests": http_requests})
        except Exception as record_exc:  # noqa: BLE001 - the connection may be gone
            logger.error("could not record failed run; it will be marked interrupted later",
                         extra={**log, "event": "persistence_error",
                                "error": type(record_exc).__name__})
        raise

    result.interrupted_runs_recovered = recovered
    for event_type, count in sorted(result.events_by_type.items()):
        logger.info("events detected", extra={
            **log, "event": "events_detected", "event_type": event_type, "count": count})
    for reason, count in sorted(result.skipped_by_reason.items()):
        level_r = logging.WARNING if reason.startswith("review") else logging.INFO
        logger.log(level_r, "events skipped", extra={
            **log, "event": "events_skipped", "reason": reason, "count": count})
    if result.baseline_established:
        logger.info("monitoring baseline established", extra={
            **log, "event": "baseline_established"})
    level = logging.INFO if result.status == "success" else logging.WARNING
    lock_pid = getattr(store, "lock_backend_pid", None)
    logger.log(level, "poll finished", extra={
        **log, "event": "poll_finished", "status": result.status, "coverage": result.coverage,
        "end_reason": result.end_reason, "duration_ms": result.duration_ms,
        "http_requests": result.http_requests, "pages_fetched": result.pages_fetched,
        "applications_inspected": result.applications_inspected,
        "scorecards_synced": result.scorecards_synced, "events_created": result.events_created,
        "lock_backend_pid": lock_pid, "lock_verified_before_commit": True,
        "reasons": result.reasons})
    return result


def _apply(
    store: StateBackend,
    run: DiscoveryRun,
    run_id: str,
    seen_at: str,
    *,
    http_requests: int = 0,
    elapsed_ms: Callable[[], int] = lambda: 0,
    scope: dict[str, Any] | None = None,
    key: str | None = None,
    policy: DetectionPolicy | None = None,
) -> PollResult:
    result = PollResult(
        run_id=run_id,
        status="success",
        coverage=run.coverage,
        end_reason=run.end_reason,
        pages_fetched=run.pages_fetched,
        http_requests=http_requests,
        applications_inspected=len(run.applications),
        scorecards_observed=len(run.scorecards),
    )
    if run.coverage != "complete":
        result.reasons.append(f"partial coverage: stopped after {run.pages_fetched} page(s)")
    unavailable = [a for a in run.applications if a.classification == APP_RELATIONSHIP_UNAVAILABLE]
    incomplete = [a for a in run.applications if a.classification == APP_INCOMPLETE_RESPONSE]
    if unavailable:
        result.reasons.append(f"{len(unavailable)} application(s) without user_scorecards relationship")
    if incomplete:
        result.reasons.append(f"{len(incomplete)} application(s) with scorecards missing from included")
    if run.duplicate_application_rows:
        result.reasons.append(f"{run.duplicate_application_rows} duplicate application row(s) across pages")
    if result.reasons:
        result.status = "partial"
    stage_by_app = {a.application_id: a.stage_id for a in run.applications}
    scope = scope or canonical_scope(run.scope.get("filters", {}))
    key = key or scope_key(scope)
    policy = policy or DetectionPolicy()
    result.scope_key = key

    events: Counter[str] = Counter()
    skipped: Counter[str] = Counter()
    with store.transaction() as conn:
        state = store.load_state()
        raw_ctx = store.detection_context(key, run_id)
        ctx = DetectionContext(
            scope_key=key,
            baseline_run_id=raw_ctx["baseline_run_id"],
            any_baseline=raw_ctx["any_baseline"],
            watermark_run_id=raw_ctx["watermark_run_id"],
            watermark_at=(datetime.fromisoformat(raw_ctx["watermark_at"]).astimezone(UTC)
                          if raw_ctx["watermark_at"] else None),
        )
        observed_keys: set[tuple[str, str]] = set()
        for obs in run.scorecards:
            if not obs.attributes_loaded:
                # Unknown attributes: leave the stored state untouched so no change is lost.
                result.scorecards_skipped += 1
                continue
            observed_keys.add(obs.key)
            prev = state.get(obs.key)
            new_hash = observation_hash(obs)
            if prev is None:
                sighting = classify_first_sighting(obs, ctx, seen_at, policy)
                planned = [(sighting.event_type, sighting.processing_status,
                            sighting.processing_reason, sighting.evidence)]
            else:
                planned = [(et, *initial_processing(et), None)
                           for et in derive_event_types(prev, obs)]
            for event_type, processing_status, processing_reason, evidence in planned:
                inserted = store.insert_event(conn, EventRow(
                    event_id=event_id(obs.application_id, obs.user_scorecard_id, event_type,
                                      prev.observation_hash if prev else None, new_hash,
                                      prev.last_changed_at if prev else None),
                    application_id=obs.application_id,
                    user_scorecard_id=obs.user_scorecard_id,
                    event_type=event_type,
                    previous_status=prev.status if prev else None,
                    new_status=obs.status,
                    previous_sentiment=prev.sentiment if prev else None,
                    new_sentiment=obs.sentiment,
                    detected_at=seen_at,
                    processed_at=None,
                    run_id=run_id,
                    processing_status=processing_status,
                    processing_reason=processing_reason,
                    evidence=evidence,
                ))
                if inserted:
                    events[event_type] += 1
                    if processing_status == "skipped" and processing_reason:
                        skipped[processing_reason] += 1
            store.upsert_state(conn, {
                "application_id": obs.application_id,
                "user_scorecard_id": obs.user_scorecard_id,
                "stage_id": stage_by_app.get(obs.application_id),
                "status": obs.status,
                "sentiment": obs.sentiment,
                "updated_at": obs.updated_at,
                "completed_at": obs.completed_at,
                "observation_hash": new_hash,
                "seen_at": seen_at,
                "run_id": run_id,
            })
            result.scorecards_synced += 1

        # Not seen != deleted. Only counted; stored state is kept as-is.
        result.scorecards_not_seen = len(state.keys() - observed_keys)
        if result.scorecards_not_seen and run.coverage == "complete":
            result.reasons.append(
                f"{result.scorecards_not_seen} stored scorecard(s) not seen in this run (kept, not deleted)"
            )
        result.events_by_type = dict(events)
        result.skipped_by_reason = dict(skipped)
        review = sum(c for r, c in skipped.items() if r.startswith("review"))
        if review:
            result.reasons.append(f"{review} new scorecard(s) recorded for review (not actionable)")
        # A baseline is established only by a run that saw the whole scope cleanly.
        if not ctx.baseline_established and result.status == "success":
            result.baseline_established = store.establish_baseline(
                conn, key, scope, run_id, seen_at)
        result.duration_ms = elapsed_ms()
        # Same session must still own the lock, or another instance could be writing.
        if not store.verify_poller_lock():
            raise PollerLockLostError("poller lock not held by this session; rolling back")
        store.finish_run(conn, run_id, {
            "finished_at": _now(),
            "duration_ms": result.duration_ms,
            "status": result.status,
            "coverage": result.coverage,
            "end_reason": result.end_reason,
            "pages_fetched": result.pages_fetched,
            "http_requests": result.http_requests,
            "applications_inspected": result.applications_inspected,
            "scorecards_observed": result.scorecards_observed,
            "scorecards_synced": result.scorecards_synced,
            "scorecards_skipped": result.scorecards_skipped,
            "scorecards_not_seen": result.scorecards_not_seen,
            "events_created": result.events_created,
            "reasons": result.reasons,
        })
    result.scorecards_in_state = store.count_rows("scorecard_state")
    return result
