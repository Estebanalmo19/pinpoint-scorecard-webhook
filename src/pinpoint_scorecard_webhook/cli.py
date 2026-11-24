"""Read-only Pinpoint scorecard tooling.

`discover` / `discover-diff` scan applications account-wide (bounded by
--max-pages). The other commands target a single application (APPLICATION_ID
or --application-id). Only GET requests are issued.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from pydantic import ValidationError

from .client import PinpointClient
from .config import Settings
from .discovery import (
    DiscoveryRun,
    diff_runs,
    list_runs,
    load_run,
    run_discovery,
    save_run,
    summarize,
)
from .errors import PinpointError, PinpointNotFoundError, PinpointPermissionError
from .poller import PollResult, poll_once
from .snapshot import (
    ApplicationSnapshot,
    diff_snapshots,
    load_snapshot,
    save_snapshot,
    snapshot_from_document,
    snapshot_path,
)
from .store import StateStore

EXIT_OK, EXIT_ERROR, EXIT_CONFIG = 0, 1, 2

ClientFactory = Callable[[Settings], PinpointClient]


# ----------------------------------------------------------------------- output


def _v(value: object) -> str:
    return "-" if value is None else str(value)


def render_snapshot(snap: ApplicationSnapshot) -> str:
    lines = [
        f"Application ID         : {snap.application_id}",
        f"Candidate ID           : {_v(snap.candidate_id)}",
        f"Stage ID               : {_v(snap.stage_id)}",
        f"Application updated_at : {_v(snap.updated_at)}",
        f"Captured at            : {snap.captured_at}",
        "",
    ]
    if not snap.user_scorecards_loaded:
        lines.append("User scorecards: relationship NOT returned by the API (unknown, not empty)")
        return "\n".join(lines)
    if not snap.user_scorecards:
        lines.append("User scorecards: none")
        return "\n".join(lines)

    headers = ("User Scorecard ID", "status", "completed_at", "updated_at", "sentiment")
    rows = [
        (s.id, _v(s.status), _v(s.completed_at), _v(s.updated_at), _v(s.sentiment))
        if s.attributes_loaded
        else (s.id, "(not included)", "-", "-", "-")
        for s in snap.user_scorecards
    ]
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    lines += [fmt.format(*headers), fmt.format(*("-" * w for w in widths))]
    lines += [fmt.format(*row) for row in rows]
    return "\n".join(lines)


# --------------------------------------------------------------------- commands


def _resolve_app_id(args: argparse.Namespace, settings: Settings) -> str:
    app_id = args.application_id or settings.application_id
    if not app_id:
        raise SystemExit("APPLICATION_ID is not set (use .env or --application-id)")
    if not str(app_id).isdigit():
        raise SystemExit("application id must be numeric")
    return str(app_id)


def _fetch_snapshot(client: PinpointClient, app_id: str) -> ApplicationSnapshot:
    return snapshot_from_document(client.get_application(app_id))


def cmd_inspect(args, settings, client_factory) -> int:
    app_id = _resolve_app_id(args, settings)
    with client_factory(settings) as client:
        snap = _fetch_snapshot(client, app_id)
    print(render_snapshot(snap))
    return EXIT_OK


def cmd_snapshot_before(args, settings, client_factory) -> int:
    app_id = _resolve_app_id(args, settings)
    path = snapshot_path(settings.snapshot_dir, app_id, "before")
    if path.exists() and not args.force:
        print(f"{path} already exists; use --force to overwrite", file=sys.stderr)
        return EXIT_ERROR
    with client_factory(settings) as client:
        snap = _fetch_snapshot(client, app_id)
    save_snapshot(snap, path)
    print(render_snapshot(snap))
    print(f"\nSaved: {path}")
    return EXIT_OK


def _print_and_save_diff(before: ApplicationSnapshot, after: ApplicationSnapshot, settings) -> None:
    changes = diff_snapshots(before, after)
    print(f"\nComparison {before.captured_at} -> {after.captured_at}")
    if not changes:
        print("No changes detected.")
    for change in changes:
        print("  " + change.describe())
    diff_path = snapshot_path(settings.snapshot_dir, after.application_id, "diff")
    diff_path.write_text(
        json.dumps(
            {
                "application_id": after.application_id,
                "before_captured_at": before.captured_at,
                "after_captured_at": after.captured_at,
                "changes": [c.__dict__ for c in changes],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\nSaved: {diff_path}")


def cmd_snapshot_after(args, settings, client_factory) -> int:
    app_id = _resolve_app_id(args, settings)
    before_path = snapshot_path(settings.snapshot_dir, app_id, "before")
    if not before_path.exists():
        print(f"{before_path} not found; run snapshot-before first", file=sys.stderr)
        return EXIT_ERROR
    before = load_snapshot(before_path)
    with client_factory(settings) as client:
        after = _fetch_snapshot(client, app_id)
    after_path = snapshot_path(settings.snapshot_dir, app_id, "after")
    save_snapshot(after, after_path)
    print(render_snapshot(after))
    print(f"\nSaved: {after_path}")
    _print_and_save_diff(before, after, settings)
    return EXIT_OK


def cmd_compare(args, settings, client_factory) -> int:
    """Re-run the comparison from saved files, without calling the API."""
    app_id = _resolve_app_id(args, settings)
    paths = [snapshot_path(settings.snapshot_dir, app_id, label) for label in ("before", "after")]
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        print(f"missing snapshot(s): {', '.join(missing)}", file=sys.stderr)
        return EXIT_ERROR
    _print_and_save_diff(load_snapshot(paths[0]), load_snapshot(paths[1]), settings)
    return EXIT_OK


def cmd_probe_user_scorecards(args, settings, client_factory) -> int:
    """Check whether the undocumented GET /user_scorecards/{id} is reachable.

    Uses only scorecard ids already linked to the test application.
    """
    app_id = _resolve_app_id(args, settings)
    with client_factory(settings) as client:
        snap = _fetch_snapshot(client, app_id)
        if not snap.user_scorecards:
            print("Test application has no user scorecards to probe.")
            return EXIT_OK
        target = snap.user_scorecards[0]
        try:
            doc = client.get_user_scorecard(target.id)
        except PinpointNotFoundError as exc:
            print(f"GET /user_scorecards/{target.id}: not available ({exc})")
            return EXIT_OK
        except PinpointPermissionError as exc:
            print(f"GET /user_scorecards/{target.id}: forbidden ({exc})")
            return EXIT_OK
    data = doc.get("data") if isinstance(doc, dict) else None
    attrs = (data or {}).get("attributes") or {}
    print(f"GET /user_scorecards/{target.id}: 200 OK")
    print(f"  resource type      : {_v((data or {}).get('type'))}")
    print(f"  attribute keys     : {', '.join(sorted(attrs)) or '-'}")
    print(f"  completed_at match : {attrs.get('completed_at') == target.completed_at}")
    return EXIT_OK


def render_discovery(run: DiscoveryRun) -> str:
    headers = ("application_id", "stage_id", "application_updated_at", "classification",
               "user_scorecard_id", "status", "updated_at", "completed_at", "sentiment", "flag")
    by_app: dict[str, list] = {}
    for sc in run.scorecards:
        by_app.setdefault(sc.application_id, []).append(sc)
    rows: list[tuple[str, ...]] = []
    for app in run.applications:
        base = (app.application_id, _v(app.stage_id), _v(app.updated_at), app.classification)
        scs = by_app.get(app.application_id, [])
        if not scs:
            rows.append((*base, "-", "-", "-", "-", "-", "-"))
        for sc in scs:
            if sc.attributes_loaded:
                rows.append((*base, sc.user_scorecard_id, _v(sc.status), _v(sc.updated_at),
                             _v(sc.completed_at), _v(sc.sentiment), _v(sc.discrepancy)))
            else:
                rows.append((*base, sc.user_scorecard_id, "(not included)", "-", "-", "-", "-"))
    if not rows:
        return "No applications returned."
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    lines = [fmt.format(*headers), fmt.format(*("-" * w for w in widths))]
    return "\n".join(lines + [fmt.format(*r) for r in rows])


def render_summary(summary: dict) -> str:
    lines = ["Summary"]
    for key, value in summary.items():
        if isinstance(value, dict):
            inner = ", ".join(f"{k}={v}" for k, v in sorted(value.items())) or "-"
            lines.append(f"  {key:<34}: {inner}")
        else:
            lines.append(f"  {key:<34}: {_v(value)}")
    return "\n".join(lines)


def cmd_discover(args, settings, client_factory) -> int:
    max_pages = settings.pinpoint_max_pages if args.all else args.max_pages
    page_size = args.page_size or settings.pinpoint_page_size
    with client_factory(settings) as client:
        run = run_discovery(
            client,
            page_size=page_size,
            max_pages=max_pages,
            include_confidential=args.include_confidential,
            include_sourced=args.include_sourced,
        )
    if not args.summary_only:
        print(render_discovery(run))
        print()
    print(render_summary(summarize(run)))
    if run.coverage == "partial":
        print(f"\nCoverage PARTIAL: stopped at max_pages={max_pages} before the end of the "
              "collection. Records beyond this point were not reviewed.")
    if not args.no_save:
        print(f"\nSaved: {save_run(run, settings.snapshot_dir)}")
    return EXIT_OK


def cmd_discover_diff(args, settings, client_factory) -> int:
    """Compare two saved discovery runs (no API call)."""
    if args.before and args.after:
        before_path, after_path = args.before, args.after
    else:
        runs = list_runs(settings.snapshot_dir)
        if len(runs) < 2:
            print("need at least two saved discovery runs (or --before/--after)", file=sys.stderr)
            return EXIT_ERROR
        before_path, after_path = runs[-2], runs[-1]
    before, after = load_run(before_path), load_run(after_path)
    changes = diff_runs(before, after)
    print(f"Before: {before_path} ({before.captured_at}, coverage={before.coverage})")
    print(f"After : {after_path} ({after.captured_at}, coverage={after.coverage})")
    if not changes:
        print("No changes detected.")
    for change in changes:
        print("  " + change.describe())
    counts: dict[str, int] = {}
    for change in changes:
        counts[change.kind] = counts.get(change.kind, 0) + 1
    if counts:
        print("\nChange counts: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    return EXIT_OK


def render_poll_result(result: PollResult) -> str:
    lines = [
        f"Run ID                   : {result.run_id}",
        f"Run status               : {result.status}",
        f"Coverage                 : {_v(result.coverage)} (end_reason={_v(result.end_reason)})",
        f"Pages fetched            : {result.pages_fetched}",
        f"Applications inspected   : {result.applications_inspected}",
        f"Scorecards observed      : {result.scorecards_observed}",
        f"Scorecards synced        : {result.scorecards_synced}",
        f"Scorecards skipped       : {result.scorecards_skipped}",
        f"Stored, not seen this run: {result.scorecards_not_seen}",
        f"Scorecards in state      : {result.scorecards_in_state}",
        "Events created           : " + (
            ", ".join(f"{k}={v}" for k, v in sorted(result.events_by_type.items())) or "none"
        ),
    ]
    for reason in result.reasons:
        lines.append(f"Reason                   : {reason}")
    if result.error:
        lines.append(f"Error                    : {result.error}")
    return "\n".join(lines)


def cmd_poll_once(args, settings, client_factory) -> int:
    max_pages = args.max_pages or settings.pinpoint_max_pages
    page_size = args.page_size or settings.pinpoint_page_size
    with StateStore(settings.state_db_path) as store, client_factory(settings) as client:
        result = poll_once(
            client,
            store,
            page_size=page_size,
            max_pages=max_pages,
            include_confidential=args.include_confidential,
            include_sourced=args.include_sourced,
        )
    print(render_poll_result(result))
    return EXIT_ERROR if result.status == "failed" else EXIT_OK


def cmd_events(args, settings, client_factory) -> int:
    """List recent detected events from SQLite (no API call)."""
    if not settings.state_db_path.exists():
        print(f"{settings.state_db_path} not found; run poll-once first", file=sys.stderr)
        return EXIT_ERROR
    with StateStore(settings.state_db_path) as store:
        events = store.recent_events(
            limit=args.limit, event_types=args.type or (), unprocessed_only=args.unprocessed
        )
    if not events:
        print("No events.")
        return EXIT_OK
    headers = ("detected_at", "event_type", "application_id", "user_scorecard_id",
               "prev_status", "new_status", "prev_sentiment", "new_sentiment", "processed_at",
               "event_id")
    rows = [
        (e.detected_at, e.event_type, e.application_id, e.user_scorecard_id,
         _v(e.previous_status), _v(e.new_status), _v(e.previous_sentiment),
         _v(e.new_sentiment), _v(e.processed_at), e.event_id[:12])
        for e in events
    ]
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print(fmt.format(*("-" * w for w in widths)))
    for row in rows:
        print(fmt.format(*row))
    return EXIT_OK


COMMANDS = {
    "poll-once": (cmd_poll_once, "One polling pass: sync scorecards to SQLite, emit events"),
    "events": (cmd_events, "Show recent detected events from SQLite (no API call)"),
    "discover": (cmd_discover, "Scan applications and user scorecards (bounded, read-only)"),
    "discover-diff": (cmd_discover_diff, "Compare the two latest discovery runs (no API call)"),
    "inspect": (cmd_inspect, "Show technical metadata of the test application"),
    "snapshot-before": (cmd_snapshot_before, "Capture the 'before' snapshot"),
    "snapshot-after": (cmd_snapshot_after, "Capture the 'after' snapshot and compare"),
    "compare": (cmd_compare, "Compare saved before/after snapshots (no API call)"),
    "probe-user-scorecards": (
        cmd_probe_user_scorecards,
        "Optional: test the undocumented /user_scorecards/{id} endpoint",
    ),
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pinpoint-scorecards", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name, (_, help_text) in COMMANDS.items():
        p = sub.add_parser(name, help=help_text)
        if name == "discover":
            limit = p.add_mutually_exclusive_group(required=True)
            limit.add_argument("--max-pages", type=_positive_int,
                               help="stop after N pages (must be <= PINPOINT_MAX_PAGES)")
            limit.add_argument("--all", action="store_true",
                               help="page to the end, bounded by PINPOINT_MAX_PAGES")
            p.add_argument("--page-size", type=_positive_int, help="1..1000 (default from .env)")
            p.add_argument("--include-confidential", action="store_true",
                           help="add filter[job_visibility] with all values (API excludes "
                                "confidential jobs by default)")
            p.add_argument("--include-sourced", action="store_true",
                           help="add filter[stage_basic_type] with all values (API excludes "
                                "the sourced stage by default)")
            p.add_argument("--summary-only", action="store_true", help="do not print rows")
            p.add_argument("--no-save", action="store_true", help="do not save the run")
            continue
        if name == "poll-once":
            p.add_argument("--max-pages", type=_positive_int,
                           help="default and maximum: PINPOINT_MAX_PAGES")
            p.add_argument("--page-size", type=_positive_int, help="1..1000 (default from .env)")
            p.add_argument("--include-confidential", action="store_true")
            p.add_argument("--include-sourced", action="store_true")
            continue
        if name == "events":
            p.add_argument("--limit", type=_positive_int, default=20)
            p.add_argument("--type", action="append", help="filter by event type (repeatable)")
            p.add_argument("--unprocessed", action="store_true", help="only processed_at IS NULL")
            continue
        if name == "discover-diff":
            p.add_argument("--before", type=Path, help="path of the earlier run")
            p.add_argument("--after", type=Path, help="path of the later run")
            continue
        p.add_argument("--application-id", help="override APPLICATION_ID from .env")
        if name == "snapshot-before":
            p.add_argument("--force", action="store_true", help="overwrite an existing snapshot")
    return parser


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return number


def _configure_logging(level: str) -> None:
    logging.basicConfig(level=level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # httpx logs every request URL at INFO; keep it quiet.
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def main(
    argv: Sequence[str] | None = None,
    *,
    settings: Settings | None = None,
    client_factory: ClientFactory = PinpointClient,
) -> int:
    args = build_parser().parse_args(argv)
    if settings is None:
        try:
            settings = Settings()
        except ValidationError as exc:
            # Print field names and messages only, never input values.
            for err in exc.errors():
                field = ".".join(str(p) for p in err["loc"]).upper()
                print(f"config error: {field}: {err['msg']}", file=sys.stderr)
            return EXIT_CONFIG
    _configure_logging(settings.log_level)
    handler = COMMANDS[args.command][0]
    try:
        return handler(args, settings, client_factory)
    except PinpointError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
