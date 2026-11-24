"""Backend selection and database maintenance commands (db-check, db-migrate, migrate-sqlite)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from .config import AUTHORIZED_SCHEMA, Settings
from .store import StateBackend, StateStore


def backend_name(args: Any, settings: Settings) -> str:
    return getattr(args, "backend", None) or settings.state_backend


def open_store(settings: Settings, backend: str) -> StateBackend:
    if backend == "postgres":
        from .pg_store import PostgresStateStore

        return PostgresStateStore.from_settings(settings)
    return StateStore(settings.state_db_path)


def cmd_db_check(args: Any, settings: Settings, _client_factory: Any) -> int:
    """Read-only: connection identity, server version and privileges."""
    from .pg_migrations import migration_status
    from .pg_store import connect

    with connect(settings) as conn:
        info = conn.execute(
            "SELECT current_database() AS database, current_user AS current_user, "
            "session_user AS session_user, inet_server_addr()::text AS server_addr, "
            "inet_server_port() AS server_port, current_setting('server_version') AS version, "
            "current_setting('TimeZone') AS timezone"
        ).fetchone()
        role = conn.execute(
            "SELECT rolsuper, rolcreaterole, rolcreatedb FROM pg_roles WHERE rolname = current_user"
        ).fetchone()
        db_priv = conn.execute(
            "SELECT has_database_privilege(current_database(), 'CONNECT') AS connect, "
            "has_database_privilege(current_database(), 'CREATE') AS create_schema, "
            "has_database_privilege(current_database(), 'TEMP') AS temp"
        ).fetchone()
        schema = conn.execute(
            "SELECT n.nspname, pg_get_userbyid(n.nspowner) AS owner, "
            "has_schema_privilege(n.oid, 'USAGE') AS usage, "
            "has_schema_privilege(n.oid, 'CREATE') AS create_objects "
            "FROM pg_namespace n WHERE n.nspname = %s",
            (AUTHORIZED_SCHEMA,),
        ).fetchone()
        tables = []
        status: dict[str, Any] = {"applied": [], "pending": []}
        if schema:
            tables = conn.execute(
                "SELECT c.relname AS name, "
                "has_table_privilege(c.oid, 'SELECT') AS sel, "
                "has_table_privilege(c.oid, 'INSERT') AS ins, "
                "has_table_privilege(c.oid, 'UPDATE') AS upd "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relkind = 'r' ORDER BY c.relname",
                (AUTHORIZED_SCHEMA,),
            ).fetchall()
            status = migration_status(conn)

    print("Connection")
    for key in ("database", "version", "current_user", "session_user", "server_addr",
                "server_port", "timezone"):
        print(f"  {key:<16}: {info[key]}")
    print("Role attributes")
    print(f"  superuser       : {role['rolsuper']}")
    print(f"  createrole      : {role['rolcreaterole']}")
    print(f"  createdb        : {role['rolcreatedb']}")
    print("Database privileges")
    for key, value in db_priv.items():
        print(f"  {key:<16}: {value}")
    print(f"Schema {AUTHORIZED_SCHEMA}")
    if schema:
        print(f"  exists          : True (owner {schema['owner']})")
        print(f"  USAGE / CREATE  : {schema['usage']} / {schema['create_objects']}")
        for t in tables:
            print(f"  table {t['name']:<24} SELECT={t['sel']} INSERT={t['ins']} UPDATE={t['upd']}")
        print(f"  migrations      : applied={status['applied']} pending={status['pending']}")
    else:
        print("  exists          : False")
        print(f"  can create it   : {db_priv['create_schema']} (needs CREATE on the database)")
    return 0


def cmd_db_migrate(args: Any, settings: Settings, _client_factory: Any) -> int:
    from .pg_migrations import apply_migrations, migration_status
    from .pg_store import connect

    with connect(settings) as conn:
        applied = apply_migrations(conn, settings.db_schema)
        status = migration_status(conn, settings.db_schema)
    print(f"Schema           : {settings.db_schema}")
    print(f"Applied now      : {applied or 'none (already up to date)'}")
    print(f"Applied total    : {status['applied']}")
    print(f"Pending          : {status['pending'] or 'none'}")
    return 0


def cmd_migrate_sqlite(args: Any, settings: Settings, _client_factory: Any) -> int:
    from .history_migration import migrate_history, verify_history

    source = Path(args.source or settings.state_db_path)
    if not source.exists():
        print(f"{source} not found", file=sys.stderr)
        return 1
    backend = backend_name(args, settings)
    if backend != "postgres":
        print("migrate-sqlite targets PostgreSQL; use --backend postgres", file=sys.stderr)
        return 1
    with open_store(settings, backend) as target:
        if not args.verify_only:
            report = migrate_history(source, target)
            print("Table              source  inserted  already_present")
            for table in ("poll_runs", "scorecard_state", "detected_events"):
                print(f"{table:<18} {report.source_counts[table]:>6}  "
                      f"{report.inserted[table]:>8}  {report.already_present[table]:>15}")
        problems = verify_history(source, target)
        counts = {t: target.count_rows(t) for t in ("poll_runs", "scorecard_state", "detected_events")}
    print(f"\nTarget counts      : {counts}")
    if problems:
        print(f"Verification       : {len(problems)} mismatch(es)")
        for problem in problems[:50]:
            print(f"  - {problem}")
        return 1
    print("Verification       : OK (every source row present with identical ids, hashes and timestamps)")
    return 0
