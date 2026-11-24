"""Runtime configuration loaded from environment variables / `.env`."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# The only schema this service is authorised to create objects in or write to.
AUTHORIZED_SCHEMA = "pinpoint_scorecard_webhook"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pinpoint_subdomain: str = Field(..., min_length=1)
    pinpoint_api_key: SecretStr
    pinpoint_timeout_seconds: float = Field(30.0, gt=0, le=120)
    # page[size]: default 100, max 1000 per the OpenAPI spec.
    pinpoint_page_size: int = Field(100, ge=1, le=1000)
    pinpoint_max_retries: int = Field(3, ge=0, le=10)
    pinpoint_backoff_base_seconds: float = Field(1.0, ge=0, le=30)
    pinpoint_backoff_max_seconds: float = Field(30.0, ge=0, le=300)
    # Hard ceiling on pages per list run. Exceeding it is an explicit error, and a
    # run that stops at --max-pages before the end is reported as partial.
    pinpoint_max_pages: int = Field(200, ge=1, le=10000)
    # Pause between consecutive pages to stay well below undocumented rate limits.
    pinpoint_page_delay_seconds: float = Field(0.5, ge=0, le=30)

    # First sighting of a complete scorecard counts as a NEW completion only if
    # completed_at >= last full observation - tolerance, and that observation is
    # not older than the max gap. Otherwise it is recorded for review.
    new_completion_tolerance_seconds: int = Field(300, ge=0, le=3600)
    max_observation_gap_hours: float = Field(24.0, gt=0, le=720)

    read_only: bool = True
    application_id: str | None = None
    snapshot_dir: Path = Path("snapshots")
    # Local SQLite state for the polling prototype (git-ignored).
    state_db_path: Path = Path("state/pinpoint_state.sqlite3")
    log_level: str = "INFO"
    # json (one object per line, for journald) or text (human-readable).
    log_format: Literal["json", "text"] = "json"

    # State backend: sqlite (local tests) or postgres (VM1).
    state_backend: Literal["sqlite", "postgres"] = "sqlite"
    db_host: str | None = None
    db_port: int = Field(5432, ge=1, le=65535)
    db_name: str | None = None
    db_user: str | None = None
    db_password: SecretStr | None = None
    db_schema: str = AUTHORIZED_SCHEMA
    db_connect_timeout_seconds: int = Field(10, ge=1, le=120)
    db_statement_timeout_ms: int = Field(30000, ge=1000, le=600000)

    @field_validator("pinpoint_subdomain")
    @classmethod
    def _validate_subdomain(cls, value: str) -> str:
        value = value.strip().lower()
        # Prevent host injection: a subdomain label only.
        if not value.replace("-", "").isalnum() or value.startswith("-") or value.endswith("-"):
            raise ValueError("PINPOINT_SUBDOMAIN must be a bare subdomain label, e.g. 'acme'")
        return value

    @field_validator("read_only")
    @classmethod
    def _enforce_read_only(cls, value: bool) -> bool:
        if not value:
            raise ValueError("READ_ONLY must be true: this tool only supports read operations")
        return value

    @field_validator("application_id")
    @classmethod
    def _validate_application_id(cls, value: str | None) -> str | None:
        if value is None or value.strip() == "":
            return None
        value = value.strip()
        if not value.isdigit():
            raise ValueError("APPLICATION_ID must be a numeric Pinpoint application id")
        return value

    @field_validator("db_schema")
    @classmethod
    def _enforce_schema(cls, value: str) -> str:
        if value != AUTHORIZED_SCHEMA:
            raise ValueError(f"DB_SCHEMA must be '{AUTHORIZED_SCHEMA}' (the only authorised schema)")
        return value

    def require_postgres(self) -> None:
        missing = [n for n in ("db_host", "db_name", "db_user", "db_password") if not getattr(self, n)]
        if missing:
            raise ValueError("missing PostgreSQL settings: " + ", ".join(m.upper() for m in missing))

    def secret_values(self) -> list[str]:
        """Values that must never appear in logs or stored errors."""
        values = [self.pinpoint_api_key.get_secret_value()]
        if self.db_password:
            values.append(self.db_password.get_secret_value())
        return [v for v in values if v]

    @property
    def base_url(self) -> str:
        return f"https://{self.pinpoint_subdomain}.pinpointhq.com/api/v1"
