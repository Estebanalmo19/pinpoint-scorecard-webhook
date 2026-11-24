"""Runtime configuration loaded from environment variables / `.env`."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    read_only: bool = True
    application_id: str | None = None
    snapshot_dir: Path = Path("snapshots")
    # Local SQLite state for the polling prototype (git-ignored).
    state_db_path: Path = Path("state/pinpoint_state.sqlite3")
    log_level: str = "INFO"

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

    @property
    def base_url(self) -> str:
        return f"https://{self.pinpoint_subdomain}.pinpointhq.com/api/v1"
