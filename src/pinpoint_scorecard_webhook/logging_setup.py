"""Structured logging for journald/systemd.

Logs go to stderr (stdout is reserved for command output), one JSON object per
line by default. Structured fields are passed with `extra={...}`. A filter
redacts known secret values and credential-looking fragments from every record.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from collections.abc import Iterable
from datetime import UTC, datetime

_STANDARD_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {
    "message", "asctime", "taskName",
}

_CREDENTIAL_PATTERNS = (
    re.compile(r"(password|passwd|pwd)\s*=\s*\S+", re.IGNORECASE),
    re.compile(r"(x-api-key)\s*[:=]\s*\S+", re.IGNORECASE),
    re.compile(r"(postgres(?:ql)?://[^:/\s]+):[^@\s]+@", re.IGNORECASE),
)
MAX_ERROR_LENGTH = 500


def sanitize(text: str, secrets: Iterable[str] = ()) -> str:
    """Remove secret values and credential fragments; bound the length."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    text = _CREDENTIAL_PATTERNS[0].sub(r"\1=***", text)
    text = _CREDENTIAL_PATTERNS[1].sub(r"\1: ***", text)
    text = _CREDENTIAL_PATTERNS[2].sub(r"\1:***@", text)
    text = " ".join(text.split())
    return text[:MAX_ERROR_LENGTH]


class RedactingFilter(logging.Filter):
    def __init__(self, secrets: Iterable[str] = ()) -> None:
        super().__init__()
        self._secrets = [s for s in secrets if s]

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = sanitize(record.getMessage(), self._secrets)
        record.args = ()
        for key, value in list(vars(record).items()):
            if key not in _STANDARD_ATTRS and isinstance(value, str):
                setattr(record, key, sanitize(value, self._secrets))
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _STANDARD_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            # Exception type only: tracebacks can carry request data.
            payload["exc_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
        return json.dumps(payload, default=str)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        extras = " ".join(
            f"{k}={v}" for k, v in vars(record).items()
            if k not in _STANDARD_ATTRS and not k.startswith("_")
        )
        base = f"{datetime.fromtimestamp(record.created, UTC).isoformat()} {record.levelname} " \
               f"{record.name}: {record.getMessage()}"
        return f"{base} {extras}".rstrip()


def configure_logging(level: str = "INFO", fmt: str = "json", secrets: Iterable[str] = ()) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if fmt == "json" else TextFormatter())
    handler.addFilter(RedactingFilter(secrets))
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level.upper())
    # Library loggers would log full URLs/connection details at INFO/DEBUG.
    for noisy in ("httpx", "httpcore", "psycopg"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
