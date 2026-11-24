"""Typed errors raised by the Pinpoint client.

Messages carry only the HTTP status and JSON:API error `code`/`title`, never
response bodies, request headers or the API key.
"""

from __future__ import annotations


class PinpointError(Exception):
    """Base class for all client errors."""


class ReadOnlyViolationError(PinpointError):
    """Raised if anything attempts a non-GET request."""


class PinpointAPIError(PinpointError):
    def __init__(self, status_code: int, path: str, reason: str = "") -> None:
        self.status_code = status_code
        self.path = path
        self.reason = reason
        message = f"HTTP {status_code} on GET {path}"
        if reason:
            message += f" ({reason})"
        super().__init__(message)


class PinpointAuthError(PinpointAPIError):
    """401: missing or invalid API key."""


class PinpointPermissionError(PinpointAPIError):
    """403: integration disabled or key lacks the required read permission."""


class PinpointNotFoundError(PinpointAPIError):
    """404: record or endpoint not found."""


class PinpointRateLimitError(PinpointAPIError):
    """429 that persisted after all retries."""


class PinpointTransportError(PinpointError):
    """Network/timeout failure that persisted after all retries."""
