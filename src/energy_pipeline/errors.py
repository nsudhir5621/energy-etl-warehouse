"""Failure taxonomy.

The retry policy is not uniform because the failure *semantics* are not uniform.
Each class below carries an explicit ``retryable`` verdict, and the HTTP client
reads that verdict rather than deciding for itself. Assigning one blanket retry
policy to every failure is the mistake this module exists to prevent.

    Transient   — the source is unhealthy or our budget is spent. The same
                  request may well succeed later. RETRY.
    RateLimit   — a *budget* signal, not a *server* signal. RETRY, but only after
                  the server's own Retry-After has elapsed. Retrying sooner burns
                  the whole lockout window (measured at ~96 minutes).
    Client      — we asked for something wrong. RETRYING IS USELESS: a bad key or
                  a malformed filter will fail identically forever.
    Malformed   — the source returned something we cannot trust. Retrying usually
                  reproduces it, and a guess at the "intended" content would be
                  fabrication. RETRY ONLY IF the transport failed mid-body;
                  otherwise fail.
    Validation  — a deterministic rule about the data was violated. NEVER RETRY.
                  A retry cannot change the answer; only a new extraction can.
"""

from __future__ import annotations


class ExtractionError(Exception):
    """Base for everything this package raises."""

    retryable = False

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ConfigError(ExtractionError, ValueError):
    """Configuration is missing or nonsensical.

    Also a ``ValueError``, because a bad value is what it is — callers that catch
    ``ValueError`` around config parsing should keep working.
    """


class TransientSourceError(ExtractionError):
    """Timeout, connection reset, or a 5xx. The same request may succeed later."""

    retryable = True


class RateLimitError(TransientSourceError):
    """429. Retryable, but only after Retry-After has elapsed."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 429,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message, status_code=status_code)
        self.retry_after = retry_after


class ClientSourceError(ExtractionError):
    """4xx that is not 429. We sent a bad request; retrying cannot help."""


class MalformedResponseError(ExtractionError):
    """The source returned a body we cannot parse or that violates the envelope."""


class ValidationError(ExtractionError):
    """A deterministic data-quality rule failed. Never retryable."""


class StorageError(ExtractionError):
    """Raw storage failed."""


class StorageConfigError(StorageError):
    """Storage is missing or misconfigured. Fail here, at the boundary, rather
    than as an opaque SDK error several frames later."""


class ObjectExistsError(StorageError):
    """A create-only write hit an object that already exists.

    This is a *good* failure. It means the raw zone's immutability guarantee held:
    an unexpected path collision stopped the write instead of destroying history.
    """


class PartialUploadError(StorageError):
    """A run wrote some objects and then failed.

    The objects already written are deliberately **not** cleaned up. Raw history is
    append-only; the correct recovery is a fresh run under a new run id, not a
    rollback that would itself be a mutation of stored history.
    """

    def __init__(self, message: str, *, written_paths: list[str], failed_path: str) -> None:
        super().__init__(message)
        self.written_paths = written_paths
        self.failed_path = failed_path


def error_for_status(status_code: int, *, retry_after: float | None = None) -> ExtractionError:
    """Map an HTTP status onto the failure class that governs its retry policy."""
    if status_code == 429:
        return RateLimitError(
            f"rate limited by source (HTTP 429); Retry-After={retry_after}",
            retry_after=retry_after,
        )
    if status_code in (408, 425, 500, 502, 503, 504):
        return TransientSourceError(f"source error (HTTP {status_code})", status_code=status_code)
    if 400 <= status_code < 500:
        return ClientSourceError(
            f"request rejected by source (HTTP {status_code})", status_code=status_code
        )
    return ExtractionError(f"unexpected HTTP status {status_code}", status_code=status_code)
