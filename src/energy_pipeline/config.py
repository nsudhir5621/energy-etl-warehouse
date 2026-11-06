"""Environment configuration with lazy, protected EIA credential fallback.

Live entry points prefer EIA_API_KEY, then ~/.config/energy-etl/eia.env.
Offline configuration never reads the private file. No credential is defaulted,
written, logged or included in Settings repr; missing keys remain None.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import ConfigError

__all__ = ["MAX_PAGE_SIZE", "ConfigError", "Settings"]

DEFAULT_BASE_URL = "https://api.eia.gov/v2"
DEFAULT_ROUTE = "electricity/electric-power-operational-data"
DEFAULT_FREQUENCY = "monthly"
DEFAULT_MEASURE = "generation"

#: The API refuses to return more than 5000 rows in JSON format.
MAX_PAGE_SIZE = 5000


def _env(env: Mapping[str, str], key: str, default: Any, cast: Callable[[str], Any]) -> Any:
    """Read one environment variable, falling back to the dataclass default.

    A blank value means "unset", not "empty": an ``EIA_API_KEY=`` in a shell
    profile should behave exactly like an absent one, not poison every later read.
    """
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    try:
        return cast(raw)
    except ValueError as exc:
        raise ConfigError(f"{key} is not a valid {cast.__name__}: {raw!r}") from exc


def load_api_key(env: Mapping[str, str] | None = None) -> str | None:
    """Resolve a live credential without changing the process environment.

    Call only for live operations. A nonempty environment value wins without
    inspecting the optional private file. File syntax is one literal
    EIA_API_KEY assignment, optionally quoted, plus blank/comment lines.
    """
    env = os.environ if env is None else env
    if key := env.get("EIA_API_KEY"):
        return key

    home = Path.home()
    path = home / ".config" / "energy-etl" / "eia.env"
    try:
        source = path.lstat()
    except FileNotFoundError:
        return None
    except OSError:
        raise ConfigError("private EIA credential file cannot be inspected") from None

    if not hasattr(os, "getuid") or not hasattr(os, "O_NOFOLLOW"):
        raise ConfigError("private EIA credential protection requires a POSIX filesystem")
    uid = os.getuid()
    try:
        for directory in (home, home / ".config", path.parent):
            metadata = directory.lstat()
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != uid
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                raise ConfigError("private EIA credential directory is unsafe")
        if stat.S_IMODE(path.parent.stat().st_mode) != 0o700:
            raise ConfigError("private EIA credential directory must have mode 0700")
        if (
            not stat.S_ISREG(source.st_mode)
            or source.st_uid != uid
            or source.st_nlink != 1
            or stat.S_IMODE(source.st_mode) not in (0o400, 0o600)
            or source.st_size > 4096
        ):
            raise ConfigError("private EIA credential file must be owner-only and regular")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            opened = os.fstat(stream.fileno())
            if (opened.st_dev, opened.st_ino, opened.st_uid, opened.st_mode, opened.st_nlink) != (
                source.st_dev,
                source.st_ino,
                source.st_uid,
                source.st_mode,
                source.st_nlink,
            ):
                raise ConfigError("private EIA credential file changed during loading")
            raw = stream.read(4097)
            after = os.fstat(stream.fileno())
            if (
                len(raw) > 4096
                or after.st_size != source.st_size
                or after.st_mtime_ns != source.st_mtime_ns
                or (after.st_uid, after.st_mode, after.st_nlink)
                != (source.st_uid, source.st_mode, source.st_nlink)
            ):
                raise ConfigError("private EIA credential file changed during loading")
        text = raw.decode("utf-8")
    except (OSError, UnicodeError):
        raise ConfigError("private EIA credential file cannot be read securely") from None

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    lines = [line for line in lines if not line.startswith("#")]
    if not lines:
        return None
    if len(lines) != 1 or not lines[0].startswith("EIA_API_KEY="):
        raise ConfigError("private EIA credential file requires one EIA_API_KEY assignment")
    key = lines[0].partition("=")[2].strip()
    if key.startswith(("'", '"')):
        if len(key) < 2 or key[-1] != key[0]:
            raise ConfigError("private EIA credential assignment has invalid quoting")
        key = key[1:-1]
    if not key:
        return None
    if not key.isascii() or not all(char.isalnum() or char in "-_" for char in key):
        raise ConfigError("private EIA credential assignment has invalid characters")
    return key


@dataclass(frozen=True)
class Settings:
    """Runtime settings. Frozen so a run cannot mutate its own configuration."""

    api_key: str | None = field(repr=False)
    base_url: str = DEFAULT_BASE_URL
    route: str = DEFAULT_ROUTE
    frequency: str = DEFAULT_FREQUENCY
    measure: str = DEFAULT_MEASURE
    page_size: int = MAX_PAGE_SIZE
    timeout_seconds: float = 30.0
    max_retries: int = 4
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    #: A 429 can lock the key out for over an hour (measured: ~96 minutes). Honour
    #: the header, but cap it so a pathological value cannot hang an unattended
    #: run for days. The cap is generous enough that a real lockout is never
    #: truncated — a too-short cap would just spend one wasted request.
    max_retry_after_seconds: float = 21600.0
    #: Self-throttle before the budget hits zero. The API publishes
    #: ``X-Ratelimit-Limit`` / ``X-Ratelimit-Remaining``; we read those rather than
    #: guessing a constant that would be wrong for every key but one.
    min_request_interval_seconds: float = 0.2
    #: Initial operational revision lookback. Deliberately conservative; the real
    #: revision horizon is not yet measured. See docs/source-data.md.
    lookback_months: int = 24
    #: Filesystem root for the raw zone; logical object keys resolve under it.
    raw_root: Path = Path("data/raw")
    #: Logical object-key prefix. One value for every real run; tests override
    #: it so their objects cannot be mistaken for real raw data.
    raw_prefix: str = "raw/eia"

    @classmethod
    def from_env(
        cls, env: Mapping[str, str] | None = None, *, require_api_key: bool = False
    ) -> Settings:
        env = os.environ if env is None else env
        page_size = _env(env, "EIA_PAGE_SIZE", MAX_PAGE_SIZE, int)
        if not 1 <= page_size <= MAX_PAGE_SIZE:
            raise ConfigError(
                f"EIA_PAGE_SIZE must be between 1 and {MAX_PAGE_SIZE} (the API "
                f"will not return more than {MAX_PAGE_SIZE} rows in JSON), got {page_size}"
            )
        max_retries = _env(env, "EIA_MAX_RETRIES", 4, int)
        if max_retries < 0:
            raise ConfigError("EIA_MAX_RETRIES must be >= 0")
        lookback = _env(env, "EIA_LOOKBACK_MONTHS", 24, int)
        if lookback < 1:
            raise ConfigError("EIA_LOOKBACK_MONTHS must be >= 1")
        return cls(
            api_key=load_api_key(env) if require_api_key else env.get("EIA_API_KEY") or None,
            base_url=_env(env, "EIA_BASE_URL", DEFAULT_BASE_URL, str).rstrip("/"),
            route=_env(env, "EIA_ROUTE", DEFAULT_ROUTE, str).strip("/"),
            frequency=_env(env, "EIA_FREQUENCY", DEFAULT_FREQUENCY, str),
            measure=_env(env, "EIA_MEASURE", DEFAULT_MEASURE, str),
            page_size=page_size,
            timeout_seconds=_env(env, "EIA_TIMEOUT_SECONDS", 30.0, float),
            max_retries=max_retries,
            backoff_base_seconds=_env(env, "EIA_BACKOFF_BASE_SECONDS", 1.0, float),
            backoff_max_seconds=_env(env, "EIA_BACKOFF_MAX_SECONDS", 60.0, float),
            max_retry_after_seconds=_env(env, "EIA_MAX_RETRY_AFTER_SECONDS", 21600.0, float),
            min_request_interval_seconds=_env(env, "EIA_MIN_REQUEST_INTERVAL_SECONDS", 0.2, float),
            lookback_months=lookback,
            raw_root=Path(_env(env, "RAW_ROOT", "data/raw", str)),
            raw_prefix=_env(env, "RAW_PREFIX", "raw/eia", str).strip("/"),
        )
