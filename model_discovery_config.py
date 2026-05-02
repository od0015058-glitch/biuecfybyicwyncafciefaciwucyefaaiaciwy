"""DB-backed override layer for ``DISCOVERY_INTERVAL_SECONDS``.

Stage-15-Step-E #10b row 23. The model-discovery loop's cadence was
env-only (``DISCOVERY_INTERVAL_SECONDS``, default 6 h). This module
adds a DB-backed override slot so operators can tune the refresh
cadence from ``/admin/models-config`` without a redeploy.

Resolution order: in-process override → env → default (21 600 s / 6 h).
"""

from __future__ import annotations

import logging
import math
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from database import Database

log = logging.getLogger("bot.model_discovery_config")


# ------------------------------------------------------------------
# DISCOVERY_INTERVAL_SECONDS
# ------------------------------------------------------------------

DEFAULT_DISCOVERY_INTERVAL_SECONDS: int = 6 * 60 * 60  # 6 hours
DISCOVERY_INTERVAL_MINIMUM: int = 60  # 1 minute floor
DISCOVERY_INTERVAL_MAXIMUM: int = 7 * 24 * 60 * 60  # 1 week
DISCOVERY_INTERVAL_SETTING_KEY: str = "DISCOVERY_INTERVAL_SECONDS"

_DISCOVERY_INTERVAL_OVERRIDE: int | None = None


def _coerce_discovery_interval(value: object) -> int | None:
    """Validate a discovery-interval candidate.

    Returns the coerced integer on success or ``None`` on rejection.
    Rejects ``bool``, non-finite, non-integer-valued, or
    out-of-range values.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        coerced = value
    else:
        try:
            f = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(f):
            return None
        if not f.is_integer():
            return None
        coerced = int(f)
    if (
        coerced < DISCOVERY_INTERVAL_MINIMUM
        or coerced > DISCOVERY_INTERVAL_MAXIMUM
    ):
        return None
    return coerced


def set_discovery_interval_override(value: int) -> None:
    """Replace the in-process discovery-interval override."""
    global _DISCOVERY_INTERVAL_OVERRIDE
    if isinstance(value, bool):
        raise ValueError(
            "discovery interval override must be int, not bool"
        )
    coerced = _coerce_discovery_interval(value)
    if coerced is None:
        raise ValueError(
            f"discovery interval override {value!r} must be an "
            f"integer in [{DISCOVERY_INTERVAL_MINIMUM}, "
            f"{DISCOVERY_INTERVAL_MAXIMUM}]"
        )
    _DISCOVERY_INTERVAL_OVERRIDE = coerced


def clear_discovery_interval_override() -> bool:
    """Drop the in-process override.  Returns True if one was active."""
    global _DISCOVERY_INTERVAL_OVERRIDE
    had = _DISCOVERY_INTERVAL_OVERRIDE is not None
    _DISCOVERY_INTERVAL_OVERRIDE = None
    return had


def get_discovery_interval_override() -> int | None:
    """Return the current in-process override (or ``None``)."""
    return _DISCOVERY_INTERVAL_OVERRIDE


async def refresh_discovery_interval_override_from_db(
    db: "Database | None",
) -> int | None:
    """Reload the override from ``system_settings``.

    Transient DB errors keep the previous cache; malformed values
    are treated as "no override".
    """
    global _DISCOVERY_INTERVAL_OVERRIDE
    if db is None:
        return _DISCOVERY_INTERVAL_OVERRIDE
    try:
        raw = await db.get_setting(DISCOVERY_INTERVAL_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_discovery_interval_override_from_db: "
            "get_setting failed; keeping previous cache value=%s",
            _DISCOVERY_INTERVAL_OVERRIDE,
        )
        return _DISCOVERY_INTERVAL_OVERRIDE
    if raw is None:
        _DISCOVERY_INTERVAL_OVERRIDE = None
        return None
    coerced = _coerce_discovery_interval(raw)
    if coerced is None:
        log.warning(
            "refresh_discovery_interval_override_from_db: "
            "rejected stored value %r; clearing override",
            raw,
        )
        _DISCOVERY_INTERVAL_OVERRIDE = None
        return None
    _DISCOVERY_INTERVAL_OVERRIDE = coerced
    return coerced


def get_discovery_interval_seconds() -> int:
    """Return the resolved discovery interval in seconds.

    Resolution order: in-process override → env → default (21600).
    """
    if _DISCOVERY_INTERVAL_OVERRIDE is not None:
        return _DISCOVERY_INTERVAL_OVERRIDE
    raw = os.getenv("DISCOVERY_INTERVAL_SECONDS")
    if raw is not None:
        coerced = _coerce_discovery_interval(raw)
        if coerced is not None:
            return coerced
    return DEFAULT_DISCOVERY_INTERVAL_SECONDS


def get_discovery_interval_source() -> str:
    """Return ``db`` / ``env`` / ``default``."""
    if _DISCOVERY_INTERVAL_OVERRIDE is not None:
        return "db"
    raw = os.getenv("DISCOVERY_INTERVAL_SECONDS")
    if raw is not None and _coerce_discovery_interval(raw) is not None:
        return "env"
    return "default"


def format_interval_human(seconds: int) -> str:
    """Format seconds as a human-readable duration string."""
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{m}m" if s == 0 else f"{m}m {s}s"
    h, remainder = divmod(seconds, 3600)
    m = remainder // 60
    return f"{h}h" if m == 0 else f"{h}h {m}m"
