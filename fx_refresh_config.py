"""DB-backed override layer for ``FX_REFRESH_INTERVAL_SECONDS``.

Stage-15-Step-E #10b row 24. The USD→Toman FX refresher's cadence
was env-only (``FX_REFRESH_INTERVAL_SECONDS``, default 10 min).
This module adds a DB-backed override slot so operators can tune
the refresh cadence from ``/admin/wallet-config`` without a
redeploy.

Resolution order: in-process override → env → default (600 s / 10 min).

Same shape as :mod:`model_discovery_config` (Stage-15-Step-E #10b
row 23): module-level cache, coercion validator, set/clear/get,
``refresh_*_from_db`` helper, ``get_*_seconds`` resolver,
``get_*_source`` reporter.
"""

from __future__ import annotations

import logging
import math
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from database import Database

log = logging.getLogger("bot.fx_refresh_config")


# ------------------------------------------------------------------
# FX_REFRESH_INTERVAL_SECONDS
# ------------------------------------------------------------------

DEFAULT_FX_REFRESH_INTERVAL_SECONDS: int = 10 * 60  # 10 minutes
FX_REFRESH_INTERVAL_MINIMUM: int = 60  # 1 minute floor
FX_REFRESH_INTERVAL_MAXIMUM: int = 24 * 60 * 60  # 1 day
FX_REFRESH_INTERVAL_SETTING_KEY: str = "FX_REFRESH_INTERVAL_SECONDS"

_FX_REFRESH_INTERVAL_OVERRIDE: int | None = None


def _coerce_fx_refresh_interval(value: object) -> int | None:
    """Validate an FX-refresh-interval candidate.

    Returns the coerced integer on success or ``None`` on rejection.
    Rejects ``bool``, non-finite, non-integer-valued, or out-of-range
    values.
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
        coerced < FX_REFRESH_INTERVAL_MINIMUM
        or coerced > FX_REFRESH_INTERVAL_MAXIMUM
    ):
        return None
    return coerced


def set_fx_refresh_interval_override(value: int) -> None:
    """Replace the in-process FX-refresh-interval override."""
    global _FX_REFRESH_INTERVAL_OVERRIDE
    if isinstance(value, bool):
        raise ValueError(
            "fx refresh interval override must be int, not bool"
        )
    coerced = _coerce_fx_refresh_interval(value)
    if coerced is None:
        raise ValueError(
            f"fx refresh interval override {value!r} must be an "
            f"integer in [{FX_REFRESH_INTERVAL_MINIMUM}, "
            f"{FX_REFRESH_INTERVAL_MAXIMUM}]"
        )
    _FX_REFRESH_INTERVAL_OVERRIDE = coerced


def clear_fx_refresh_interval_override() -> bool:
    """Drop the in-process override.  Returns True if one was active."""
    global _FX_REFRESH_INTERVAL_OVERRIDE
    had = _FX_REFRESH_INTERVAL_OVERRIDE is not None
    _FX_REFRESH_INTERVAL_OVERRIDE = None
    return had


def get_fx_refresh_interval_override() -> int | None:
    """Return the current in-process override (or ``None``)."""
    return _FX_REFRESH_INTERVAL_OVERRIDE


async def refresh_fx_refresh_interval_override_from_db(
    db: "Database | None",
) -> int | None:
    """Reload the override from ``system_settings``.

    Transient DB errors keep the previous cache; malformed values
    are treated as "no override".
    """
    global _FX_REFRESH_INTERVAL_OVERRIDE
    if db is None:
        return _FX_REFRESH_INTERVAL_OVERRIDE
    try:
        raw = await db.get_setting(FX_REFRESH_INTERVAL_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_fx_refresh_interval_override_from_db: "
            "get_setting failed; keeping previous cache value=%s",
            _FX_REFRESH_INTERVAL_OVERRIDE,
        )
        return _FX_REFRESH_INTERVAL_OVERRIDE
    if raw is None:
        _FX_REFRESH_INTERVAL_OVERRIDE = None
        return None
    coerced = _coerce_fx_refresh_interval(raw)
    if coerced is None:
        log.warning(
            "refresh_fx_refresh_interval_override_from_db: "
            "rejected stored value %r; clearing override",
            raw,
        )
        _FX_REFRESH_INTERVAL_OVERRIDE = None
        return None
    _FX_REFRESH_INTERVAL_OVERRIDE = coerced
    return coerced


def get_fx_refresh_interval_seconds() -> int:
    """Return the resolved FX-refresh interval in seconds.

    Resolution order: in-process override → env → default (600).
    """
    if _FX_REFRESH_INTERVAL_OVERRIDE is not None:
        return _FX_REFRESH_INTERVAL_OVERRIDE
    raw = os.getenv("FX_REFRESH_INTERVAL_SECONDS")
    if raw is not None:
        coerced = _coerce_fx_refresh_interval(raw)
        if coerced is not None:
            return coerced
    return DEFAULT_FX_REFRESH_INTERVAL_SECONDS


def get_fx_refresh_interval_source() -> str:
    """Return ``db`` / ``env`` / ``default``."""
    if _FX_REFRESH_INTERVAL_OVERRIDE is not None:
        return "db"
    raw = os.getenv("FX_REFRESH_INTERVAL_SECONDS")
    if raw is not None and _coerce_fx_refresh_interval(raw) is not None:
        return "env"
    return "default"


def format_interval_human(seconds: int) -> str:
    """Format seconds as a human-readable duration string.

    Mirrors :func:`model_discovery_config.format_interval_human` so
    the wallet-config and models-config breakdown tables render
    consistently.
    """
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{m}m" if s == 0 else f"{m}m {s}s"
    h, remainder = divmod(seconds, 3600)
    m = remainder // 60
    return f"{h}h" if m == 0 else f"{h}h {m}m"
