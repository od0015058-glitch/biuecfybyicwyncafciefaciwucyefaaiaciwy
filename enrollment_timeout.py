"""DB-backed override layer for ``ADMIN_2FA_ENROLLMENT_TIMEOUT``.

Stage-15-Step-E #10b row 26. The TOTP enrolment helper page at
``/admin/enroll_2fa`` currently has no server-side time limit — an
operator who opens the page and walks away leaves the QR / secret
visible indefinitely on an unlocked browser. This module introduces
a configurable timeout (in seconds) after which the enrolment page
auto-expires the suggested secret. The value controls how long the
QR code and secret remain valid on the page; the template renders a
countdown and auto-reloads when the window closes.

Resolution order: in-process override → env → default (300 s / 5 min).

Same shape as :mod:`fx_refresh_config` (Stage-15-Step-E #10b row 24):
module-level cache, coercion validator, set/clear/get,
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

log = logging.getLogger("bot.enrollment_timeout")


# ------------------------------------------------------------------
# ADMIN_2FA_ENROLLMENT_TIMEOUT
# ------------------------------------------------------------------

DEFAULT_ENROLLMENT_TIMEOUT_SECONDS: int = 5 * 60  # 5 minutes
ENROLLMENT_TIMEOUT_MINIMUM: int = 30  # 30-second floor
ENROLLMENT_TIMEOUT_MAXIMUM: int = 3600  # 1-hour ceiling
ENROLLMENT_TIMEOUT_SETTING_KEY: str = "ADMIN_2FA_ENROLLMENT_TIMEOUT"

_ENROLLMENT_TIMEOUT_OVERRIDE: int | None = None


def _coerce_enrollment_timeout(value: object) -> int | None:
    """Validate an enrollment-timeout candidate.

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
        coerced < ENROLLMENT_TIMEOUT_MINIMUM
        or coerced > ENROLLMENT_TIMEOUT_MAXIMUM
    ):
        return None
    return coerced


def set_enrollment_timeout_override(value: int) -> None:
    """Replace the in-process enrollment-timeout override."""
    global _ENROLLMENT_TIMEOUT_OVERRIDE
    if isinstance(value, bool):
        raise ValueError(
            "enrollment timeout override must be int, not bool"
        )
    coerced = _coerce_enrollment_timeout(value)
    if coerced is None:
        raise ValueError(
            f"enrollment timeout override {value!r} must be an "
            f"integer in [{ENROLLMENT_TIMEOUT_MINIMUM}, "
            f"{ENROLLMENT_TIMEOUT_MAXIMUM}]"
        )
    _ENROLLMENT_TIMEOUT_OVERRIDE = coerced


def clear_enrollment_timeout_override() -> bool:
    """Drop the in-process override.  Returns True if one was active."""
    global _ENROLLMENT_TIMEOUT_OVERRIDE
    had = _ENROLLMENT_TIMEOUT_OVERRIDE is not None
    _ENROLLMENT_TIMEOUT_OVERRIDE = None
    return had


def get_enrollment_timeout_override() -> int | None:
    """Return the current in-process override (or ``None``)."""
    return _ENROLLMENT_TIMEOUT_OVERRIDE


async def refresh_enrollment_timeout_override_from_db(
    db: "Database | None",
) -> int | None:
    """Reload the override from ``system_settings``.

    Transient DB errors keep the previous cache; malformed values
    are treated as "no override".
    """
    global _ENROLLMENT_TIMEOUT_OVERRIDE
    if db is None:
        return _ENROLLMENT_TIMEOUT_OVERRIDE
    try:
        raw = await db.get_setting(ENROLLMENT_TIMEOUT_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_enrollment_timeout_override_from_db: "
            "get_setting failed; keeping previous cache value=%s",
            _ENROLLMENT_TIMEOUT_OVERRIDE,
        )
        return _ENROLLMENT_TIMEOUT_OVERRIDE
    if raw is None:
        _ENROLLMENT_TIMEOUT_OVERRIDE = None
        return None
    coerced = _coerce_enrollment_timeout(raw)
    if coerced is None:
        log.warning(
            "refresh_enrollment_timeout_override_from_db: "
            "rejected stored value %r; clearing override",
            raw,
        )
        _ENROLLMENT_TIMEOUT_OVERRIDE = None
        return None
    _ENROLLMENT_TIMEOUT_OVERRIDE = coerced
    return coerced


def get_enrollment_timeout_seconds() -> int:
    """Return the resolved enrollment timeout in seconds.

    Resolution order: in-process override → env → default (300).
    """
    if _ENROLLMENT_TIMEOUT_OVERRIDE is not None:
        return _ENROLLMENT_TIMEOUT_OVERRIDE
    raw = os.getenv("ADMIN_2FA_ENROLLMENT_TIMEOUT")
    if raw is not None:
        coerced = _coerce_enrollment_timeout(raw)
        if coerced is not None:
            return coerced
    return DEFAULT_ENROLLMENT_TIMEOUT_SECONDS


def get_enrollment_timeout_source() -> str:
    """Return ``db`` / ``env`` / ``default``."""
    if _ENROLLMENT_TIMEOUT_OVERRIDE is not None:
        return "db"
    raw = os.getenv("ADMIN_2FA_ENROLLMENT_TIMEOUT")
    if raw is not None and _coerce_enrollment_timeout(raw) is not None:
        return "env"
    return "default"


def format_timeout_human(seconds: int) -> str:
    """Format seconds as a human-readable duration string.

    Mirrors :func:`fx_refresh_config.format_interval_human` for
    consistent rendering across admin editor cards.
    """
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{m}m" if s == 0 else f"{m}m {s}s"
    h, remainder = divmod(seconds, 3600)
    m = remainder // 60
    return f"{h}h" if m == 0 else f"{h}h {m}m"
