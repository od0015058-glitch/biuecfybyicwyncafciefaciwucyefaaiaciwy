"""memory_config: env-var config + DB-backed override layer for the
conversation memory caps ``MEMORY_CONTEXT_LIMIT`` and
``MEMORY_CONTENT_MAX_CHARS``.

Stage-15-Step-E #10b row 8.  Mirrors the
``FREE_MESSAGES_PER_USER`` / ``COST_MARKUP`` / ``MIN_TOPUP_USD``
recipe:

* ``_MEMORY_CONTEXT_LIMIT_OVERRIDE`` / ``_MEMORY_CONTENT_MAX_CHARS_OVERRIDE``
  — process-local caches, populated from ``system_settings`` via
  the corresponding ``refresh_*_from_db`` helpers at boot (in
  ``main.py``) and on every ``/admin/memory-config`` render.
  The web admin form writes these rows so an operator can retune
  the caps without a redeploy.

* ``MEMORY_CONTEXT_LIMIT`` / ``MEMORY_CONTENT_MAX_CHARS`` env vars
  — documented in ``.env.example`` for staging deploys.

* Compile-time defaults match the historical hardcoded class
  attributes in ``Database`` (30 messages, 8 000 chars).

Resolution order (both knobs): in-process override → env → default.
"""

from __future__ import annotations

import logging
import math
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass  # no asyncpg import needed at type-check time

log = logging.getLogger(__name__)


# ------------------------------------------------------------------
# MEMORY_CONTEXT_LIMIT
# ------------------------------------------------------------------

DEFAULT_MEMORY_CONTEXT_LIMIT: int = 30
MEMORY_CONTEXT_LIMIT_MINIMUM: int = 1
MEMORY_CONTEXT_LIMIT_MAXIMUM: int = 500
MEMORY_CONTEXT_LIMIT_SETTING_KEY: str = "MEMORY_CONTEXT_LIMIT"

_MEMORY_CONTEXT_LIMIT_OVERRIDE: int | None = None


def _coerce_memory_context_limit(value: object) -> int | None:
    """Parse a context-limit candidate from raw input.

    Returns ``None`` for non-numeric, ``bool``, non-finite,
    non-integer-valued, or out-of-range values.
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
        coerced < MEMORY_CONTEXT_LIMIT_MINIMUM
        or coerced > MEMORY_CONTEXT_LIMIT_MAXIMUM
    ):
        return None
    return coerced


def set_memory_context_limit_override(value: int) -> None:
    """Replace the in-process context-limit override."""
    global _MEMORY_CONTEXT_LIMIT_OVERRIDE
    if isinstance(value, bool):
        raise ValueError(
            "memory context limit override must be int, not bool"
        )
    coerced = _coerce_memory_context_limit(value)
    if coerced is None:
        raise ValueError(
            f"memory context limit override {value!r} must be an "
            f"integer in [{MEMORY_CONTEXT_LIMIT_MINIMUM}, "
            f"{MEMORY_CONTEXT_LIMIT_MAXIMUM}]"
        )
    _MEMORY_CONTEXT_LIMIT_OVERRIDE = coerced


def clear_memory_context_limit_override() -> bool:
    """Drop the in-process override.  Returns True if one was active."""
    global _MEMORY_CONTEXT_LIMIT_OVERRIDE
    had = _MEMORY_CONTEXT_LIMIT_OVERRIDE is not None
    _MEMORY_CONTEXT_LIMIT_OVERRIDE = None
    return had


def get_memory_context_limit_override() -> int | None:
    """Return the current in-process override (or ``None``)."""
    return _MEMORY_CONTEXT_LIMIT_OVERRIDE


async def refresh_memory_context_limit_override_from_db(db) -> int | None:
    """Reload the override from ``system_settings``.

    Transient DB errors keep the previous cache; malformed values
    are treated as "no override".
    """
    global _MEMORY_CONTEXT_LIMIT_OVERRIDE
    if db is None:
        return _MEMORY_CONTEXT_LIMIT_OVERRIDE
    try:
        raw = await db.get_setting(MEMORY_CONTEXT_LIMIT_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_memory_context_limit_override_from_db: "
            "get_setting failed; keeping previous cache value=%s",
            _MEMORY_CONTEXT_LIMIT_OVERRIDE,
        )
        return _MEMORY_CONTEXT_LIMIT_OVERRIDE
    if raw is None:
        _MEMORY_CONTEXT_LIMIT_OVERRIDE = None
        return None
    coerced = _coerce_memory_context_limit(raw)
    if coerced is None:
        log.warning(
            "refresh_memory_context_limit_override_from_db: "
            "rejected stored value %r; clearing override",
            raw,
        )
        _MEMORY_CONTEXT_LIMIT_OVERRIDE = None
        return None
    _MEMORY_CONTEXT_LIMIT_OVERRIDE = coerced
    return coerced


def get_memory_context_limit() -> int:
    """Return the resolved context-message cap.

    Resolution order: in-process override → env → default (30).
    """
    if _MEMORY_CONTEXT_LIMIT_OVERRIDE is not None:
        return _MEMORY_CONTEXT_LIMIT_OVERRIDE
    raw = os.getenv("MEMORY_CONTEXT_LIMIT")
    if raw is not None:
        coerced = _coerce_memory_context_limit(raw)
        if coerced is not None:
            return coerced
    return DEFAULT_MEMORY_CONTEXT_LIMIT


def get_memory_context_limit_source() -> str:
    """Return ``db`` / ``env`` / ``default``."""
    if _MEMORY_CONTEXT_LIMIT_OVERRIDE is not None:
        return "db"
    raw = os.getenv("MEMORY_CONTEXT_LIMIT")
    if raw is not None and _coerce_memory_context_limit(raw) is not None:
        return "env"
    return "default"


# ------------------------------------------------------------------
# MEMORY_CONTENT_MAX_CHARS
# ------------------------------------------------------------------

DEFAULT_MEMORY_CONTENT_MAX_CHARS: int = 8000
MEMORY_CONTENT_MAX_CHARS_MINIMUM: int = 100
MEMORY_CONTENT_MAX_CHARS_MAXIMUM: int = 100_000
MEMORY_CONTENT_MAX_CHARS_SETTING_KEY: str = "MEMORY_CONTENT_MAX_CHARS"

_MEMORY_CONTENT_MAX_CHARS_OVERRIDE: int | None = None


def _coerce_memory_content_max_chars(value: object) -> int | None:
    """Parse a content-max-chars candidate from raw input.

    Returns ``None`` for non-numeric, ``bool``, non-finite,
    non-integer-valued, or out-of-range values.
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
        coerced < MEMORY_CONTENT_MAX_CHARS_MINIMUM
        or coerced > MEMORY_CONTENT_MAX_CHARS_MAXIMUM
    ):
        return None
    return coerced


def set_memory_content_max_chars_override(value: int) -> None:
    """Replace the in-process content-max-chars override."""
    global _MEMORY_CONTENT_MAX_CHARS_OVERRIDE
    if isinstance(value, bool):
        raise ValueError(
            "memory content max chars override must be int, not bool"
        )
    coerced = _coerce_memory_content_max_chars(value)
    if coerced is None:
        raise ValueError(
            f"memory content max chars override {value!r} must be an "
            f"integer in [{MEMORY_CONTENT_MAX_CHARS_MINIMUM}, "
            f"{MEMORY_CONTENT_MAX_CHARS_MAXIMUM}]"
        )
    _MEMORY_CONTENT_MAX_CHARS_OVERRIDE = coerced


def clear_memory_content_max_chars_override() -> bool:
    """Drop the in-process override.  Returns True if one was active."""
    global _MEMORY_CONTENT_MAX_CHARS_OVERRIDE
    had = _MEMORY_CONTENT_MAX_CHARS_OVERRIDE is not None
    _MEMORY_CONTENT_MAX_CHARS_OVERRIDE = None
    return had


def get_memory_content_max_chars_override() -> int | None:
    """Return the current in-process override (or ``None``)."""
    return _MEMORY_CONTENT_MAX_CHARS_OVERRIDE


async def refresh_memory_content_max_chars_override_from_db(db) -> int | None:
    """Reload the override from ``system_settings``.

    Transient DB errors keep the previous cache; malformed values
    are treated as "no override".
    """
    global _MEMORY_CONTENT_MAX_CHARS_OVERRIDE
    if db is None:
        return _MEMORY_CONTENT_MAX_CHARS_OVERRIDE
    try:
        raw = await db.get_setting(MEMORY_CONTENT_MAX_CHARS_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_memory_content_max_chars_override_from_db: "
            "get_setting failed; keeping previous cache value=%s",
            _MEMORY_CONTENT_MAX_CHARS_OVERRIDE,
        )
        return _MEMORY_CONTENT_MAX_CHARS_OVERRIDE
    if raw is None:
        _MEMORY_CONTENT_MAX_CHARS_OVERRIDE = None
        return None
    coerced = _coerce_memory_content_max_chars(raw)
    if coerced is None:
        log.warning(
            "refresh_memory_content_max_chars_override_from_db: "
            "rejected stored value %r; clearing override",
            raw,
        )
        _MEMORY_CONTENT_MAX_CHARS_OVERRIDE = None
        return None
    _MEMORY_CONTENT_MAX_CHARS_OVERRIDE = coerced
    return coerced


def get_memory_content_max_chars() -> int:
    """Return the resolved per-message content cap.

    Resolution order: in-process override → env → default (8000).
    """
    if _MEMORY_CONTENT_MAX_CHARS_OVERRIDE is not None:
        return _MEMORY_CONTENT_MAX_CHARS_OVERRIDE
    raw = os.getenv("MEMORY_CONTENT_MAX_CHARS")
    if raw is not None:
        coerced = _coerce_memory_content_max_chars(raw)
        if coerced is not None:
            return coerced
    return DEFAULT_MEMORY_CONTENT_MAX_CHARS


def get_memory_content_max_chars_source() -> str:
    """Return ``db`` / ``env`` / ``default``."""
    if _MEMORY_CONTENT_MAX_CHARS_OVERRIDE is not None:
        return "db"
    raw = os.getenv("MEMORY_CONTENT_MAX_CHARS")
    if (
        raw is not None
        and _coerce_memory_content_max_chars(raw) is not None
    ):
        return "env"
    return "default"
