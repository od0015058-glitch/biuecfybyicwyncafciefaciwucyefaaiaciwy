"""audit_retention: env-var config + DB-backed override layer for
``AUDIT_RETENTION_DAYS`` and a background reaper loop that prunes
``admin_audit_log`` rows older than the configured retention window.

Stage-15-Step-E #10b row 20. The audit table grows unboundedly —
a busy bot with frequent logins, promo creation, and config changes
can accumulate millions of rows over a year. The reaper wakes once
per day (configurable via ``AUDIT_RETENTION_INTERVAL_HOURS``) and
batch-deletes rows whose ``ts`` is older than ``NOW() - INTERVAL
'<retention_days> days'``.

Resolution order: in-process override → env → default (90 days).
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from database import Database

log = logging.getLogger("bot.audit_retention")


# ------------------------------------------------------------------
# AUDIT_RETENTION_DAYS
# ------------------------------------------------------------------

DEFAULT_AUDIT_RETENTION_DAYS: int = 90
AUDIT_RETENTION_DAYS_MINIMUM: int = 7
AUDIT_RETENTION_DAYS_MAXIMUM: int = 3650  # ~10 years
AUDIT_RETENTION_DAYS_SETTING_KEY: str = "AUDIT_RETENTION_DAYS"

_AUDIT_RETENTION_DAYS_OVERRIDE: int | None = None


def _coerce_audit_retention_days(value: object) -> int | None:
    """Parse a retention-days candidate from raw input.

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
        coerced < AUDIT_RETENTION_DAYS_MINIMUM
        or coerced > AUDIT_RETENTION_DAYS_MAXIMUM
    ):
        return None
    return coerced


def set_audit_retention_days_override(value: int) -> None:
    """Replace the in-process retention-days override."""
    global _AUDIT_RETENTION_DAYS_OVERRIDE
    if isinstance(value, bool):
        raise ValueError(
            "audit retention days override must be int, not bool"
        )
    coerced = _coerce_audit_retention_days(value)
    if coerced is None:
        raise ValueError(
            f"audit retention days override {value!r} must be an "
            f"integer in [{AUDIT_RETENTION_DAYS_MINIMUM}, "
            f"{AUDIT_RETENTION_DAYS_MAXIMUM}]"
        )
    _AUDIT_RETENTION_DAYS_OVERRIDE = coerced


def clear_audit_retention_days_override() -> bool:
    """Drop the in-process override.  Returns True if one was active."""
    global _AUDIT_RETENTION_DAYS_OVERRIDE
    had = _AUDIT_RETENTION_DAYS_OVERRIDE is not None
    _AUDIT_RETENTION_DAYS_OVERRIDE = None
    return had


def get_audit_retention_days_override() -> int | None:
    """Return the current in-process override (or ``None``)."""
    return _AUDIT_RETENTION_DAYS_OVERRIDE


async def refresh_audit_retention_days_override_from_db(
    db: "Database | None",
) -> int | None:
    """Reload the override from ``system_settings``.

    Transient DB errors keep the previous cache; malformed values
    are treated as "no override".
    """
    global _AUDIT_RETENTION_DAYS_OVERRIDE
    if db is None:
        return _AUDIT_RETENTION_DAYS_OVERRIDE
    try:
        raw = await db.get_setting(AUDIT_RETENTION_DAYS_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_audit_retention_days_override_from_db: "
            "get_setting failed; keeping previous cache value=%s",
            _AUDIT_RETENTION_DAYS_OVERRIDE,
        )
        return _AUDIT_RETENTION_DAYS_OVERRIDE
    if raw is None:
        _AUDIT_RETENTION_DAYS_OVERRIDE = None
        return None
    coerced = _coerce_audit_retention_days(raw)
    if coerced is None:
        log.warning(
            "refresh_audit_retention_days_override_from_db: "
            "rejected stored value %r; clearing override",
            raw,
        )
        _AUDIT_RETENTION_DAYS_OVERRIDE = None
        return None
    _AUDIT_RETENTION_DAYS_OVERRIDE = coerced
    return coerced


def get_audit_retention_days() -> int:
    """Return the resolved retention window in days.

    Resolution order: in-process override → env → default (90).
    """
    if _AUDIT_RETENTION_DAYS_OVERRIDE is not None:
        return _AUDIT_RETENTION_DAYS_OVERRIDE
    raw = os.getenv("AUDIT_RETENTION_DAYS")
    if raw is not None:
        coerced = _coerce_audit_retention_days(raw)
        if coerced is not None:
            return coerced
    return DEFAULT_AUDIT_RETENTION_DAYS


def get_audit_retention_days_source() -> str:
    """Return ``db`` / ``env`` / ``default``."""
    if _AUDIT_RETENTION_DAYS_OVERRIDE is not None:
        return "db"
    raw = os.getenv("AUDIT_RETENTION_DAYS")
    if raw is not None and _coerce_audit_retention_days(raw) is not None:
        return "env"
    return "default"


# ------------------------------------------------------------------
# Reaper interval (not DB-backed — env-only, rarely changed)
# ------------------------------------------------------------------

DEFAULT_RETENTION_INTERVAL_HOURS: int = 24
RETENTION_INTERVAL_HOURS_MINIMUM: int = 1


def _get_retention_interval_hours() -> int:
    """Return the reaper interval (hours). Env-only, default 24."""
    raw = os.getenv("AUDIT_RETENTION_INTERVAL_HOURS")
    if raw is not None:
        try:
            val = int(raw)
            if val >= RETENTION_INTERVAL_HOURS_MINIMUM:
                return val
        except (TypeError, ValueError):
            pass
    return DEFAULT_RETENTION_INTERVAL_HOURS


# ------------------------------------------------------------------
# Batch delete helper
# ------------------------------------------------------------------

DEFAULT_RETENTION_BATCH: int = 5000


def _get_retention_batch() -> int:
    """Max rows deleted per tick.  Env-only, default 5000."""
    raw = os.getenv("AUDIT_RETENTION_BATCH")
    if raw is not None:
        try:
            val = int(raw)
            if val >= 1:
                return val
        except (TypeError, ValueError):
            pass
    return DEFAULT_RETENTION_BATCH


# ------------------------------------------------------------------
# Background reaper loop
# ------------------------------------------------------------------

# Per-process counters for ops visibility.
_REAPER_TICKS: int = 0
_REAPER_TOTAL_DELETED: int = 0
_REAPER_LAST_RUN_EPOCH: float = 0.0


def get_reaper_counters() -> dict:
    """Return per-process reaper stats."""
    return {
        "ticks": _REAPER_TICKS,
        "total_deleted": _REAPER_TOTAL_DELETED,
        "last_run_epoch": _REAPER_LAST_RUN_EPOCH,
    }


async def _delete_old_audit_rows(
    db: "Database",
    retention_days: int,
    batch: int,
) -> int:
    """Delete audit rows older than ``retention_days`` in a single
    batch-capped DELETE.

    Returns the number of rows deleted.
    """
    query = """
        DELETE FROM admin_audit_log
         WHERE id IN (
           SELECT id FROM admin_audit_log
            WHERE ts < NOW() - make_interval(days => $1)
            LIMIT $2
         )
    """
    async with db.pool.acquire() as connection:
        result = await connection.execute(query, retention_days, batch)
    try:
        return int(result.split()[-1])
    except (ValueError, IndexError):
        return 0


async def audit_retention_loop(db: "Database") -> None:
    """Background task: delete audit rows older than the retention window.

    Runs forever. Spawned by ``main.main``.
    """
    global _REAPER_TICKS, _REAPER_TOTAL_DELETED, _REAPER_LAST_RUN_EPOCH
    import time

    from bot_health import register_loop

    register_loop("audit_retention")

    while True:
        interval_hours = _get_retention_interval_hours()
        await asyncio.sleep(interval_hours * 3600)

        try:
            await refresh_audit_retention_days_override_from_db(db)
        except Exception:
            log.exception(
                "audit_retention_loop: refresh_override failed"
            )

        retention_days = get_audit_retention_days()
        batch = _get_retention_batch()
        total_this_tick = 0

        try:
            while True:
                deleted = await _delete_old_audit_rows(
                    db, retention_days, batch,
                )
                total_this_tick += deleted
                if deleted < batch:
                    break
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(
                "audit_retention_loop: delete failed "
                "after deleting %d rows this tick",
                total_this_tick,
            )

        _REAPER_TICKS += 1
        _REAPER_TOTAL_DELETED += total_this_tick
        _REAPER_LAST_RUN_EPOCH = time.time()

        if total_this_tick > 0:
            log.info(
                "audit_retention_loop: pruned %d rows older than %d days",
                total_this_tick, retention_days,
            )
