"""free_trial: env-var config + DB-backed override layer for the
``FREE_MESSAGES_PER_USER`` knob.

Stage-15-Step-E #10b row 6. Mirrors the
``COST_MARKUP`` / ``MIN_TOPUP_USD`` / ``REFERRAL_BONUS_*`` recipe:

* ``_FREE_MESSAGES_PER_USER_OVERRIDE`` — process-local cache,
  populated from ``system_settings.FREE_MESSAGES_PER_USER`` via
  :func:`refresh_free_messages_per_user_override_from_db` at boot
  (in ``main.py``) and on every ``/admin/wallet-config`` render.
  The web admin form writes this row so an operator can retune
  the trial allowance without a redeploy.
* ``FREE_MESSAGES_PER_USER`` env var — bumped in ``.env.example``
  for staging deploys.
* ``DEFAULT_FREE_MESSAGES_PER_USER = 10`` — compile-time fallback,
  matches the historical SQL ``DEFAULT 10`` on
  ``users.free_messages_left`` from ``0001_baseline.py``.

The resolved value is consumed by :meth:`Database.create_user`
(it explicitly sets ``free_messages_left`` to the resolved
allowance instead of relying on the schema default — that's what
makes a saved override apply to *new* registrants without a
schema change).

Knob characteristics:

* Integer (you can't grant 1.5 messages); validated by
  :func:`_coerce_free_messages_per_user`.
* ``[0, 10_000]`` allowed range. ``0`` is a deliberate "no trial,
  pay-to-play only" mode (operators sometimes want this for a
  closed beta). ``10_000`` is the absolute upper bound — anything
  bigger is a fat-finger.
* ``bool`` rejected explicitly so a stored ``True`` cannot sneak
  through ``int(value)`` as ``1``.

Affects only NEW users from the moment the override lands;
existing rows keep whatever ``free_messages_left`` they had at
``/start`` time. There's no retroactive top-up — operators that
want to grant more trial messages to an existing user use the
``/admin/users/<id>`` adjust form (which is a separate audit path).
"""

from __future__ import annotations

import logging
import math
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg  # noqa: F401

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Knob bounds & key
# ----------------------------------------------------------------------

DEFAULT_FREE_MESSAGES_PER_USER: int = 10
FREE_MESSAGES_PER_USER_MINIMUM: int = 0
# Inclusive upper bound — operators that want "unlimited trial" should
# instead change the pricing model (give users a starter balance via
# admin adjust). 10_000 is the highest reasonable trial size; anything
# higher would let a single stored override quietly turn the bot into
# free-forever for every new registrant.
FREE_MESSAGES_PER_USER_MAXIMUM: int = 10_000
FREE_MESSAGES_PER_USER_SETTING_KEY: str = "FREE_MESSAGES_PER_USER"

_FREE_MESSAGES_PER_USER_OVERRIDE: int | None = None


# ----------------------------------------------------------------------
# Coercion / validation
# ----------------------------------------------------------------------


def _coerce_free_messages_per_user(value: object) -> int | None:
    """Parse a free-messages-per-user candidate from raw input.

    Returns ``None`` if the value is non-numeric, a ``bool`` (a
    subclass of ``int`` — we refuse it explicitly so a ``True``
    sneaking through doesn't render as ``1``), a non-finite float,
    a non-integer-valued float (``2.7`` is not a meaningful trial
    size), or outside the
    [``FREE_MESSAGES_PER_USER_MINIMUM``,
    ``FREE_MESSAGES_PER_USER_MAXIMUM``] inclusive range.

    Strings that parse as a clean integer (e.g. ``"15"`` from the
    ``system_settings`` overlay) are accepted. ``"15.0"`` is
    accepted because ``float("15.0").is_integer()``; ``"15.5"`` is
    rejected.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        coerced = value
    else:
        # Try float-then-int because the override path stores everything
        # as TEXT in ``system_settings`` and we want to accept ``"15"``
        # *and* the legacy ``"15.0"`` writers.
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
        coerced < FREE_MESSAGES_PER_USER_MINIMUM
        or coerced > FREE_MESSAGES_PER_USER_MAXIMUM
    ):
        return None
    return coerced


# ----------------------------------------------------------------------
# Override get / set / clear
# ----------------------------------------------------------------------


def set_free_messages_per_user_override(value: int) -> None:
    """Replace the in-process free-messages-per-user override.

    Validates against the same rules as
    :func:`_coerce_free_messages_per_user`. Refuses ``bool``."""
    global _FREE_MESSAGES_PER_USER_OVERRIDE
    if isinstance(value, bool):
        raise ValueError(
            "free-messages-per-user override must be int, not bool"
        )
    coerced = _coerce_free_messages_per_user(value)
    if coerced is None:
        raise ValueError(
            f"free-messages-per-user override {value!r} must be an "
            f"integer in [{FREE_MESSAGES_PER_USER_MINIMUM}, "
            f"{FREE_MESSAGES_PER_USER_MAXIMUM}]"
        )
    _FREE_MESSAGES_PER_USER_OVERRIDE = coerced


def clear_free_messages_per_user_override() -> bool:
    """Drop the in-process override. Returns True if one was active."""
    global _FREE_MESSAGES_PER_USER_OVERRIDE
    had = _FREE_MESSAGES_PER_USER_OVERRIDE is not None
    _FREE_MESSAGES_PER_USER_OVERRIDE = None
    return had


def get_free_messages_per_user_override() -> int | None:
    """Return the current in-process override (or ``None``)."""
    return _FREE_MESSAGES_PER_USER_OVERRIDE


async def refresh_free_messages_per_user_override_from_db(db) -> int | None:
    """Reload the override from the ``system_settings`` overlay.

    Mirrors :func:`payments.refresh_min_topup_override_from_db`: a
    transient DB error keeps the previous cache in place so a pool
    blip can't accidentally revert to env / default mid-incident.
    A malformed stored value (non-int / out-of-range / NUL-ridden)
    is treated as "no override" rather than crashing the bot.
    """
    global _FREE_MESSAGES_PER_USER_OVERRIDE
    if db is None:
        return _FREE_MESSAGES_PER_USER_OVERRIDE
    try:
        raw = await db.get_setting(FREE_MESSAGES_PER_USER_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_free_messages_per_user_override_from_db: "
            "get_setting failed; keeping previous cache value=%s",
            _FREE_MESSAGES_PER_USER_OVERRIDE,
        )
        return _FREE_MESSAGES_PER_USER_OVERRIDE
    if raw is None:
        _FREE_MESSAGES_PER_USER_OVERRIDE = None
        return None
    coerced = _coerce_free_messages_per_user(raw)
    if coerced is None:
        log.warning(
            "refresh_free_messages_per_user_override_from_db: "
            "rejected stored value %r; clearing override",
            raw,
        )
        _FREE_MESSAGES_PER_USER_OVERRIDE = None
        return None
    _FREE_MESSAGES_PER_USER_OVERRIDE = coerced
    return coerced


# ----------------------------------------------------------------------
# Public lookup
# ----------------------------------------------------------------------


def get_free_messages_per_user() -> int:
    """Return the resolved trial-message allowance.

    Resolution order: in-process override → ``FREE_MESSAGES_PER_USER``
    env → ``DEFAULT_FREE_MESSAGES_PER_USER`` (10). Always returns a
    valid integer in
    ``[FREE_MESSAGES_PER_USER_MINIMUM, FREE_MESSAGES_PER_USER_MAXIMUM]``.
    """
    if _FREE_MESSAGES_PER_USER_OVERRIDE is not None:
        return _FREE_MESSAGES_PER_USER_OVERRIDE
    raw = os.getenv("FREE_MESSAGES_PER_USER")
    if raw is not None:
        coerced = _coerce_free_messages_per_user(raw)
        if coerced is not None:
            return coerced
    return DEFAULT_FREE_MESSAGES_PER_USER


def get_free_messages_per_user_source() -> str:
    """Return ``db`` / ``env`` / ``default`` for the resolved value.

    Used by the ``/admin/wallet-config`` panel to render the same
    "effective / db / env / default" badge the
    ``COST_MARKUP`` / ``MIN_TOPUP_USD`` / ``REFERRAL_BONUS_*``
    editors use.
    """
    if _FREE_MESSAGES_PER_USER_OVERRIDE is not None:
        return "db"
    raw = os.getenv("FREE_MESSAGES_PER_USER")
    if (
        raw is not None
        and _coerce_free_messages_per_user(raw) is not None
    ):
        return "env"
    return "default"
