"""i18n_lock: env-var config + DB-backed override layer that gates
live admin edits to the ``bot_strings`` override table.

Stage-15-Step-E #10b row 22. During a rolling deploy or i18n-PO
roundtrip the operator wants a "do not touch" sign on the runtime
string editor: locking blocks the upsert / revert handlers on
``/admin/strings/{lang}/{key}`` and the per-string editor's POST
endpoints. Existing overrides keep serving (the runtime ``t()``
helper is *unaffected*), but no new edits land while the lock is
on. Translators picking up partial work at the wrong moment can
no longer race a deploy.

Resolution order: in-process override (DB) → env (``I18N_LOCK``)
→ default ``False`` (unlocked). The DB override is set/cleared
from ``/admin/strings`` via the toggle form (``ROLE_SUPER``-gated),
and a boot warm-up in ``main.py`` loads the DB value into the
in-process cache so a sibling worker's flip lands on this process
without a restart.

The semantics mirror the other DB-backed overrides shipped under
Stage-15-Step-E #10b (rows 4 / 6 / 8 / 20 / 21 / 23 / 24 / 25):
the in-process slot is a tri-state — ``None`` (no override, fall
through to env / default), ``True`` (locked), ``False`` (explicitly
unlocked, beats the env default-false but lets you record an
explicit "we DECIDED to leave it on" gesture in the audit log).
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from database import Database

log = logging.getLogger("bot.i18n_lock")


# ------------------------------------------------------------------
# I18N_LOCK
# ------------------------------------------------------------------

I18N_LOCK_SETTING_KEY: str = "I18N_LOCK"

# Tri-state. ``None`` means "no override, fall through". ``True`` /
# ``False`` are explicit operator decisions that win over the env
# default. Tests reset this to ``None`` between cases; production
# code never touches it directly, only via the helpers below.
_I18N_LOCK_OVERRIDE: bool | None = None


# Recognised string forms accepted by the env var and the DB-backed
# override. Mirrors the convention used elsewhere in the codebase
# (e.g. ``ADMIN_COOKIE_SECURE`` parses ``"1"`` / ``"0"``).
_TRUTHY_TOKENS = frozenset({"1", "true", "t", "yes", "y", "on", "lock", "locked"})
_FALSY_TOKENS = frozenset({"0", "false", "f", "no", "n", "off", "unlock", "unlocked"})


def _coerce_i18n_lock(value: object) -> bool | None:
    """Parse a lock candidate from raw input.

    Returns ``True`` / ``False`` for any recognised token (case-
    insensitive, whitespace-stripped), or ``None`` if the value is
    unrecognised or empty. ``bool`` itself round-trips. Numeric
    inputs are coerced via the truthy-token table after stringifying
    so ``1`` and ``0`` work alongside their string forms.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # NaN / inf collapse to None; ints fall through to string
        # tokens so "1" / "0" / 0 / 1 all resolve the same way.
        try:
            stringified = str(int(value))
        except (TypeError, ValueError, OverflowError):
            return None
        return _coerce_i18n_lock(stringified)
    if not isinstance(value, str):
        return None
    token = value.strip().lower()
    if not token:
        return None
    if token in _TRUTHY_TOKENS:
        return True
    if token in _FALSY_TOKENS:
        return False
    return None


def set_i18n_lock_override(value: bool) -> None:
    """Replace the in-process lock override.

    Only ``bool`` is accepted — numeric / string inputs that *look*
    boolean still need to be coerced by the caller (the web handler
    parses the form value). This keeps the boundary between "data
    coming from the operator" and "settled boolean state" clean.
    """
    global _I18N_LOCK_OVERRIDE
    if not isinstance(value, bool):
        raise ValueError(
            f"i18n lock override must be bool, got {type(value).__name__}"
        )
    _I18N_LOCK_OVERRIDE = value


def clear_i18n_lock_override() -> bool:
    """Drop the in-process override.  Returns True if one was active."""
    global _I18N_LOCK_OVERRIDE
    had = _I18N_LOCK_OVERRIDE is not None
    _I18N_LOCK_OVERRIDE = None
    return had


def get_i18n_lock_override() -> bool | None:
    """Return the current in-process override (or ``None``)."""
    return _I18N_LOCK_OVERRIDE


async def refresh_i18n_lock_override_from_db(
    db: "Database | None",
) -> bool | None:
    """Reload the override from ``system_settings``.

    Transient DB errors keep the previous cache (so a flaky network
    blip doesn't unexpectedly unlock the editor); a missing row
    clears the override; an unrecognised stored value is logged at
    WARNING and treated as "no override". Returns the resolved
    cache value after the refresh.
    """
    global _I18N_LOCK_OVERRIDE
    if db is None:
        return _I18N_LOCK_OVERRIDE
    try:
        raw = await db.get_setting(I18N_LOCK_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_i18n_lock_override_from_db: get_setting failed; "
            "keeping previous cache value=%s",
            _I18N_LOCK_OVERRIDE,
        )
        return _I18N_LOCK_OVERRIDE
    if raw is None:
        _I18N_LOCK_OVERRIDE = None
        return None
    coerced = _coerce_i18n_lock(raw)
    if coerced is None:
        log.warning(
            "refresh_i18n_lock_override_from_db: rejected stored "
            "value %r; clearing override",
            raw,
        )
        _I18N_LOCK_OVERRIDE = None
        return None
    _I18N_LOCK_OVERRIDE = coerced
    return coerced


def is_i18n_locked(env_value: str | None = None) -> bool:
    """Return whether the i18n string editor is currently locked.

    Resolution order: in-process override → env → default ``False``.

    *env_value* lets callers thread the live env reading explicitly;
    the default ``None`` reads ``I18N_LOCK`` from the process env at
    call time (handy for tests that ``monkeypatch.setenv`` it).
    """
    if _I18N_LOCK_OVERRIDE is not None:
        return _I18N_LOCK_OVERRIDE
    if env_value is None:
        env_value = os.getenv("I18N_LOCK", "")
    coerced = _coerce_i18n_lock(env_value)
    if coerced is not None:
        return coerced
    return False


def get_i18n_lock_source(env_value: str | None = None) -> str:
    """Return the resolution source for the current lock state.

    One of ``"db"`` (in-process override is set), ``"env"`` (env var
    parses to a recognised value with no override active), or
    ``"default"`` (neither a DB row nor a usable env value).
    """
    if _I18N_LOCK_OVERRIDE is not None:
        return "db"
    if env_value is None:
        env_value = os.getenv("I18N_LOCK", "")
    if _coerce_i18n_lock(env_value) is not None:
        return "env"
    return "default"


def serialise_lock_for_db(value: bool) -> str:
    """Return the canonical string form persisted to ``system_settings``.

    We pick ``"1"`` / ``"0"`` so the DB row reads cleanly when
    inspected via ``SELECT value FROM system_settings`` without
    having to remember whether we wrote ``"true"`` or ``"yes"``.
    The reverse parser accepts every recognised token, so an
    operator who sets it manually with a different spelling won't
    blow up — the resolver still recognises ``"true"`` / ``"on"`` /
    etc.
    """
    if not isinstance(value, bool):
        raise ValueError(
            f"i18n lock serialisation expects bool, got {type(value).__name__}"
        )
    return "1" if value else "0"
