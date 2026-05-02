"""Background reaper for stuck PENDING transactions.

Stage-9-Step-5: NowPayments invoices that the user abandons mid-checkout
sit in the ``transactions`` ledger as ``PENDING`` forever, polluting
``/admin/transactions`` and the dashboard "pending payments" tile.

This module spawns a single ``asyncio.Task`` at boot that wakes up
every ``PENDING_EXPIRATION_INTERVAL_MIN`` minutes (default 15),
calls :func:`Database.expire_stale_pending` to flip rows older than
``PENDING_EXPIRATION_HOURS`` (default 24) to ``EXPIRED``, fires a
courtesy Telegram notification to the user, and writes one
``payment_expired`` audit row per closed invoice.

Why a Postgres ``SELECT … FOR UPDATE SKIP LOCKED`` rather than a
Redis-locked cron: the bot is a single deploy with one webhook
listener; the lock is purely belt-and-suspenders against a future
multi-replica deploy. The reaper itself is idempotent (rerunning a
second time after a successful flush is a no-op WHERE clause), so
even without the lock you can't double-process a row.

Why no fancy retry / backoff for Telegram send failures: a user who
blocked the bot or deleted their account is exactly the user whose
abandoned invoice we're cleaning up. Logging at WARNING and moving
on is correct.
"""

from __future__ import annotations

import asyncio
import logging
import os

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

import strings
from bot_health import register_loop
from database import db


log = logging.getLogger("bot.pending_expiration")


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 9: DB-backed override for the
# PENDING_EXPIRATION_HOURS knob.
#
# The reaper loop, the manual ``/admin/control`` Tick-now button, and
# the Database.expire_stale_pending caller all read this through
# :func:`get_pending_expiration_hours` so a saved override takes
# effect on the next iteration without a redeploy.
#
# (The HANDOFF table calls this row "PENDING_EXPIRATION_HOURS_DEFAULT"
# but the actual env-var name in this module — and in
# ``database.py``'s docstring — is ``PENDING_EXPIRATION_HOURS``. We use
# the latter as the canonical name. Same naming-discrepancy
# documentation pattern as §10b.3 row #7 used for the referral knobs.)
# ---------------------------------------------------------------------

EXPIRATION_HOURS_SETTING_KEY: str = "PENDING_EXPIRATION_HOURS"
EXPIRATION_HOURS_DEFAULT: int = 24
EXPIRATION_HOURS_MINIMUM: int = 1
# 1-year cap on the override slot. The default is 24 h; an operator
# might bump to a week (168 h) or a month (~720 h) for high-value
# crypto payments that take a long time to confirm. Any value above
# 1 year almost certainly means the operator pasted the wrong digit
# and would silently disable the reaper for the rest of the deploy
# lifetime — refuse so the panel returns "no changes were made"
# instead.
EXPIRATION_HOURS_OVERRIDE_MAXIMUM: int = 24 * 365
_EXPIRATION_HOURS_OVERRIDE: int | None = None


def _coerce_expiration_hours(value: object) -> int | None:
    """Validate an expiration-hours candidate for the override slot.

    Returns the coerced integer on success or ``None`` on rejection.
    Rejects ``bool`` explicitly even though it's an ``int`` subclass
    (``True`` passing as ``1`` would silently shrink the window to
    1 hour and EXPIRE most of the legitimate-but-slow PENDING
    invoices on the next tick).
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        candidate = value
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            candidate = int(stripped)
        except ValueError:
            return None
    else:
        return None
    if candidate < EXPIRATION_HOURS_MINIMUM:
        return None
    if candidate > EXPIRATION_HOURS_OVERRIDE_MAXIMUM:
        return None
    return candidate


def set_expiration_hours_override(value: int) -> None:
    """Apply an in-process override for ``PENDING_EXPIRATION_HOURS``.

    Raises ``ValueError`` if *value* fails coercion. The web admin
    handler that wires this up always pre-validates the form input,
    but defence-in-depth so a bad value can't slip through if a
    future caller forgets.
    """
    coerced = _coerce_expiration_hours(value)
    if coerced is None:
        raise ValueError(
            f"PENDING_EXPIRATION_HOURS override {value!r} must be an "
            f"int in [{EXPIRATION_HOURS_MINIMUM}, "
            f"{EXPIRATION_HOURS_OVERRIDE_MAXIMUM}]"
        )
    global _EXPIRATION_HOURS_OVERRIDE
    _EXPIRATION_HOURS_OVERRIDE = coerced


def clear_expiration_hours_override() -> bool:
    """Drop the in-process override. Returns ``True`` if one was active."""
    global _EXPIRATION_HOURS_OVERRIDE
    had = _EXPIRATION_HOURS_OVERRIDE is not None
    _EXPIRATION_HOURS_OVERRIDE = None
    return had


def get_expiration_hours_override() -> int | None:
    """Return the current in-process override (or ``None``)."""
    return _EXPIRATION_HOURS_OVERRIDE


def reset_expiration_hours_override_for_tests() -> None:
    """Test-helper: drop the override slot without the public API.

    Mirrors :func:`bot_health_alert.reset_alert_interval_override_for_tests`
    so a test fixture can reset the slot without going through the
    public mutator (which logs).
    """
    global _EXPIRATION_HOURS_OVERRIDE
    _EXPIRATION_HOURS_OVERRIDE = None


async def refresh_expiration_hours_override_from_db(database) -> int | None:
    """Reload the override from the ``system_settings`` overlay.

    Best-effort: a transient DB error keeps the previous cache value
    in place rather than silently reverting to env / default.
    A malformed stored value (non-int, below minimum, above max) is
    rejected with a WARNING and the override is cleared so the
    resolver falls back to env / default — avoids a poisoned row
    silently disabling the reaper.
    """
    global _EXPIRATION_HOURS_OVERRIDE
    if database is None:
        return _EXPIRATION_HOURS_OVERRIDE
    try:
        raw = await database.get_setting(EXPIRATION_HOURS_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_expiration_hours_override_from_db: get_setting "
            "failed; keeping previous cache value=%r",
            _EXPIRATION_HOURS_OVERRIDE,
        )
        return _EXPIRATION_HOURS_OVERRIDE
    if raw is None:
        _EXPIRATION_HOURS_OVERRIDE = None
        return None
    coerced = _coerce_expiration_hours(raw)
    if coerced is None:
        log.warning(
            "refresh_expiration_hours_override_from_db: rejected stored "
            "value %r; clearing override",
            raw,
        )
        _EXPIRATION_HOURS_OVERRIDE = None
        return None
    _EXPIRATION_HOURS_OVERRIDE = coerced
    return coerced


def get_pending_expiration_hours() -> int:
    """Resolve the reaper threshold with DB → env → default precedence.

    The reaper loop, the manual ``/admin/control`` Tick-now button,
    and any future caller all should funnel through this resolver so
    a saved override is honoured uniformly.
    """
    if _EXPIRATION_HOURS_OVERRIDE is not None:
        return _EXPIRATION_HOURS_OVERRIDE
    return _read_int_env("PENDING_EXPIRATION_HOURS", EXPIRATION_HOURS_DEFAULT)


def get_pending_expiration_hours_source() -> str:
    """Return ``db`` / ``env`` / ``default`` for the resolved threshold.

    Used by the panel's "source" badge so the operator can tell at a
    glance whether a saved override is live or whether the loop is
    falling through to env / compile-time default.
    """
    if _EXPIRATION_HOURS_OVERRIDE is not None:
        return "db"
    raw = os.getenv("PENDING_EXPIRATION_HOURS", "").strip()
    if not raw:
        return "default"
    try:
        int(raw)
    except ValueError:
        # ``_read_int_env`` falls back to the compile-time default
        # for non-numeric env values, so the SOURCE is "default".
        return "default"
    # Any numeric env value — even one ``_read_int_env`` clamps below
    # ``minimum`` — counts as "env" because the operator did set it.
    return "env"


def _read_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    """Parse a small integer env var with a clamping floor.

    Wrapped so a deploy-time typo (``PENDING_EXPIRATION_HOURS=abc``)
    falls back to the default with a loud log instead of crashing the
    boot sequence. The ``minimum`` floor stops a deployer from
    accidentally setting the threshold to 0 (which would EXPIRE every
    row in the table on the first tick).
    """
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.error(
            "%s=%r is not an integer; using default %d", name, raw, default
        )
        return default
    if value < minimum:
        log.error(
            "%s=%d is below the minimum %d; using minimum", name, value, minimum
        )
        return minimum
    return value


async def _notify_expired(bot: Bot, row: dict) -> None:
    """Best-effort courtesy ping to the user that their invoice closed.

    Pre-fix the bot stayed silent on EXPIRED so a user who funded an
    invoice in the wrong tab never knew their first tab had closed —
    they'd come back later, see the invoice gone, and rage-DM
    support. Sending an explicit "your invoice expired" line + a
    nudge to start a fresh top-up cuts that support volume.

    All Telegram-side exceptions are swallowed (the user blocked the
    bot, the chat doesn't exist, etc.) — none of them should block
    the reaper from continuing through the rest of the batch.
    """
    telegram_id = row.get("telegram_id")
    if telegram_id is None:
        return
    try:
        lang = await db.get_user_language(int(telegram_id))
    except Exception:
        lang = None
    text = strings.t(lang or strings.DEFAULT_LANGUAGE, "pay_expired_pending")
    try:
        await bot.send_message(int(telegram_id), text)
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        log.warning(
            "expire-stale-pending notify telegram_id=%s skipped: %s",
            telegram_id,
            exc,
        )
    except Exception:
        log.exception(
            "expire-stale-pending notify telegram_id=%s failed", telegram_id
        )


async def _record_expiration_audit(
    row: dict, *, threshold_hours_used: int | None = None
) -> None:
    """Drop one ``payment_expired`` row into ``admin_audit_log``.

    Marked actor=``"reaper"`` so forensics can distinguish reaper-
    closed invoices from operator-closed ones (operator closes flow
    through ``mark_transaction_terminal`` from the IPN handler with
    the IPN's source IP).

    Stage-15-Step-E #10b row 9 bundled bug fix — ``threshold_hours_used``
    is the resolved threshold the reaper passed to
    ``Database.expire_stale_pending`` for the batch this row was part
    of. Pre-fix the audit row carried no threshold metadata, so an
    investigator looking at an EXPIRED row weeks later couldn't tell
    whether it expired under the default 24h or a custom override —
    which made "did we lose a paid invoice because the threshold was
    set too aggressively?" unanswerable. Now every audit row pins the
    exact threshold the reaper used for that batch.
    """
    try:
        await db.record_admin_audit(
            "reaper",
            "payment_expired",
            target=f"transaction:{row.get('transaction_id', '?')}",
            outcome="ok",
            meta={
                "gateway_invoice_id": row.get("gateway_invoice_id"),
                "telegram_id": row.get("telegram_id"),
                "amount_usd_credited": row.get("amount_usd_credited"),
                "currency_used": row.get("currency_used"),
                "created_at": row.get("created_at"),
                "threshold_hours_used": threshold_hours_used,
            },
        )
    except Exception:
        # Audit-write failure must never block the reaper. The flip
        # to EXPIRED already committed; a failed audit is a logging
        # gap, not a correctness gap.
        log.exception(
            "expire-stale-pending audit write failed for transaction_id=%s",
            row.get("transaction_id"),
        )


async def expire_pending_once(
    bot: Bot,
    *,
    threshold_hours: int,
    batch_limit: int = 1000,
) -> int:
    """Run a single reap pass. Returns the number of rows expired.

    Exposed so admins can poke it manually from a Python shell or via
    a future ``/admin/payment_health`` "Expire now" button without
    the 15-minute wait. Tests also call this directly to avoid
    setting up a long-running asyncio.Task.
    """
    try:
        rows = await db.expire_stale_pending(
            threshold_hours=threshold_hours, limit=batch_limit
        )
    except Exception:
        log.exception(
            "expire_stale_pending DB call failed (threshold_hours=%d)",
            threshold_hours,
        )
        return 0

    if not rows:
        return 0

    log.info(
        "reaper expired %d stuck PENDING transactions "
        "(threshold_hours=%d)",
        len(rows),
        threshold_hours,
    )
    for row in rows:
        await _record_expiration_audit(
            row, threshold_hours_used=threshold_hours,
        )
        await _notify_expired(bot, row)
    return len(rows)


async def _tick_pending_reaper_from_app(app) -> None:
    """Run a single ``pending_reaper`` pass, deps from *app*.

    Stage-15-Step-E #10b row 9: routes the threshold through
    :func:`get_pending_expiration_hours` so a saved override is
    honoured by the manual ``Tick now`` button on
    ``/admin/control``. Pre-fix this read the env var directly,
    which would have made the manual tick path silently bypass
    any DB override the operator had applied — surprising and
    inconsistent with the loop's iteration-time behaviour.
    """
    from web_admin import APP_KEY_BOT  # local: avoid import cycle

    bot = app.get(APP_KEY_BOT)
    if bot is None:
        raise RuntimeError(
            "pending_reaper tick-now: bot not in app state — "
            "manual ticks require a bot to DM expired senders."
        )
    threshold_hours = get_pending_expiration_hours()
    batch_limit = _read_int_env("PENDING_EXPIRATION_BATCH", 1000)
    await expire_pending_once(
        bot,
        threshold_hours=threshold_hours,
        batch_limit=batch_limit,
    )


@register_loop(
    "pending_reaper",
    cadence_seconds=15 * 60,
    runner=_tick_pending_reaper_from_app,
)
async def _expiration_loop(
    bot: Bot,
    *,
    interval_seconds: int,
    threshold_hours: int,
    batch_limit: int,
) -> None:
    """The forever-running reaper. Cancellation-safe: the outer
    ``CancelledError`` propagates so ``asyncio.Task.cancel()`` from the
    main shutdown path joins cleanly without leaving a zombie loop.

    A single iteration that raises an exception is logged and the loop
    keeps going — we'd rather miss one tick than have the reaper die
    silently and let the backlog accumulate again.

    Stage-15-Step-E #10b row 9: ``threshold_hours`` is the value
    captured at startup but the loop re-reads it via
    :func:`get_pending_expiration_hours` on every iteration so a
    saved DB override takes effect on the next tick — no restart
    required. The kwarg is retained as the bootstrap value for the
    very first iteration (mirrors :func:`bot_health_alert._alert_loop`).
    """
    log.info(
        "pending-expiration reaper started "
        "(interval=%ds, threshold=%dh, batch=%d)",
        interval_seconds,
        threshold_hours,
        batch_limit,
    )
    next_threshold = threshold_hours
    try:
        while True:
            try:
                await expire_pending_once(
                    bot,
                    threshold_hours=next_threshold,
                    batch_limit=batch_limit,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("expire_pending_once raised; will retry next tick")
            else:
                # Stage-15-Step-A: heartbeat for
                # ``meowassist_pending_reaper_last_run_epoch``.
                from metrics import record_loop_tick

                record_loop_tick("pending_reaper")
            await asyncio.sleep(interval_seconds)
            try:
                next_threshold = get_pending_expiration_hours()
            except Exception:
                log.exception(
                    "get_pending_expiration_hours raised; keeping "
                    "previous threshold %dh",
                    next_threshold,
                )
    except asyncio.CancelledError:
        log.info("pending-expiration reaper cancelled; exiting cleanly")
        raise


def start_pending_expiration_task(bot: Bot) -> asyncio.Task:
    """Spawn the reaper task and return its handle.

    The caller (``main.main``) is responsible for cancelling +
    awaiting the task during shutdown so the asyncio.run() loop
    closes cleanly. We deliberately do NOT register a signal
    handler here — the bot already has one in ``main.main`` via
    aiogram's polling shutdown plumbing.

    Reads three env vars (with defaults):
      * ``PENDING_EXPIRATION_INTERVAL_MIN`` (default 15)
      * ``PENDING_EXPIRATION_HOURS`` (default 24)
      * ``PENDING_EXPIRATION_BATCH`` (default 1000)
    """
    interval_seconds = (
        _read_int_env("PENDING_EXPIRATION_INTERVAL_MIN", 15) * 60
    )
    # Stage-15-Step-E #10b row 9: bootstrap from the same resolver
    # the loop will re-read on every iteration so a DB override
    # warmed by ``main.warm_up_caches`` is picked up immediately —
    # the very first reaper tick after boot uses the operator's
    # configured threshold rather than env / compile-time default.
    threshold_hours = get_pending_expiration_hours()
    batch_limit = _read_int_env("PENDING_EXPIRATION_BATCH", 1000)

    return asyncio.create_task(
        _expiration_loop(
            bot,
            interval_seconds=interval_seconds,
            threshold_hours=threshold_hours,
            batch_limit=batch_limit,
        ),
        name="pending-expiration-reaper",
    )
