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


async def _record_expiration_audit(row: dict) -> None:
    """Drop one ``payment_expired`` row into ``admin_audit_log``.

    Marked actor=``"reaper"`` so forensics can distinguish reaper-
    closed invoices from operator-closed ones (operator closes flow
    through ``mark_transaction_terminal`` from the IPN handler with
    the IPN's source IP).
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
        await _record_expiration_audit(row)
        await _notify_expired(bot, row)
    return len(rows)


@register_loop("pending_reaper", cadence_seconds=15 * 60)
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
    """
    log.info(
        "pending-expiration reaper started "
        "(interval=%ds, threshold=%dh, batch=%d)",
        interval_seconds,
        threshold_hours,
        batch_limit,
    )
    try:
        while True:
            try:
                await expire_pending_once(
                    bot,
                    threshold_hours=threshold_hours,
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
    threshold_hours = _read_int_env("PENDING_EXPIRATION_HOURS", 24)
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
