"""Background backfill reaper for Zarinpal browser-close races.

Stage-15-Step-E #8 follow-up #2.

Why this exists
---------------

Zarinpal's ``?Authority=...&Status=OK`` callback is a USER-AGENT
redirect, not a server-to-server webhook. The user lands on
``zarinpal.com/pg/StartPay/<authority>``, completes the card flow,
and Zarinpal redirects their browser back to our
``/zarinpal-callback?Authority=...&Status=OK`` endpoint. If the
user closes the browser tab between the gateway settling the
order and the redirect actually firing, we never get the success
signal — but Zarinpal's side has the order marked SETTLED.

Without this reaper, those orders sit in our ``transactions``
ledger as ``PENDING`` until the 24-hour
:meth:`Database.expire_stale_pending` sweep flips them to
``EXPIRED``. The user's wallet never gets credited despite their
card having been charged. Support tickets ensue.

The reaper closes the gap: a periodic task that wakes every
``ZARINPAL_BACKFILL_INTERVAL_MIN`` minutes (default 5), fetches
the small window of PENDING Zarinpal rows older than
``ZARINPAL_BACKFILL_MIN_AGE_MINUTES`` (default 5) and younger than
``ZARINPAL_BACKFILL_MAX_AGE_HOURS`` (default 23), and for each row:

  1. Calls :func:`zarinpal.verify_payment` — the same authoritative
     settlement check the user-redirect callback would have made.
     A successful verify means the gateway has the order settled
     and we just missed the redirect.
  2. Calls :meth:`Database.finalize_payment` — same idempotent
     credit path the callback uses. The FOR UPDATE + status check
     in ``finalize_payment`` makes parallel reaper / callback
     deliveries safe (whichever wins commits first; the loser's
     UPDATE returns 0 rows and ``finalize_payment`` returns
     ``None``).
  3. Sends the credit DM the user would have seen on the
     successful redirect — same string (``zarinpal_credit_notification``)
     so the user's experience is identical regardless of whether
     the callback or the backfill triggered the credit.
  4. Records an audit row marked ``actor="zarinpal_backfill"`` so
     forensics can distinguish backfill-credited rows from
     callback-credited ones.

Why TetraPay doesn't need a backfill reaper
-------------------------------------------

TetraPay's webhook is a true server-to-server POST with retries
on 5xx responses. The browser-close gap doesn't exist because
the gateway re-delivers until our endpoint 200s. NowPayments is
the same story.

Jurisdictional split with the expire reaper
-------------------------------------------

* Backfill owns: ``[min_age, max_age]`` window. Default 5 min — 23 h.
* Expire owns: ``> 24 h`` (the
  :meth:`Database.expire_stale_pending` threshold).

The 1-hour buffer between ``ZARINPAL_BACKFILL_MAX_AGE_HOURS=23``
and ``PENDING_EXPIRATION_HOURS=24`` is intentional: it gives the
backfill reaper a final ~12 ticks (at 5-minute intervals) to
catch a row before the expire reaper turns it into a noop. If an
operator overrides the env vars to remove the buffer, the only
risk is a race where expire fires between two backfill ticks —
the backfill would then see an EXPIRED row, ``finalize_payment``
would refuse it, and the user goes uncredited. Don't do that;
the README documents the recommended values.

Concurrency-safety against the user redirect
--------------------------------------------

If the user finally re-opens their tab while the backfill is
mid-verify on the same row, the redirect callback also calls
verify_payment. Zarinpal's ``code=101`` (already verified) is
treated as success by both code paths, so neither delivery is
lost. The first :meth:`finalize_payment` to acquire the row's
``FOR UPDATE`` lock credits; the second sees the row is already
SUCCESS and returns ``None``. Worst case the user gets two
"کیف پول شما شارژ شد" DMs — minor and survivable. We don't
attempt to suppress the second DM because doing so would require
a ledger column for "DM sent" + a write under the row lock,
which is a lot of plumbing for a cosmetic dedupe.
"""

from __future__ import annotations

import asyncio
import logging
import os

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

import strings
import zarinpal
from bot_health import register_loop
from database import db
from metrics import record_loop_tick


log = logging.getLogger("bot.zarinpal_backfill")


# Env-var defaults. The reaper interval is shorter than
# pending_expiration's 15 min because Zarinpal users typically
# come back within seconds-to-minutes of paying — running every
# 5 minutes is a sensible upper bound on credit latency for the
# browser-close case.
_DEFAULT_INTERVAL_MIN = 5
_DEFAULT_MIN_AGE_MIN = 5
_DEFAULT_MAX_AGE_HOURS = 23
_DEFAULT_BATCH_LIMIT = 100


def _read_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    """Parse a small integer env var with a clamping floor.

    Mirrors :func:`pending_expiration._read_int_env`. Wrapped so a
    deploy-time typo (``ZARINPAL_BACKFILL_INTERVAL_MIN=abc``) falls
    back to the default with a loud log instead of crashing the
    boot sequence. The ``minimum`` floor stops a deployer from
    accidentally setting the threshold to 0 (which would either
    spin the reaper hot or pull every PENDING row including ones
    a normal callback should be allowed to land first).
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
            "%s=%d is below the minimum %d; using minimum",
            name, value, minimum,
        )
        return minimum
    return value


# Per-process counters. Mirror the
# :data:`zarinpal._ZARINPAL_DROP_COUNTERS` shape so future ops
# panels can plug them in next to the callback drop counts.
_BACKFILL_COUNTERS: dict[str, int] = {
    "rows_examined": 0,
    "credited": 0,
    "verify_failed": 0,
    "transport_error": 0,
    "finalize_noop": 0,
    "audit_failed": 0,
}


def _bump_counter(reason: str) -> None:
    _BACKFILL_COUNTERS[reason] = _BACKFILL_COUNTERS.get(reason, 0) + 1


def get_zarinpal_backfill_counters() -> dict[str, int]:
    """Snapshot copy of the backfill reaper's per-process counters."""
    return dict(_BACKFILL_COUNTERS)


def reset_counters_for_tests() -> None:
    """Tests-only: clear the per-process counters."""
    for key in list(_BACKFILL_COUNTERS):
        _BACKFILL_COUNTERS[key] = 0


async def _credit_row_if_settled(bot: Bot, row: dict) -> bool:
    """Verify the order on Zarinpal's side and finalize if settled.

    Returns True iff the row was credited.

    Every failure mode logs at the appropriate level and returns
    False — the reaper loop continues with the next row. The
    counters bucket the outcome so an operator looking at the
    panel can tell at a glance whether the backfill is healthy
    (most rows credited or verify_failed) or sick (most rows
    transport_error).
    """
    authority = row["gateway_invoice_id"]
    locked_irr = int(row["locked_irr"])
    locked_usd = float(row["locked_usd"])
    telegram_id = row["telegram_id"]

    try:
        await zarinpal.verify_payment(authority, locked_irr)
    except zarinpal.ZarinpalError as exc:
        # Gateway said "no, this order is not settled" — could mean
        # the user genuinely never paid, or paid and Zarinpal hasn't
        # finished settling on their side yet (Shaparak settlement
        # is fast but not synchronous). Either way the backfill
        # should not credit. The next reaper tick will retry.
        _bump_counter("verify_failed")
        log.info(
            "backfill verify rejected authority=%s code=%r: %s",
            authority, exc.code, exc,
        )
        return False
    except Exception as exc:  # noqa: BLE001 — transport / unexpected
        # asyncio.TimeoutError, aiohttp.ClientError, or anything
        # else the verify path might raise. Treat as transient and
        # retry next tick.
        _bump_counter("transport_error")
        log.warning(
            "backfill verify transport error authority=%s: %s",
            authority, exc,
        )
        return False

    try:
        result = await db.finalize_payment(authority, locked_usd)
    except Exception:
        _bump_counter("transport_error")
        log.exception(
            "backfill finalize_payment crashed for authority=%s",
            authority,
        )
        return False

    if result is None:
        # The row was credited by the user-redirect callback in
        # the milliseconds between our list query and our
        # finalize call, OR the row left PENDING for some other
        # reason. Either way: not our problem, log + move on.
        _bump_counter("finalize_noop")
        log.info(
            "backfill finalize_payment noop for authority=%s "
            "(callback raced ahead, row no longer PENDING)",
            authority,
        )
        return False

    delta_credited = float(result["delta_credited"])
    bonus_credited = float(result.get("promo_bonus_credited") or 0.0)

    # Audit row. Marked actor="zarinpal_backfill" so a future
    # operator can SQL-filter callback-credited rows from
    # backfill-credited ones.
    try:
        await db.record_admin_audit(
            "zarinpal_backfill",
            "zarinpal_backfill_credited",
            target=f"transaction:{result.get('transaction_id', '?')}",
            outcome="ok",
            meta={
                "authority": authority,
                "telegram_id": telegram_id,
                "delta_usd": delta_credited,
                "bonus_usd": bonus_credited,
            },
        )
    except Exception:
        # Audit is observability, not correctness. Wallet was
        # credited; a missed audit row is a logging gap.
        _bump_counter("audit_failed")
        log.exception(
            "backfill audit row failed for authority=%s "
            "(wallet was already credited; non-critical)",
            authority,
        )

    # Best-effort credit DM. Same string the callback uses so the
    # user's experience is identical regardless of which path
    # delivered.
    try:
        lang = await db.get_user_language(int(telegram_id))
    except Exception:
        lang = None
    text = strings.t(
        lang or strings.DEFAULT_LANGUAGE,
        "zarinpal_credit_notification",
        amount=delta_credited,
    )
    if bonus_credited > 0:
        text = text + "\n\n" + strings.t(
            lang or strings.DEFAULT_LANGUAGE,
            "pay_promo_bonus",
            bonus=bonus_credited,
        )
    try:
        await bot.send_message(
            int(telegram_id), text, parse_mode="Markdown",
        )
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        # User blocked the bot or deleted their account. Wallet
        # was already credited; log and move on.
        log.info(
            "backfill credit DM undeliverable user=%s authority=%s: %s",
            telegram_id, authority, exc,
        )
    except Exception:
        log.exception(
            "backfill credit DM failed user=%s authority=%s "
            "(wallet was already credited; non-critical)",
            telegram_id, authority,
        )

    _bump_counter("credited")
    log.info(
        "backfill credited authority=%s user=%s delta=%.6f bonus=%.6f",
        authority, telegram_id, delta_credited, bonus_credited,
    )
    return True


async def backfill_pending_once(
    bot: Bot,
    *,
    min_age_seconds: int,
    max_age_hours: int,
    batch_limit: int = _DEFAULT_BATCH_LIMIT,
) -> int:
    """Run a single backfill pass. Returns the number of rows credited.

    Exposed so admins can poke the reaper from a Python shell or a
    future ``/admin/payment_health`` "Run Zarinpal backfill now"
    button without the 5-minute wait. Tests also call this directly
    to avoid the long-running asyncio.Task.
    """
    try:
        rows = await db.list_pending_zarinpal_for_backfill(
            min_age_seconds=min_age_seconds,
            max_age_hours=max_age_hours,
            limit=batch_limit,
        )
    except Exception:
        log.exception(
            "backfill list query failed (min_age=%ds, max_age=%dh)",
            min_age_seconds, max_age_hours,
        )
        return 0

    if not rows:
        return 0

    credited = 0
    for row in rows:
        _bump_counter("rows_examined")
        try:
            if await _credit_row_if_settled(bot, row):
                credited += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            # Per-row safety net so one corrupt row doesn't kill
            # the whole batch. _credit_row_if_settled already has
            # try/except around its own steps, but a programming
            # error inside this module would otherwise propagate.
            log.exception(
                "backfill unexpected crash on authority=%s",
                row.get("gateway_invoice_id"),
            )

    if credited:
        log.info(
            "backfill credited %d/%d Zarinpal PENDING rows",
            credited, len(rows),
        )
    return credited


@register_loop(
    "zarinpal_backfill", cadence_seconds=_DEFAULT_INTERVAL_MIN * 60,
)
async def _backfill_loop(
    bot: Bot,
    *,
    interval_seconds: int,
    min_age_seconds: int,
    max_age_hours: int,
    batch_limit: int,
) -> None:
    """The forever-running backfill reaper.

    Cancellation-safe: outer ``CancelledError`` propagates so
    ``asyncio.Task.cancel()`` from the main shutdown path joins
    cleanly without leaving a zombie loop. Per-tick exceptions are
    logged and the loop keeps going — we'd rather miss one tick
    than have the reaper die silently.
    """
    log.info(
        "zarinpal-backfill reaper started "
        "(interval=%ds, min_age=%ds, max_age=%dh, batch=%d)",
        interval_seconds, min_age_seconds, max_age_hours, batch_limit,
    )
    try:
        while True:
            try:
                await backfill_pending_once(
                    bot,
                    min_age_seconds=min_age_seconds,
                    max_age_hours=max_age_hours,
                    batch_limit=batch_limit,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "backfill_pending_once raised; will retry next tick"
                )
            else:
                # Heartbeat for
                # ``meowassist_zarinpal_backfill_last_run_epoch``.
                record_loop_tick("zarinpal_backfill")
            await asyncio.sleep(interval_seconds)
    except asyncio.CancelledError:
        log.info("zarinpal-backfill reaper cancelled; exiting cleanly")
        raise


def start_zarinpal_backfill_task(bot: Bot) -> asyncio.Task:
    """Spawn the backfill reaper task and return its handle.

    The caller (``main.main``) is responsible for cancelling +
    awaiting the task during shutdown so the asyncio.run() loop
    closes cleanly.

    Reads four env vars (with defaults):
      * ``ZARINPAL_BACKFILL_INTERVAL_MIN`` (default 5)
      * ``ZARINPAL_BACKFILL_MIN_AGE_MINUTES`` (default 5)
      * ``ZARINPAL_BACKFILL_MAX_AGE_HOURS`` (default 23) — must
        stay below ``PENDING_EXPIRATION_HOURS`` (default 24) to
        avoid the jurisdictional overlap with
        ``pending_expiration``.
      * ``ZARINPAL_BACKFILL_BATCH`` (default 100)
    """
    interval_seconds = (
        _read_int_env(
            "ZARINPAL_BACKFILL_INTERVAL_MIN",
            _DEFAULT_INTERVAL_MIN,
        ) * 60
    )
    min_age_seconds = (
        _read_int_env(
            "ZARINPAL_BACKFILL_MIN_AGE_MINUTES",
            _DEFAULT_MIN_AGE_MIN,
        ) * 60
    )
    max_age_hours = _read_int_env(
        "ZARINPAL_BACKFILL_MAX_AGE_HOURS",
        _DEFAULT_MAX_AGE_HOURS,
    )
    batch_limit = _read_int_env(
        "ZARINPAL_BACKFILL_BATCH",
        _DEFAULT_BATCH_LIMIT,
    )

    return asyncio.create_task(
        _backfill_loop(
            bot,
            interval_seconds=interval_seconds,
            min_age_seconds=min_age_seconds,
            max_age_hours=max_age_hours,
            batch_limit=batch_limit,
        ),
        name="zarinpal-backfill-reaper",
    )


__all__ = [
    "backfill_pending_once",
    "get_zarinpal_backfill_counters",
    "reset_counters_for_tests",
    "start_zarinpal_backfill_task",
]
