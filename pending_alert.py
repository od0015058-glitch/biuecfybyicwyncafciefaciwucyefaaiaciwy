"""Background loop that DMs admins about stuck PENDING transactions.

Stage-12-Step-B. Stage-9-Step-9 surfaces "Pending payments: N
(oldest: Xh ago)" on the admin dashboard, but an admin who isn't
*actively* looking at the dashboard has no way to know an invoice is
stuck — IPN delivery delay, gateway flap, or webhook misconfiguration
can pile up ``PENDING`` rows for hours before anyone notices. The
reaper (:mod:`pending_expiration`) doesn't catch this either, because
its threshold is the terminal 24 h cleanup line — anything younger is
left alone (correctly: NowPayments invoices can legitimately sit
PENDING for tens of minutes during a slow chain confirmation).

The contract:

* Wakes every ``PENDING_ALERT_INTERVAL_MIN`` minutes (default 30).
* DMs every admin in :func:`admin.get_admin_user_ids` when *any*
  ``PENDING`` row's age exceeds ``PENDING_ALERT_THRESHOLD_HOURS``
  (default 2).
* Per-row alert key = ``(transaction_id, hour_bucket)`` so the same
  stuck row doesn't spam the same alert every 30 min — once per
  hour-bucket per transaction. ``hour_bucket`` is
  ``floor(age_hours)`` so an invoice that crosses the 2 h line gets
  one alert at hour 2, another at hour 3, etc., capped naturally
  by the reaper at hour 24.
* Per-admin fault isolation, mirroring
  :func:`model_discovery.notify_admins_of_price_deltas`: a
  ``TelegramForbiddenError`` (admin blocked the bot) on admin A is
  logged INFO and skipped; ``TelegramAPIError`` is logged and skipped;
  the loop never crashes.
* Bootstrap: dedupe state lives in process memory, so a restart can
  re-alert once on already-stuck rows. That's intentional — an
  operator who deployed a fix expects the alert to re-fire if the
  fix didn't actually unstick the rows.

Why a separate module rather than tacking onto
:mod:`pending_expiration`: the reaper *closes* rows; this loop
*notifies about them*. They have different cadences, different
thresholds, and different failure semantics (the reaper's failure is
silent log; this loop's failure is no DM). Keeping them split makes
it obvious which knob does which thing.
"""

from __future__ import annotations

import asyncio
import logging
import os

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

from admin import get_admin_user_ids
from bot_health import register_loop
from database import db


log = logging.getLogger("bot.pending_alert")


# ---------------------------------------------------------------------
# env var parsing
# ---------------------------------------------------------------------

# Defaults are documented in HANDOFF Stage-12-Step-B and surfaced in
# ``.env.example``. The minimum for the threshold is 1 h (a sub-hour
# alert would stutter on rows that just hit the line, and we want
# this loop to be quiet by default — its job is "something is wrong",
# not "an invoice is taking longer than usual"). The minimum for the
# interval is 1 min (mostly so tests can drive it fast; in production
# you'd never set it below the threshold).
_PENDING_ALERT_INTERVAL_MIN_DEFAULT = 30
_PENDING_ALERT_THRESHOLD_HOURS_DEFAULT = 2
_PENDING_ALERT_LIMIT_DEFAULT = 500
# Cap on rows enumerated in the DM body. Anything above this is
# summarised as "+N more". Picked low because Telegram messages over
# a few KB get clipped client-side and an admin staring at 50 lines
# of forensic data isn't going to act faster than one staring at 10.
_PENDING_ALERT_MAX_ROWS_IN_BODY = 10


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 10: DB-backed override for the
# PENDING_ALERT_THRESHOLD_HOURS knob.
# ---------------------------------------------------------------------
#
# Same shape as the Row-#9 override layer in ``pending_expiration``.
# The reaper threshold (``PENDING_EXPIRATION_HOURS``) is the *terminal*
# 24h cleanup line; the alert threshold (``PENDING_ALERT_THRESHOLD_HOURS``,
# default 2h) is the much-earlier "something is wrong" line. Operators
# need to retune the latter without a redeploy when traffic patterns
# change (e.g. a slow chain that legitimately keeps invoices PENDING
# for 4+h would page admins constantly under the default 2h threshold).

ALERT_THRESHOLD_SETTING_KEY: str = "PENDING_ALERT_THRESHOLD_HOURS"
ALERT_THRESHOLD_DEFAULT: int = _PENDING_ALERT_THRESHOLD_HOURS_DEFAULT
ALERT_THRESHOLD_MINIMUM: int = 1
# 1-year cap on the override slot. The alert threshold is *less* than
# the reaper threshold by definition (2h vs 24h), so values above 24h
# don't make operational sense — but the slot is bounded by the
# reaper's own cap rather than the reaper threshold (which is itself
# a runtime-tunable override) so the bounds don't go stale if Row #9
# is also retuned.
ALERT_THRESHOLD_OVERRIDE_MAXIMUM: int = 24 * 365
_ALERT_THRESHOLD_OVERRIDE: int | None = None


def _coerce_alert_threshold_hours(value: object) -> int | None:
    """Validate a threshold-hours candidate for the override slot.

    Rejects ``bool`` explicitly even though it's an ``int`` subclass —
    a stored ``"true"`` / ``"True"`` row would otherwise coerce to
    ``1`` and shrink the threshold to "anything PENDING for an hour
    is suspicious", which would page admins constantly.
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
    if candidate < ALERT_THRESHOLD_MINIMUM:
        return None
    if candidate > ALERT_THRESHOLD_OVERRIDE_MAXIMUM:
        return None
    return candidate


def set_alert_threshold_override(value: int) -> None:
    """Apply an in-process override for the alert threshold.

    Defence-in-depth: re-validates via :func:`_coerce_alert_threshold_hours`
    so a future caller that bypasses the web-UI's coercer (e.g. a
    test that sets a bad value directly) still gets a clean rejection.
    """
    coerced = _coerce_alert_threshold_hours(value)
    if coerced is None:
        raise ValueError(
            f"alert_threshold must be int in "
            f"[{ALERT_THRESHOLD_MINIMUM}, {ALERT_THRESHOLD_OVERRIDE_MAXIMUM}], "
            f"got {value!r}"
        )
    global _ALERT_THRESHOLD_OVERRIDE
    _ALERT_THRESHOLD_OVERRIDE = coerced


def clear_alert_threshold_override() -> bool:
    """Drop the in-process override. Returns ``True`` if one was active."""
    global _ALERT_THRESHOLD_OVERRIDE
    had = _ALERT_THRESHOLD_OVERRIDE is not None
    _ALERT_THRESHOLD_OVERRIDE = None
    return had


def get_alert_threshold_override() -> int | None:
    """Return the current in-process override (or ``None``)."""
    return _ALERT_THRESHOLD_OVERRIDE


def reset_alert_threshold_override_for_tests() -> None:
    """Test-helper: drop the override slot without the public API.

    Mirrors :func:`pending_expiration.reset_expiration_hours_override_for_tests`
    so the autouse reset fixture in ``test_pending_alert.py`` can
    null the slot in setUp / tearDown.
    """
    global _ALERT_THRESHOLD_OVERRIDE
    _ALERT_THRESHOLD_OVERRIDE = None


async def refresh_alert_threshold_override_from_db(database) -> int | None:
    """Reload the override from the ``system_settings`` overlay.

    Best-effort: a transient DB error keeps the cache value in place
    (logged at ERROR). A malformed value (non-int, below min, above
    max) clears the cache (logged at WARNING). Returns the new cache
    value.
    """
    global _ALERT_THRESHOLD_OVERRIDE
    if database is None:
        return _ALERT_THRESHOLD_OVERRIDE
    try:
        raw = await database.get_setting(ALERT_THRESHOLD_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_alert_threshold_override_from_db: get_setting failed; "
            "keeping previous cache value"
        )
        return _ALERT_THRESHOLD_OVERRIDE
    if raw is None:
        _ALERT_THRESHOLD_OVERRIDE = None
        return None
    coerced = _coerce_alert_threshold_hours(raw)
    if coerced is None:
        log.warning(
            "refresh_alert_threshold_override_from_db: rejected stored "
            "value %r — clearing override and falling through to env / default",
            raw,
        )
        _ALERT_THRESHOLD_OVERRIDE = None
        return None
    _ALERT_THRESHOLD_OVERRIDE = coerced
    return coerced


def _read_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    """Parse a small integer env var with a clamping floor.

    Mirrors the helper in :mod:`pending_expiration` (intentionally
    duplicated rather than imported so a future refactor of one
    module doesn't accidentally change the parsing semantics of the
    other). A deploy-time typo logs loudly and falls back to the
    default rather than crashing the boot sequence.
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


def get_pending_alert_threshold_hours() -> int:
    """Read ``PENDING_ALERT_THRESHOLD_HOURS`` with the canonical floor.

    Exposed so the dashboard tile (``web_admin.dashboard``) and this
    loop's invocation of :meth:`Database.list_pending_payments_over_threshold`
    use the *same* threshold without duplicating the env-parse logic
    (the bug fix in Stage-12-Step-B — the original dashboard tile
    read ``MIN(created_at)`` while there was no separate "overdue"
    notion).

    Stage-15-Step-E #10b row 10: DB-backed override beats env beats
    compile-time default. The :func:`_alert_loop` re-reads this on
    every iteration so a saved override takes effect on the next
    tick (no restart). The :func:`_tick_pending_alert_from_app`
    manual-tick path already calls this resolver, so the "Tick now"
    button respects overrides too.
    """
    if _ALERT_THRESHOLD_OVERRIDE is not None:
        return _ALERT_THRESHOLD_OVERRIDE
    return _read_int_env(
        "PENDING_ALERT_THRESHOLD_HOURS",
        _PENDING_ALERT_THRESHOLD_HOURS_DEFAULT,
    )


def get_pending_alert_threshold_source() -> str:
    """Return ``db`` / ``env`` / ``default`` for the resolved threshold.

    Mirrors :func:`pending_expiration.get_pending_expiration_hours_source`.
    Used by the source-badge in the ``/admin/control`` editor card so
    operators can see at a glance where the live threshold comes
    from.
    """
    if _ALERT_THRESHOLD_OVERRIDE is not None:
        return "db"
    raw = os.getenv("PENDING_ALERT_THRESHOLD_HOURS", "").strip()
    if not raw:
        return "default"
    try:
        int(raw)
    except ValueError:
        # ``_read_int_env`` falls back to the compile-time default
        # for non-numeric env values, so the resolved threshold is
        # actually the default — surface that.
        return "default"
    return "env"


def get_pending_alert_interval_seconds() -> int:
    """Read ``PENDING_ALERT_INTERVAL_MIN`` with the canonical floor."""
    return _read_int_env(
        "PENDING_ALERT_INTERVAL_MIN",
        _PENDING_ALERT_INTERVAL_MIN_DEFAULT,
    ) * 60


def get_pending_alert_row_limit() -> int:
    """Read ``PENDING_ALERT_LIMIT`` (DB-side row cap)."""
    return _read_int_env(
        "PENDING_ALERT_LIMIT",
        _PENDING_ALERT_LIMIT_DEFAULT,
    )


# ---------------------------------------------------------------------
# alert formatting
# ---------------------------------------------------------------------


def _alert_key(row: dict) -> tuple[int, int]:
    """Compute the dedupe key for one stuck row.

    ``(transaction_id, floor(age_hours))`` — so a row stuck for 2.4 h
    has key ``(123, 2)`` and a row stuck for 3.1 h has ``(123, 3)``,
    re-alerting once per crossed integer hour boundary. Float-only
    granularity would re-alert every tick (every age changes); a
    simple ``transaction_id``-only key would alert exactly once and
    never again until the loop restarts.
    """
    age = row.get("age_hours") or 0.0
    return (int(row["transaction_id"]), int(age))


def _format_alert_body(rows: list[dict], threshold_hours: int) -> str:
    """Render the admin DM body. Plain text (no Markdown) so we don't
    have to escape gateway names or invoice ids that contain ``_`` /
    ``*``.

    Caps the rendered count at :data:`_PENDING_ALERT_MAX_ROWS_IN_BODY`
    and appends an overflow footer if the DB returned more rows than
    we render.
    """
    n = len(rows)
    head = (
        f"⚠️ {n} pending payment(s) stuck over "
        f"{threshold_hours}h:\n\n"
    )
    shown = rows[: _PENDING_ALERT_MAX_ROWS_IN_BODY]
    lines = []
    for r in shown:
        # Defensive ``.get`` accessors so a row with a NULL field
        # (shouldn't happen for PENDING rows, but the metrics loop
        # has more important things to do than 500 on a NULL) renders
        # the line with a placeholder rather than crashing the loop.
        tx = r.get("transaction_id", "?")
        gw = r.get("gateway") or "?"
        amount = r.get("amount_usd_credited") or 0.0
        gateway_invoice_id = r.get("gateway_invoice_id") or "?"
        age = float(r.get("age_hours") or 0.0)
        lines.append(
            f"• tx#{tx} {gw} ${amount:.2f} "
            f"({gateway_invoice_id}) — {age:.1f}h"
        )
    overflow = n - len(shown)
    footer = ""
    if overflow > 0:
        footer = (
            f"\n\n…and {overflow} more "
            "(see /admin/transactions?status=PENDING)."
        )
    return head + "\n".join(lines) + footer


# ---------------------------------------------------------------------
# admin DM dispatch
# ---------------------------------------------------------------------


async def notify_admins_of_stuck_pending(
    bot: Bot, rows: list[dict], threshold_hours: int
) -> int:
    """Send a stuck-pending DM to each admin. Returns successful sends.

    Per-admin fault isolation: a ``TelegramForbiddenError`` (admin
    blocked the bot) on admin A doesn't stop admin B's notification.
    A ``TelegramAPIError`` is logged with stack and skipped — we'd
    rather miss one admin than have the loop die silently and let
    the backlog accumulate again.

    Returns 0 (without sending anything) if the row list is empty
    or ``ADMIN_USER_IDS`` is unset, so callers can ``await`` this
    unconditionally.
    """
    if not rows:
        return 0
    admin_ids = get_admin_user_ids()
    if not admin_ids:
        log.warning(
            "Detected %d stuck PENDING payment(s) over %dh but "
            "ADMIN_USER_IDS is empty — nothing to notify. Set "
            "ADMIN_USER_IDS to receive these alerts.",
            len(rows),
            threshold_hours,
        )
        return 0
    text = _format_alert_body(rows, threshold_hours)
    sent = 0
    for admin_id in admin_ids:
        try:
            await bot.send_message(
                admin_id, text, disable_web_page_preview=True
            )
            sent += 1
        except TelegramForbiddenError:
            log.info(
                "Admin %d blocked the bot; skipping stuck-pending alert",
                admin_id,
            )
        except TelegramAPIError:
            log.exception(
                "Failed to send stuck-pending alert to admin %d",
                admin_id,
            )
    return sent


# ---------------------------------------------------------------------
# pass + loop
# ---------------------------------------------------------------------


async def run_pending_alert_pass(
    bot: Bot,
    *,
    threshold_hours: int,
    state: set[tuple[int, int]],
    row_limit: int,
) -> int:
    """One alert pass. Returns the number of admin DMs sent.

    Queries the DB for stuck rows, filters out the ones we've already
    alerted on at this hour-bucket (in-memory ``state`` set), DMs the
    admins about the residual, then registers the residual's keys in
    ``state`` so the next tick at the same hour-bucket is silent.

    Exposed so tests + a future ``/admin/payment_health`` "Re-fire
    alerts now" button can drive it directly without a 30-minute
    asyncio.sleep wait. Caller passes the dedupe ``state`` so the
    loop and the manual trigger share a single set.
    """
    try:
        rows = await db.list_pending_payments_over_threshold(
            threshold_hours=threshold_hours,
            limit=row_limit,
        )
    except Exception:
        log.exception(
            "list_pending_payments_over_threshold DB call failed "
            "(threshold_hours=%d)",
            threshold_hours,
        )
        return 0

    if not rows:
        return 0

    # Filter rows we've already alerted on at this hour-bucket. This
    # is the per-row dedupe — the alert says "tx#42 has been stuck
    # for 2h" once at hour 2, then "tx#42 has been stuck for 3h" at
    # hour 3, never repeating the same hour.
    fresh = [r for r in rows if _alert_key(r) not in state]
    if not fresh:
        log.info(
            "pending-alert: %d row(s) over %dh, all already alerted "
            "this hour-bucket — skipping",
            len(rows),
            threshold_hours,
        )
        return 0

    log.info(
        "pending-alert: %d row(s) over %dh; %d new since last pass — "
        "DMing admins",
        len(rows),
        threshold_hours,
        len(fresh),
    )
    sent = await notify_admins_of_stuck_pending(
        bot, fresh, threshold_hours
    )
    # Register the keys whether or not the DM succeeded — a failed
    # DM is logged + retried next interval, but we don't want to
    # spam the same row every 30 min when, say, the bot is rate-
    # limited or the only admin blocked us. The hour-bucket roll
    # naturally re-fires the alert at the next integer-hour boundary.
    for r in fresh:
        state.add(_alert_key(r))
    return sent


async def _tick_pending_alert_from_app(app) -> None:
    """Run a single ``pending_alert`` pass, deps from *app*.

    Uses a fresh dedupe ``state`` set per manual tick — that's
    intentional. An operator hitting "Tick now" usually wants to
    *see* the current alerts (the dedupe set is for the loop's
    hour-bucket suppression, which a manual tick should bypass).
    """
    from web_admin import APP_KEY_BOT  # local: avoid import cycle

    bot = app.get(APP_KEY_BOT)
    if bot is None:
        raise RuntimeError(
            "pending_alert tick-now: bot not in app state — "
            "manual ticks require a bot to DM admins."
        )
    threshold_hours = get_pending_alert_threshold_hours()
    row_limit = get_pending_alert_row_limit()
    fresh_state: set[tuple[int, int]] = set()
    await run_pending_alert_pass(
        bot,
        threshold_hours=threshold_hours,
        state=fresh_state,
        row_limit=row_limit,
    )


@register_loop(
    "pending_alert",
    cadence_seconds=_PENDING_ALERT_INTERVAL_MIN_DEFAULT * 60,
    runner=_tick_pending_alert_from_app,
)
async def _alert_loop(
    bot: Bot,
    *,
    interval_seconds: int,
    threshold_hours: int,
    row_limit: int,
) -> None:
    """Forever-running alert loop. Cancellation-safe (mirrors
    :func:`pending_expiration._expiration_loop`).

    One iteration that raises is logged and the loop keeps going — we
    don't let a transient DB blip take the alert plumbing off the air.

    Stage-15-Step-E #10b row 10: ``threshold_hours`` is the value
    captured at startup but the loop re-reads it via
    :func:`get_pending_alert_threshold_hours` on every iteration so
    a saved DB override takes effect on the next tick — no restart
    required. The kwarg is retained as the bootstrap value for the
    very first iteration (mirrors :func:`pending_expiration._expiration_loop`).
    """
    state: set[tuple[int, int]] = set()
    log.info(
        "pending-alert loop started "
        "(interval=%ds, threshold=%dh, limit=%d)",
        interval_seconds,
        threshold_hours,
        row_limit,
    )
    next_threshold = threshold_hours
    try:
        while True:
            try:
                await run_pending_alert_pass(
                    bot,
                    threshold_hours=next_threshold,
                    state=state,
                    row_limit=row_limit,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "run_pending_alert_pass raised; will retry next tick"
                )
            else:
                # Stage-15-Step-A: heartbeat for
                # ``meowassist_pending_alert_last_run_epoch``.
                from metrics import record_loop_tick

                record_loop_tick("pending_alert")
            await asyncio.sleep(interval_seconds)
            try:
                next_threshold = get_pending_alert_threshold_hours()
            except Exception:
                log.exception(
                    "get_pending_alert_threshold_hours raised; keeping "
                    "previous threshold %dh",
                    next_threshold,
                )
    except asyncio.CancelledError:
        log.info("pending-alert loop cancelled; exiting cleanly")
        raise


def start_pending_alert_task(bot: Bot) -> asyncio.Task:
    """Spawn the alert loop and return its handle.

    The caller (``main.main``) is responsible for cancelling +
    awaiting the task during shutdown so the asyncio.run() loop
    closes cleanly. Mirrors
    :func:`pending_expiration.start_pending_expiration_task`.

    Reads three env vars (with defaults):
      * ``PENDING_ALERT_INTERVAL_MIN`` (default 30)
      * ``PENDING_ALERT_THRESHOLD_HOURS`` (default 2)
      * ``PENDING_ALERT_LIMIT`` (default 500)
    """
    interval_seconds = get_pending_alert_interval_seconds()
    threshold_hours = get_pending_alert_threshold_hours()
    row_limit = get_pending_alert_row_limit()
    return asyncio.create_task(
        _alert_loop(
            bot,
            interval_seconds=interval_seconds,
            threshold_hours=threshold_hours,
            row_limit=row_limit,
        ),
        name="pending-alert-loop",
    )
