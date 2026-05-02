"""Background loop that DMs admins when the bot's health crosses a
threshold.

Stage-15-Step-F follow-up #3. Stage-15-Step-F shipped the
:mod:`bot_health` classifier and the ``/admin/control`` panel, but
an operator who isn't *actively* looking at the panel has no way to
know the bot just went DEGRADED / UNDER_ATTACK / DOWN. The hub of
the user's request was *"every thing i need to have in my hands for
times that is bot is crashing or not responding or under attack"* —
and that includes a Telegram DM the moment the panel would have
turned red.

The contract:

* Wakes every ``BOT_HEALTH_ALERT_INTERVAL_SECONDS`` (default 60).
* On each tick, runs the same classification the panel + Prometheus
  use, but with a rate-window-bounded ``ipn_drops_recent`` derived
  from the previous tick's drop total. Single source of truth for
  the level — the alert loop, the panel, and the gauge agree.
* Fires an admin DM when the level escalates to a "bad" state
  (DEGRADED, UNDER_ATTACK, DOWN) and didn't already alert at that
  level, or when it *re-escalates* further (e.g. DEGRADED → DOWN
  re-fires; DOWN → DEGRADED is a recovery).
* Fires a *recovery* DM when the level returns to HEALTHY/IDLE
  after at least one bad-state alert was sent, so the operator
  knows whatever-it-was cleared.
* Per-admin fault isolation, mirroring
  :func:`pending_alert.notify_admins_of_stuck_pending`: a
  ``TelegramForbiddenError`` (admin blocked the bot) on admin A is
  logged INFO and skipped; ``TelegramAPIError`` is logged and
  skipped; the loop never crashes.
* Bootstrap: ``state`` lives in process memory, so a restart can
  re-fire one alert per still-bad level. That's intentional — an
  operator who deployed a fix expects the alert to re-fire if the
  fix didn't actually clear the condition.

Why a separate module rather than tacking onto :mod:`pending_alert`:
that loop's signal is "has any payment row been pending for >2h";
this loop's signal is "what's the bot's coarse status right now".
Different cadence, different threshold, different failure semantics
(pending_alert's failure is no DM about a stuck row; this loop's
failure is no DM about a current incident). Keeping them split
makes it obvious which knob does which thing.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

from admin import get_admin_user_ids
from bot_health import (
    BotStatus,
    BotStatusLevel,
    compute_bot_status,
    register_loop,
    update_loop_cadence,
)


log = logging.getLogger("bot.bot_health_alert")


# ---------------------------------------------------------------------
# env var parsing
# ---------------------------------------------------------------------

_BOT_HEALTH_ALERT_INTERVAL_SECONDS_DEFAULT = 60
# Anything in or above this set is "bad enough to DM the operator".
# IDLE/HEALTHY/BUSY are not page-worthy — BUSY is by definition the
# bot doing real work — so they are deliberately excluded.
_BAD_LEVELS: frozenset[BotStatusLevel] = frozenset(
    (
        BotStatusLevel.DEGRADED,
        BotStatusLevel.UNDER_ATTACK,
        BotStatusLevel.DOWN,
    )
)


# Stage-15-Step-E #10b row 21: DB-backed override layer for
# ``BOT_HEALTH_ALERT_INTERVAL_SECONDS``. Same overlay shape COST_MARKUP
# / MIN_TOPUP_USD / REQUIRED_CHANNEL / REFERRAL_BONUS_* already use:
#
# 1. ``_INTERVAL_OVERRIDE`` — process-local cache, populated from
#    ``system_settings.BOT_HEALTH_ALERT_INTERVAL_SECONDS`` via
#    :func:`refresh_alert_interval_override_from_db` at boot and on
#    every ``/admin/control`` render. The web admin form writes this
#    row so an operator can re-tune the alert cadence without a
#    redeploy.
# 2. ``BOT_HEALTH_ALERT_INTERVAL_SECONDS`` env var — same shape as
#    before; remains the fallback for env-only deploys.
# 3. :data:`_BOT_HEALTH_ALERT_INTERVAL_SECONDS_DEFAULT` (``60``) —
#    compile-time fallback.
#
# Live pickup: :func:`_alert_loop` calls
# :func:`get_bot_health_alert_interval_seconds` *each* iteration, so
# a saved override takes effect on the next tick — no restart required.
ALERT_INTERVAL_SETTING_KEY: str = "BOT_HEALTH_ALERT_INTERVAL_SECONDS"
INTERVAL_MINIMUM: int = 1
# Cap on the override slot only (env / default callers retain their
# pre-existing freedom). A 24h ceiling prevents an admin-form
# fat-finger like ``86400000`` (intended ``60``) that would silently
# stop alerting for a month — yet still allows "alert once a day"
# semantics for low-volume deployments where an hourly tick would be
# noise.
INTERVAL_OVERRIDE_MAXIMUM: int = 86_400
_INTERVAL_OVERRIDE: int | None = None


def _coerce_alert_interval(value: object) -> int | None:
    """Validate an interval candidate for the override slot.

    Returns the canonical ``int`` on success, or ``None`` if the value
    is unusable (non-integer / non-positive / above the ceiling /
    boolean). Never raises — the caller (the web admin form, the DB
    warm-up, the ``set_*`` helper) decides how to surface a rejection.

    Strings are stripped + parsed so a row written via
    :meth:`Database.upsert_setting` (which always stores ``str``)
    round-trips cleanly.
    """
    # bool is a subclass of int — refuse explicitly so ``True``
    # doesn't sneak through as ``1`` (nonsense interval).
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
    if candidate < INTERVAL_MINIMUM:
        return None
    if candidate > INTERVAL_OVERRIDE_MAXIMUM:
        return None
    return candidate


def set_alert_interval_override(value: int) -> None:
    """Apply an in-process override for the alert interval.

    Refuses non-int / out-of-range / boolean inputs with
    :class:`ValueError`. The web admin form runs the same checks
    before persisting; this guard is defence-in-depth so a buggy
    direct caller can't poison the cache.
    """
    coerced = _coerce_alert_interval(value)
    if coerced is None:
        raise ValueError(
            f"BOT_HEALTH_ALERT_INTERVAL_SECONDS override {value!r} must "
            f"be an int in [{INTERVAL_MINIMUM}, {INTERVAL_OVERRIDE_MAXIMUM}]"
        )
    global _INTERVAL_OVERRIDE
    _INTERVAL_OVERRIDE = coerced


def clear_alert_interval_override() -> bool:
    """Drop the in-process override. Returns True if one was active."""
    global _INTERVAL_OVERRIDE
    had = _INTERVAL_OVERRIDE is not None
    _INTERVAL_OVERRIDE = None
    return had


def get_alert_interval_override() -> int | None:
    """Return the current in-process override (or ``None``)."""
    return _INTERVAL_OVERRIDE


async def refresh_alert_interval_override_from_db(db) -> int | None:
    """Reload the override from the ``system_settings`` overlay.

    Mirrors :func:`force_join.refresh_required_channel_override_from_db`
    and :func:`pricing.refresh_markup_override_from_db`: a transient
    DB error keeps the previous cache in place so a pool blip can't
    accidentally revert to env / default mid-incident. A malformed
    stored value (non-int / out-of-range) clears the override
    (so a bad write that somehow bypassed the validators doesn't
    permanently poison the cadence).
    """
    global _INTERVAL_OVERRIDE
    if db is None:
        return _INTERVAL_OVERRIDE
    try:
        raw = await db.get_setting(ALERT_INTERVAL_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_alert_interval_override_from_db: get_setting "
            "failed; keeping previous cache value=%r",
            _INTERVAL_OVERRIDE,
        )
        return _INTERVAL_OVERRIDE
    if raw is None:
        _INTERVAL_OVERRIDE = None
        return None
    coerced = _coerce_alert_interval(raw)
    if coerced is None:
        log.warning(
            "refresh_alert_interval_override_from_db: rejected stored "
            "value %r; clearing override",
            raw,
        )
        _INTERVAL_OVERRIDE = None
        return None
    _INTERVAL_OVERRIDE = coerced
    return coerced


def reset_alert_interval_override_for_tests() -> None:
    """Test-only: drop the override cache. Mirrors
    :func:`reset_latest_state_for_tests` so a test that doesn't touch
    the override directly still starts from a clean slate."""
    global _INTERVAL_OVERRIDE
    _INTERVAL_OVERRIDE = None


def _read_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    """Parse a small integer env var with a clamping floor.

    Mirrors the helpers in :mod:`pending_alert` and
    :mod:`pending_expiration` (intentionally duplicated rather than
    imported so a future refactor of one module doesn't accidentally
    change the parsing semantics of the other). A deploy-time typo
    logs loudly and falls back to the default rather than crashing
    the boot sequence.
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


def get_bot_health_alert_interval_seconds() -> int:
    """Resolve the alert-loop cadence with DB → env → default precedence.

    Resolution order (Stage-15-Step-E #10b row 21):

    1. In-process DB override populated from ``system_settings`` by
       :func:`refresh_alert_interval_override_from_db`. The web admin
       ``/admin/control`` editor form writes the row + refreshes the
       cache after every save.
    2. ``BOT_HEALTH_ALERT_INTERVAL_SECONDS`` env var.
    3. :data:`_BOT_HEALTH_ALERT_INTERVAL_SECONDS_DEFAULT` (``60``).

    The loop calls this on *every* iteration, so a saved override
    takes effect on the next tick — no restart required.

    Exposed so the next AI / the panel can echo the configured
    interval back to the operator without duplicating the env-parse
    logic.
    """
    if _INTERVAL_OVERRIDE is not None:
        return _INTERVAL_OVERRIDE
    return _read_int_env(
        "BOT_HEALTH_ALERT_INTERVAL_SECONDS",
        _BOT_HEALTH_ALERT_INTERVAL_SECONDS_DEFAULT,
    )


def get_bot_health_alert_interval_source() -> str:
    """Return ``db`` / ``env`` / ``default`` for the resolved interval.

    Mirrors :func:`pricing.get_markup_source` and
    :func:`force_join.get_required_channel_source` — the
    ``/admin/control`` panel uses this badge to show operators where
    the live cadence is coming from.

    ``"env"`` is returned when there's no DB row and the env var is
    a *valid* parseable integer (negative or non-numeric env values
    fall through to ``"default"``). The shape mirrors how
    :func:`get_bot_health_alert_interval_seconds` resolves so the
    badge never disagrees with the effective number.
    """
    if _INTERVAL_OVERRIDE is not None:
        return "db"
    raw = os.getenv("BOT_HEALTH_ALERT_INTERVAL_SECONDS", "").strip()
    if not raw:
        return "default"
    try:
        int(raw)
    except ValueError:
        return "default"
    return "env"


# ---------------------------------------------------------------------
# loop state
# ---------------------------------------------------------------------


@dataclass
class AlertLoopState:
    """Mutable bookkeeping the loop carries between ticks.

    Frozen-on-the-outside, mutable-on-the-inside is intentional —
    callers (the loop, tests, a future "re-fire alerts now" admin
    button) should pass a single shared instance, and individual
    fields update as the loop progresses.
    """

    # Total IPN drops observed at the *previous* tick across every
    # gateway. Initialised to 0 and primed on the first tick so the
    # first tick's "delta" is the absolute value of the
    # since-boot count — which is fine because a process that already
    # has 100+ drops by the time the alert loop starts deserves an
    # alert anyway.
    previous_ipn_drops_total: int = 0

    # The last level we DMed about, or ``None`` if nothing has fired
    # yet. Drives the "alert on escalation, recover on de-escalation"
    # contract.
    last_dispatched_level: BotStatusLevel | None = None

    # Most recent observed level, updated every tick whether or not a
    # DM fires. Surfaced to the ``/admin/control`` panel so it can
    # show the loop's view of the level (in particular, the
    # rate-window UNDER_ATTACK signal that the panel itself can't
    # observe).
    last_observed_level: BotStatusLevel = BotStatusLevel.IDLE
    last_observed_status: BotStatus | None = None
    last_observed_recent_drops: int = 0
    last_observed_at: float = 0.0

    # Admin DM dedupe — already-sent (level, anchor_minute) keys.
    # ``anchor_minute = floor(now / 60 / re_fire_minutes)`` so a
    # still-bad state re-fires once every re_fire_minutes minutes
    # rather than every tick.
    sent_anchors: set[tuple[BotStatusLevel, int]] = field(default_factory=set)


# Module-level singleton so the panel can read the loop's most-recent
# observation without a back-channel. ``None`` until the loop has
# ticked once. Tests reset by setting ``LATEST_STATE = None``.
_LATEST_STATE: AlertLoopState | None = None


def latest_observed_status() -> BotStatus | None:
    """Read-only accessor for the panel.

    Returns ``None`` if the alert loop has not yet ticked. The panel
    falls back to its own snapshot classification in that case.
    """
    if _LATEST_STATE is None:
        return None
    return _LATEST_STATE.last_observed_status


def latest_observed_recent_drops() -> int:
    """Read-only accessor for the panel: the loop's most-recent
    rate-windowed drop count. The panel passes this to
    :func:`bot_health.compute_bot_status` so the panel + the loop
    + the gauge all classify identically.

    Returns ``0`` if the loop has not yet ticked.
    """
    if _LATEST_STATE is None:
        return 0
    return _LATEST_STATE.last_observed_recent_drops


def reset_latest_state_for_tests() -> None:
    """Test-only: clear the module-level latest state so each test
    starts from a known empty position."""
    global _LATEST_STATE
    _LATEST_STATE = None


# ---------------------------------------------------------------------
# alert formatting
# ---------------------------------------------------------------------


def _format_alert_body(
    status: BotStatus, *, recovered_from: BotStatusLevel | None = None
) -> str:
    """Render the admin DM body. Plain text (no Markdown) so we don't
    have to escape gateway names / signal text that may contain
    ``_`` / ``*``.

    If ``recovered_from`` is given, this is a recovery DM ("status is
    back to HEALTHY after being UNDER_ATTACK"); otherwise it's an
    incident DM ("bot is now UNDER_ATTACK").
    """
    if recovered_from is not None:
        head = (
            f"✅ Bot health recovered: {status.level.value} "
            f"(was {recovered_from.value}).\n"
        )
    else:
        head = f"⚠️ Bot health alert: {status.level.value}.\n"
    body = status.summary
    if status.signals and status.signals[0] != body:
        body += "\n\nSignals:\n" + "\n".join(
            f"• {s}" for s in status.signals[:5]
        )
        if len(status.signals) > 5:
            body += f"\n…and {len(status.signals) - 5} more."
    body += "\n\nDetails: /admin/control"
    return head + "\n" + body


# ---------------------------------------------------------------------
# admin DM dispatch
# ---------------------------------------------------------------------


async def _record_alert_audit(
    status: BotStatus,
    *,
    sent_count: int,
    admin_count: int,
    recovered_from: BotStatusLevel | None = None,
) -> None:
    """Append one row to ``admin_audit_log`` describing an alert DM.

    Best-effort: every exception is swallowed and logged. The alert
    loop's responsibility is to DM the operator about incidents —
    if the audit-log write fails we still want the DM to count.

    Action slug:

    * ``bot_health_alert`` — bad-level transition (DEGRADED /
      UNDER_ATTACK / DOWN entry).
    * ``bot_health_recovery`` — recovery transition back to
      HEALTHY/IDLE.

    The ``meta`` jsonb column captures everything an operator
    reviewing the incident timeline would want: the level / score,
    the recovered-from level (recovery only), the underlying
    ``signals`` tuple from the classifier (so the audit row is
    self-contained — the operator doesn't need to cross-reference
    Prometheus to know *why* the alert fired), and the per-DM
    delivery counts (so a partially-failed fan-out is visible —
    "0 of 2 admins received this DM" is a much louder signal than
    just "DM sent" when an admin blocked the bot during an
    incident).

    ``actor`` is fixed to ``"bot_health_alert"`` (the loop itself,
    not a human admin) and ``ip`` is ``None`` because no inbound
    request triggered this — it's a polled background event. The
    audit-log filter UI on ``/admin/audit`` already supports
    filtering by ``actor`` so an operator can pull just the
    alert-loop rows.
    """
    is_recovery = recovered_from is not None
    action = "bot_health_recovery" if is_recovery else "bot_health_alert"
    outcome = "ok" if sent_count > 0 else "no_admins_reachable"
    if admin_count == 0:
        outcome = "no_admins_configured"
    meta = {
        "level": status.level.value,
        "score": status.score,
        "summary": status.summary,
        "signals": list(status.signals),
        "sent_count": sent_count,
        "admin_count": admin_count,
    }
    if recovered_from is not None:
        meta["recovered_from"] = recovered_from.value
    try:
        # Lazy import so this module imports cleanly in tests that
        # don't have asyncpg / a configured DSN.
        from database import db
        await db.record_admin_audit(
            actor="bot_health_alert",
            action=action,
            target=status.level.value,
            ip=None,
            outcome=outcome,
            meta=meta,
        )
    except Exception:
        log.exception(
            "bot_health_alert: audit log write failed for %s "
            "(action=%s, outcome=%s)",
            status.level.value, action, outcome,
        )


async def notify_admins_of_health_change(
    bot: Bot,
    status: BotStatus,
    *,
    recovered_from: BotStatusLevel | None = None,
) -> int:
    """Send a health-change DM to each admin. Returns successful sends.

    Per-admin fault isolation: a ``TelegramForbiddenError`` (admin
    blocked the bot) on admin A doesn't stop admin B's notification.
    A ``TelegramAPIError`` is logged with stack and skipped — we'd
    rather miss one admin than have the loop die silently and let
    the bot stay quiet during an incident.

    Returns 0 (without sending anything) if ``ADMIN_USER_IDS`` is
    unset, so callers can ``await`` this unconditionally.
    """
    admin_ids = get_admin_user_ids()
    if not admin_ids:
        log.warning(
            "Bot health crossed into %s but ADMIN_USER_IDS is empty "
            "— nothing to notify. Set ADMIN_USER_IDS to receive "
            "these alerts.",
            status.level.value,
        )
        # Still record the no-admins-configured event in the audit
        # log so the operator reviewing the timeline can see *that*
        # the alert would have fired even though no DM went out.
        # Otherwise an unconfigured deploy that's actually under
        # attack would be silent both via DM and via audit-log,
        # which defeats the whole point of having an audit trail.
        await _record_alert_audit(
            status,
            sent_count=0,
            admin_count=0,
            recovered_from=recovered_from,
        )
        return 0
    text = _format_alert_body(status, recovered_from=recovered_from)
    sent = 0
    for admin_id in admin_ids:
        try:
            await bot.send_message(
                admin_id, text, disable_web_page_preview=True
            )
            sent += 1
        except TelegramForbiddenError:
            log.info(
                "Admin %d blocked the bot; skipping bot-health alert",
                admin_id,
            )
        except TelegramAPIError:
            log.exception(
                "Failed to send bot-health alert to admin %d",
                admin_id,
            )
    # One audit row per *event* (not per admin) — the audit table
    # answers "what fired and when", and the meta blob captures the
    # delivery fan-out so a partial failure is visible. Recording
    # one row per admin would make the audit log noisy without
    # adding signal: the per-admin DM result is captured in
    # ``meta.sent_count`` vs ``meta.admin_count``.
    await _record_alert_audit(
        status,
        sent_count=sent,
        admin_count=len(admin_ids),
        recovered_from=recovered_from,
    )
    return sent


# ---------------------------------------------------------------------
# pass + loop
# ---------------------------------------------------------------------


def _read_signals() -> tuple[int, int, dict[str, float], int, str | None]:
    """Snapshot the signals the classifier needs.

    Returns a tuple of:
      * ``inflight_count``
      * ``ipn_drops_total`` (sum across all gateways)
      * ``loop_ticks`` (map of loop-name → epoch)
      * ``login_throttle_active_keys`` — *not* observable from this
        loop because the throttle cache lives on the aiohttp app, not
        the bot process. Hard-wired to 0 here; the panel observes
        this directly. The panel's UNDER_ATTACK signal still fires on
        login-throttle saturation; this loop's UNDER_ATTACK signal
        only fires on IPN drop floods.
      * ``db_error`` — same caveat: the loop has no DB pool of its
        own; the dashboard's DB-availability signal is observed by
        the panel only. ``None`` here.

    Each accessor is wrapped in try/except so a failed import or a
    transient module-state hiccup doesn't take the alert loop off
    the air.
    """
    # Lazy imports so this module can be imported in tests that don't
    # have the full bot surface area available (matches the same
    # pattern in metrics.render_metrics + admin.py).
    inflight = 0
    try:
        from rate_limit import chat_inflight_count
        inflight = chat_inflight_count()
    except Exception:
        log.exception("bot_health_alert: chat_inflight_count failed")

    drops_total = 0
    for accessor_name in (
        ("payments", "get_ipn_drop_counters"),
        ("tetrapay", "get_tetrapay_drop_counters"),
        ("zarinpal", "get_zarinpal_drop_counters"),
    ):
        try:
            mod = __import__(accessor_name[0])
            fn = getattr(mod, accessor_name[1])
            drops_total += sum(fn().values())
        except Exception:
            log.exception(
                "bot_health_alert: %s.%s failed",
                *accessor_name,
            )

    loop_ticks: dict[str, float] = {}
    try:
        from metrics import _LOOP_LAST_TICK, _LOOP_METRIC_NAMES

        loop_ticks = {
            name: _LOOP_LAST_TICK.get(name, 0.0) for name in _LOOP_METRIC_NAMES
        }
    except Exception:
        log.exception("bot_health_alert: read of loop ticks failed")

    return inflight, drops_total, loop_ticks, 0, None


async def run_bot_health_alert_pass(
    bot: Bot,
    *,
    state: AlertLoopState,
) -> int:
    """One alert pass. Returns the number of admin DMs sent.

    Snapshots signals, classifies, decides whether a DM is in order,
    sends if so, then updates ``state`` and the module-level latest
    cache. The classifier is the single source of truth for the
    level — the loop itself just decides *whether* to DM about it.
    """
    global _LATEST_STATE

    # 1. Snapshot signals.
    inflight, drops_total, loop_ticks, login_keys, db_error = (
        _read_signals()
    )

    # 2. Compute the rate-windowed drop count (delta from prior tick).
    #    On the first tick state.previous == 0, so the delta is the
    #    absolute since-boot count — fine, see AlertLoopState docstring.
    drops_recent = max(0, drops_total - state.previous_ipn_drops_total)

    # 3. Classify.
    try:
        from metrics import _LOOP_METRIC_NAMES

        expected = _LOOP_METRIC_NAMES
    except Exception:
        log.exception(
            "bot_health_alert: failed to import _LOOP_METRIC_NAMES"
        )
        expected = ()

    status = compute_bot_status(
        inflight_count=inflight,
        ipn_drops_total=drops_total,
        ipn_drops_recent=drops_recent,
        loop_ticks=loop_ticks,
        expected_loops=expected,
        db_error=db_error,
        login_throttle_active_keys=login_keys,
    )

    # 4. Update the module-level latest cache so the panel can read
    #    the loop's view (in particular, the rate-window classification
    #    that the panel's snapshot can't see).
    state.last_observed_level = status.level
    state.last_observed_status = status
    state.last_observed_recent_drops = drops_recent
    state.last_observed_at = time.time()
    state.previous_ipn_drops_total = drops_total
    _LATEST_STATE = state

    # 5. Decide what (if anything) to DM about.
    sent = 0
    if status.level in _BAD_LEVELS:
        # Re-fire the same level once an hour — anchor key is
        # ``(level, floor(now/3600))``.
        anchor = (status.level, int(time.time() // 3600))
        is_new_level = state.last_dispatched_level != status.level
        is_anchor_fresh = anchor not in state.sent_anchors
        if is_new_level or is_anchor_fresh:
            log.warning(
                "bot_health_alert: status=%s — DMing admins (new_level=%s)",
                status.level.value, is_new_level,
            )
            sent = await notify_admins_of_health_change(bot, status)
            state.sent_anchors.add(anchor)
            state.last_dispatched_level = status.level
        else:
            log.info(
                "bot_health_alert: status=%s, anchor already dispatched "
                "this hour — skipping DM",
                status.level.value,
            )
    else:
        # Not in a bad state. If the previous dispatched level *was*
        # bad, this is a recovery — DM once and clear the dispatched
        # level so the next bad transition re-fires immediately rather
        # than waiting for the hour-anchor to roll.
        prev = state.last_dispatched_level
        if prev is not None and prev in _BAD_LEVELS:
            log.info(
                "bot_health_alert: recovered from %s → %s — DMing admins",
                prev.value, status.level.value,
            )
            sent = await notify_admins_of_health_change(
                bot, status, recovered_from=prev
            )
            state.last_dispatched_level = None
            state.sent_anchors.clear()
    return sent


def _sync_registered_cadence(cadence_seconds: int) -> None:
    """Push *cadence_seconds* into ``bot_health.LOOP_CADENCES`` so the
    ``/admin/control`` panel's stale threshold tracks the loop's
    actual sleep duration.

    Best-effort: a refusal from :func:`update_loop_cadence` (e.g. the
    test harness cleared the registry between iterations) is logged
    and swallowed; the loop's actual cadence already matches the
    resolved value, so the worst case is the panel showing a stale
    threshold for one extra tick.
    """
    try:
        update_loop_cadence("bot_health_alert", cadence_seconds)
    except Exception:
        log.exception(
            "bot_health_alert: update_loop_cadence(%d) failed; "
            "panel stale threshold may show the previous cadence",
            cadence_seconds,
        )


async def _tick_bot_health_alert_from_app(app) -> None:
    """Run a single ``bot_health_alert`` pass, deps from *app*.

    Uses a fresh ``AlertLoopState`` per manual tick — that way the
    operator immediately sees a DM if the bot is currently in an
    alert state, regardless of the loop's running dedupe state.
    """
    from web_admin import APP_KEY_BOT  # local: avoid import cycle

    bot = app.get(APP_KEY_BOT)
    if bot is None:
        raise RuntimeError(
            "bot_health_alert tick-now: bot not in app state — "
            "manual ticks require a bot to DM admins."
        )
    fresh_state = AlertLoopState()
    await run_bot_health_alert_pass(bot, state=fresh_state)


@register_loop(
    "bot_health_alert",
    cadence_seconds=_BOT_HEALTH_ALERT_INTERVAL_SECONDS_DEFAULT,
    runner=_tick_bot_health_alert_from_app,
)
async def _alert_loop(
    bot: Bot,
    *,
    interval_seconds: int,
) -> None:
    """Forever-running alert loop. Cancellation-safe (mirrors
    :func:`pending_alert._alert_loop`).

    One iteration that raises is logged and the loop keeps going — we
    don't let a transient blip take the alert plumbing off the air.

    Stage-15-Step-E #10b row 21: each iteration re-reads the cadence
    via :func:`get_bot_health_alert_interval_seconds` so an operator
    saving a new value at ``/admin/control`` takes effect on the next
    tick without a restart. The ``interval_seconds`` argument is kept
    only as the *initial* sleep duration so existing callers /
    integration tests that pass an explicit cadence still see their
    first tick at the requested time. The first iteration always
    runs immediately (matches the legacy "tick on start" behaviour).
    """
    state = AlertLoopState()
    log.info(
        "bot-health-alert loop started (interval=%ds, "
        "live-pickup from system_settings on every tick)",
        interval_seconds,
    )
    next_sleep = interval_seconds
    # Sync the registered cadence with the resolved value so the
    # ``/admin/control`` panel's "stale threshold" matches the loop's
    # actual sleep duration. Without this, an operator who set the
    # env var (or saved a DB override) to anything other than the
    # compile-time default of 60s saw the loop continuously marked
    # "overdue" because the panel computed the threshold from the
    # registered cadence (60). See ``bot_health.update_loop_cadence``
    # for the bundled bug fix rationale.
    _sync_registered_cadence(interval_seconds)
    try:
        while True:
            try:
                await run_bot_health_alert_pass(bot, state=state)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "run_bot_health_alert_pass raised; will retry next tick"
                )
            else:
                # Heartbeat for ``meowassist_bot_health_alert_last_run_epoch``.
                from metrics import record_loop_tick

                record_loop_tick("bot_health_alert")
            await asyncio.sleep(next_sleep)
            # Re-read the interval AFTER the sleep so the next tick
            # picks up any operator change saved during the previous
            # window. Falls back to ``interval_seconds`` only on the
            # implausible case where the resolver itself raises.
            try:
                next_sleep = get_bot_health_alert_interval_seconds()
            except Exception:
                log.exception(
                    "get_bot_health_alert_interval_seconds raised; "
                    "keeping previous cadence %ds",
                    next_sleep,
                )
            else:
                # Re-sync the registered cadence too so a saved
                # override picks up on the panel as well.
                _sync_registered_cadence(next_sleep)
    except asyncio.CancelledError:
        log.info("bot-health-alert loop cancelled; exiting cleanly")
        raise


def start_bot_health_alert_task(bot: Bot) -> asyncio.Task:
    """Spawn the alert loop and return its handle.

    The caller (``main.main``) is responsible for cancelling +
    awaiting the task during shutdown so the asyncio.run() loop
    closes cleanly. Mirrors
    :func:`pending_alert.start_pending_alert_task`.

    Stage-15-Step-E #10b row 21: the resolved cadence is also pushed
    into ``bot_health.LOOP_CADENCES`` so the ``/admin/control`` panel
    shows the correct "stale threshold" for the very first render
    after boot — the loop's first iteration would otherwise need to
    happen before :func:`_sync_registered_cadence` is called.
    """
    interval_seconds = get_bot_health_alert_interval_seconds()
    _sync_registered_cadence(interval_seconds)
    return asyncio.create_task(
        _alert_loop(bot, interval_seconds=interval_seconds),
        name="bot-health-alert-loop",
    )
