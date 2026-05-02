"""Bot health classification + emergency-stop primitives.

Stage-15-Step-F. The ``/admin/control`` admin panel surfaces a
traffic-light health tile (idle / healthy / busy / degraded /
under-attack / down) plus emergency controls (force-stop button,
master kill-switches for AI models and payment gateways) so an
operator can act on the bot without SSHing into the box.

This module is pure-function-first:

* :func:`compute_bot_status` is the deterministic classifier — same
  inputs always produce the same status. Easy to unit-test, no
  globals, no I/O.
* :func:`request_force_stop` is the only side-effecting primitive,
  and it's parameterised on the kill function so tests can pass a
  spy without actually murdering the test process.

Signals consumed by the classifier are deliberately lazy — collected
fresh each request, no caching — so the panel always reflects the
real state. The tile renders fast enough (< 1 ms) that a polled
refresh is cheap.

The five status levels (severity ascending):

* ``idle`` — alive, no in-flight chat slots, no recent IPN drops.
* ``healthy`` — alive, normal load, no alarms.
* ``busy`` — in-flight chat-slot count above
  ``BOT_HEALTH_BUSY_INFLIGHT`` (default 50). The bot is still
  serving but has bursty load; expect higher latency.
* ``degraded`` — at least one background loop has missed its
  expected heartbeat window
  (``BOT_HEALTH_LOOP_STALE_SECONDS``, default 1800 s). Most often
  a stuck reaper, an FX-source outage, or a model-discovery loop
  that swallowed an exception. Bot still serves users but ops
  should investigate.
* ``under_attack`` — IPN drop counters or login-throttle bucket
  count have crossed the alarm threshold. Almost always a flood
  of forged callbacks or a brute-force login spray. Operators
  should consider activating the master kill-switches.
* ``down`` — the dashboard's last DB read raised an exception, so
  the bot can't transactionally serve users (admin actions, wallet
  charges, payment finalisation are all on the floor). Force-stop
  + investigate the DB.

The classifier prefers severity (highest level wins). Multiple
signals are collected and surfaced in ``BotStatus.signals`` so the
panel can render the underlying detail.
"""

from __future__ import annotations

import dataclasses
import enum
import logging
import os
import signal
import time
from typing import Callable, Iterable, Mapping

log = logging.getLogger("bot.health")


class BotStatusLevel(str, enum.Enum):
    """Coarse health classification levels — see module docstring."""

    IDLE = "idle"
    HEALTHY = "healthy"
    BUSY = "busy"
    DEGRADED = "degraded"
    UNDER_ATTACK = "under_attack"
    DOWN = "down"


# Severity ordering. Used both by the Prometheus gauge
# (``meowassist_bot_status_score``) and by the template's
# traffic-light colour mapping. Keep ``IDLE`` at 0 so a freshly-booted
# bot scrapes as 0 (a Prometheus alert firing on
# ``meowassist_bot_status_score >= 4`` then catches under-attack /
# down without false-positives on the cold path).
_LEVEL_SCORE: dict[BotStatusLevel, int] = {
    BotStatusLevel.IDLE: 0,
    BotStatusLevel.HEALTHY: 1,
    BotStatusLevel.BUSY: 2,
    BotStatusLevel.DEGRADED: 3,
    BotStatusLevel.UNDER_ATTACK: 4,
    BotStatusLevel.DOWN: 5,
}


def status_score(level: BotStatusLevel) -> int:
    """Return the integer severity score for *level*.

    Exposed for ``metrics.render_metrics`` so the Prometheus body
    can include a single numeric gauge — operators wire alerts on
    ``meowassist_bot_status_score >= 4`` rather than parsing the
    label.
    """
    return _LEVEL_SCORE[level]


# Human-readable summaries for the no-signal default cases.
# Severity-prefixed signal lists (under-attack / degraded) are
# composed inline in :func:`compute_bot_status` so the operator
# sees the underlying numbers, not a generic blurb.
_DEFAULT_SUMMARY: dict[BotStatusLevel, str] = {
    BotStatusLevel.IDLE: "Bot is idle — no active load",
    BotStatusLevel.HEALTHY: "Bot is healthy",
    BotStatusLevel.BUSY: "Bot is busy",
    BotStatusLevel.DEGRADED: "Bot is degraded",
    BotStatusLevel.UNDER_ATTACK: "Bot is under attack",
    BotStatusLevel.DOWN: "Bot is down",
}


@dataclasses.dataclass(frozen=True)
class BotStatus:
    """Snapshot of the bot's classification at a point in time.

    Immutable on purpose — the ``/admin/control`` GET handler
    captures the snapshot once, renders it, and never mutates it.
    A future status change is reflected on the next page reload.
    """

    level: BotStatusLevel
    summary: str
    signals: tuple[str, ...]
    score: int

    @property
    def severity(self) -> int:
        """Alias for ``score`` — kept for template ergonomics."""
        return self.score


# ── Tunable thresholds ─────────────────────────────────────────────
#
# Defaults are chosen for a single-process deploy on a 1-vCPU VPS
# with NowPayments + TetraPay + Zarinpal IPNs. Operators with
# different traffic shape override via env vars (read fresh each
# call so tests can monkeypatch).

DEFAULT_BUSY_INFLIGHT = 50
DEFAULT_LOOP_STALE_SECONDS = 1800  # 30 minutes
DEFAULT_IPN_DROP_ATTACK_THRESHOLD = 100
DEFAULT_LOGIN_THROTTLE_ATTACK_KEYS = 25


# ── DB-backed threshold overrides ──────────────────────────────────
#
# Stage-15-Step-F follow-up. ``BOT_HEALTH_*`` were env-only
# previously, so an operator had to redeploy the bot to retune
# thresholds. The follow-up adds a DB-backed overlay
# (``system_settings`` table) that beats env when set. Resolution
# order for any threshold:
#
#   1. Module-level overrides cache (populated from the DB by
#      :func:`refresh_threshold_overrides_from_db`).
#   2. Env var (``os.getenv``).
#   3. ``DEFAULT_*`` constant.
#
# The cache is process-local. The web admin panel both writes the
# DB and refreshes this cache so the next ``compute_bot_status``
# call sees the new value without a restart. Other callers
# (``metrics.render_metrics``, ``bot_health_alert``) refresh the
# cache on each tick so the override propagates to every observer.
_THRESHOLD_OVERRIDES: dict[str, int] = {}

# Names of every key the admin panel can override. Other modules
# import this so the route handler + the template + the tests
# stay in lockstep.
THRESHOLD_KEYS: tuple[str, ...] = (
    "BOT_HEALTH_BUSY_INFLIGHT",
    "BOT_HEALTH_LOOP_STALE_SECONDS",
    "BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD",
    "BOT_HEALTH_LOGIN_THROTTLE_ATTACK_KEYS",
)

# Per-key minimum allowed value. The admin form refuses values
# below these floors at validation time; the env-parser also
# refuses them at runtime so a bad env override still falls
# through to ``DEFAULT_*`` (defence in depth).
THRESHOLD_MINIMUMS: dict[str, int] = {
    "BOT_HEALTH_BUSY_INFLIGHT": 1,
    "BOT_HEALTH_LOOP_STALE_SECONDS": 1,
    "BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD": 1,
    "BOT_HEALTH_LOGIN_THROTTLE_ATTACK_KEYS": 1,
}


def set_threshold_override(name: str, value: int) -> None:
    """Set an in-process override for *name*.

    Refuses non-positive values (a 0 / negative threshold would
    permanently trip the corresponding alarm — see the bundled bug
    fix in this PR's tests).
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(
            f"threshold value must be int, got {type(value).__name__}"
        )
    minimum = THRESHOLD_MINIMUMS.get(name, 1)
    if value < minimum:
        raise ValueError(
            f"threshold {name}={value} is below minimum {minimum}"
        )
    _THRESHOLD_OVERRIDES[name] = value


def clear_threshold_override(name: str) -> bool:
    """Remove an in-process override. Returns True if one existed."""
    return _THRESHOLD_OVERRIDES.pop(name, None) is not None


def get_threshold_overrides_snapshot() -> dict[str, int]:
    """Read-only copy of the current overrides cache.

    Used by the admin panel to render the "currently applied"
    column. Returning a copy keeps the caller from mutating the
    real cache by accident.
    """
    return dict(_THRESHOLD_OVERRIDES)


async def refresh_threshold_overrides_from_db(db) -> dict[str, int]:
    """Reload the override cache from the DB.

    Reads every ``BOT_HEALTH_*`` row from ``system_settings``,
    validates each against ``THRESHOLD_MINIMUMS``, and applies the
    valid ones via :func:`set_threshold_override`. Invalid rows
    are logged + skipped (the env / default fallback is the
    fail-safe). Returns the resulting snapshot.

    The whole call is wrapped so a transient DB error doesn't
    blank the cache: on failure the existing cache stays in place
    and the caller logs the exception. This means a DB outage
    can't accidentally revert to env defaults mid-incident.
    """
    if db is None:
        return get_threshold_overrides_snapshot()
    try:
        raw = await db.list_settings_with_prefix("BOT_HEALTH_")
    except Exception:
        log.exception(
            "bot_health: refresh_threshold_overrides_from_db failed; "
            "keeping previous overrides cache"
        )
        return get_threshold_overrides_snapshot()
    if not isinstance(raw, dict):
        log.warning(
            "bot_health: list_settings_with_prefix returned %r "
            "(not a dict); keeping previous overrides cache",
            type(raw).__name__,
        )
        return get_threshold_overrides_snapshot()
    # Keep keys we know about; ignore anything else stored under
    # the BOT_HEALTH_* prefix (forward-compat: a future PR could
    # store additional knobs without breaking this loader).
    valid_keys = set(THRESHOLD_KEYS)
    new_overrides: dict[str, int] = {}
    for key, value in raw.items():
        if key not in valid_keys:
            continue
        stripped = (value or "").strip()
        if not stripped:
            continue
        try:
            parsed = int(stripped)
        except ValueError:
            log.warning(
                "bot_health: ignoring system_settings %s=%r "
                "(not an int)",
                key, value,
            )
            continue
        minimum = THRESHOLD_MINIMUMS.get(key, 1)
        if parsed < minimum:
            log.warning(
                "bot_health: ignoring system_settings %s=%d "
                "(below minimum %d)",
                key, parsed, minimum,
            )
            continue
        new_overrides[key] = parsed
    # Atomic swap: replace the whole map at once so a partial
    # update doesn't leave the cache in a half-state.
    _THRESHOLD_OVERRIDES.clear()
    _THRESHOLD_OVERRIDES.update(new_overrides)
    return dict(_THRESHOLD_OVERRIDES)


# ── Per-loop expected cadences ──────────────────────────────────────
#
# Each background loop has a published interval (see HANDOFF.md).
# The single ``BOT_HEALTH_LOOP_STALE_SECONDS`` knob from the first
# slice over-flags long-cadence loops (``model_discovery`` ticks
# every 6h by design — 30 min stale threshold means it'd be DEGRADED
# 100% of the time) and under-flags short-cadence loops
# (``bot_health_alert`` ticks every 60 s — a missing 5-tick window
# is a real outage but the single 30-min knob would hide it). The
# fix is per-loop thresholds derived from each loop's cadence.
#
# The convention: a loop is "stale" if its last tick is older than
# ``2 × cadence + 60 s`` — one missed tick plus a one-minute safety
# margin to absorb scheduler jitter. The +60 s prevents
# ``min_amount_refresh`` (cadence 900 s) from oscillating between
# fresh / stale every minute when a tick happens slightly after its
# nominal window.
#
# Operators can override per-loop via the
# ``BOT_HEALTH_LOOP_STALE_<UPPER_NAME>_SECONDS`` env var (e.g.
# ``BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS=900``). The legacy
# single-knob ``BOT_HEALTH_LOOP_STALE_SECONDS`` is still honoured for
# *unknown* loop names (forward-compat: a future loop opt-in by
# adding to ``_LOOP_METRIC_NAMES`` doesn't need to also touch this
# module — it'll fall back to the legacy knob until a cadence entry
# is added here).
LOOP_CADENCES: dict[str, int] = {
    # NowPayments per-currency min-amount refresher — 15 min by
    # default, see ``payments._MIN_AMOUNT_REFRESH_INTERVAL_SECONDS``.
    "min_amount_refresh": 900,
    # USD→Toman FX refresher — 10 min by default, see
    # ``fx_rates._DEFAULT_INTERVAL_SECONDS``.
    "fx_refresh": 600,
    # OpenRouter model-discovery loop — 6h by default, see
    # ``model_discovery._DEFAULT_DISCOVERY_INTERVAL_SECONDS``.
    "model_discovery": 21_600,
    # OpenRouter catalog refresh — TTL-gated at 24h, see
    # ``models_catalog.CATALOG_TTL_SECONDS``. NB this is *not* a
    # timer-driven loop — the gauge ticks only on a successful
    # ``_refresh()`` call. A 48h threshold lets one TTL-cycle slip
    # without flagging stale.
    "catalog_refresh": 86_400,
    # Stuck-PENDING alert loop — 30 min by default, see
    # ``pending_alert._PENDING_ALERT_INTERVAL_MIN_DEFAULT``.
    "pending_alert": 1_800,
    # PENDING reaper — 15 min by default, see
    # ``pending_expiration._DEFAULT_EXPIRATION_INTERVAL_MIN``.
    "pending_reaper": 900,
    # Bot-health proactive alert loop — 60 s by default, see
    # ``bot_health_alert._BOT_HEALTH_ALERT_INTERVAL_SECONDS_DEFAULT``.
    "bot_health_alert": 60,
}

# Safety margin added on top of (2 × cadence) so a tick that lands
# just past its nominal window doesn't oscillate the panel between
# fresh and stale.
_STALE_THRESHOLD_MARGIN_SECONDS = 60


def _stale_threshold_seconds(loop_name: str, *, fallback: int) -> int:
    """Per-loop stale threshold in seconds.

    Resolution order:

    1. Explicit env override
       ``BOT_HEALTH_LOOP_STALE_<UPPER_NAME>_SECONDS`` if set to a
       positive integer. Bad values fall through silently to the
       next layer (mirrors ``_env_int``'s fail-safe).
    2. Cadence-derived: ``2 × LOOP_CADENCES[name] +
       _STALE_THRESHOLD_MARGIN_SECONDS``.
    3. *fallback* (the caller's legacy single-knob value) for
       unknown loop names.
    """
    explicit_key = (
        f"BOT_HEALTH_LOOP_STALE_{loop_name.upper()}_SECONDS"
    )
    raw = os.getenv(explicit_key, "").strip()
    if raw:
        try:
            value = int(raw)
        except ValueError:
            log.warning(
                "bot_health: invalid %s=%r (not an int) — "
                "falling back to cadence-derived threshold",
                explicit_key, raw,
            )
        else:
            if value > 0:
                return value
            log.warning(
                "bot_health: invalid %s=%d (non-positive) — "
                "falling back to cadence-derived threshold",
                explicit_key, value,
            )
    cadence = LOOP_CADENCES.get(loop_name)
    if cadence is not None:
        return cadence * 2 + _STALE_THRESHOLD_MARGIN_SECONDS
    return fallback


# Process-boot epoch — used to grace-period a never-ticked loop on
# a fresh deploy. Captured at module-load time so re-importing this
# module in tests doesn't shift the perceived boot time. Tests that
# need a frozen value pass ``process_start_epoch=`` explicitly to
# :func:`compute_bot_status`.
_PROCESS_START_EPOCH: float = time.time()


def get_process_start_epoch() -> float:
    """Read-only accessor for the process-boot epoch.

    Exposed so ``web_admin`` and ``metrics`` can use the same epoch
    the classifier uses (single source of truth — the panel's
    "uptime" tile and the classifier's never-ticked grace check
    must agree, otherwise a fresh bot can show a 47-second uptime
    on the panel while DEGRADED-because-loop-hasn't-ticked alarms
    fire below).
    """
    return _PROCESS_START_EPOCH


def _env_int(key: str, default: int, *, minimum: int = 1) -> int:
    """Resolve a positive int threshold for *key*.

    Resolution order (Stage-15-Step-F follow-up):

    1. In-process override (set by the admin panel via
       :func:`set_threshold_override` after writing the DB).
    2. Env var.
    3. *default*.

    Bug-fix history: prior versions only refused *negative* values
    and silently accepted ``0``. With ``BOT_HEALTH_BUSY_INFLIGHT=0``
    every chat slot tripped BUSY; with
    ``BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD=0`` the panel /
    Prometheus / alert loop permanently flagged UNDER_ATTACK on a
    healthy bot because ``ipn_drops_recent >= 0`` is always
    true. Same shape for the other two thresholds. The new
    *minimum* kwarg refuses anything below it (defaults to ``1``
    so the previous failure mode is now a default + warning); a
    caller that genuinely needs ``0`` can pass ``minimum=0``.
    """
    override = _THRESHOLD_OVERRIDES.get(key)
    if override is not None:
        if override >= minimum:
            return override
        log.warning(
            "bot_health: ignoring override %s=%d (below minimum %d) — "
            "falling through to env / default",
            key, override, minimum,
        )
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.warning(
            "bot_health: invalid %s=%r (not an int) — using default %d",
            key, raw, default,
        )
        return default
    if value < minimum:
        log.warning(
            "bot_health: invalid %s=%r (below minimum %d) — using default %d",
            key, raw, minimum, default,
        )
        return default
    return value


# ── Classifier ────────────────────────────────────────────────────


def compute_bot_status(
    *,
    inflight_count: int,
    ipn_drops_total: int,
    ipn_drops_recent: int = 0,
    loop_ticks: Mapping[str, float],
    expected_loops: Iterable[str],
    db_error: str | None,
    login_throttle_active_keys: int,
    now: float | None = None,
    process_start_epoch: float | None = None,
) -> BotStatus:
    """Compute the bot's coarse health classification.

    Pure function. Inputs:

    * ``inflight_count`` — current count from
      ``rate_limit.chat_inflight_count()``.
    * ``ipn_drops_total`` — sum of every gateway's drop-counter dict
      since process boot. Used only for the *informational*
      HEALTHY summary ("N IPN drop(s) since boot"). DO NOT use
      this for UNDER_ATTACK classification: a long-running deploy
      slowly accumulates one bad-signature row a day and would
      eventually false-fire UNDER_ATTACK after ~3 months of normal
      uptime. UNDER_ATTACK reads ``ipn_drops_recent`` instead.
    * ``ipn_drops_recent`` — drops observed in a recent rate-window
      the *caller* tracks. The :mod:`bot_health_alert` loop
      records the previous total at every tick and passes the
      delta-since-last-tick here so an actual flood (≥ threshold
      drops in one alert interval) trips UNDER_ATTACK without
      false-firing on slow-burn drops accumulated over months.
      Snapshot callers (Prometheus, dashboard) that don't track a
      window pass ``0`` and rely on the loop-DM channel for
      under-attack detection. Default 0 to keep the snapshot
      callers' call-sites unchanged.
    * ``loop_ticks`` — map of loop-name → last-success epoch
      (from ``metrics.get_loop_last_tick``). Loops not yet ticked
      are absent from the map (or set to 0.0).
    * ``expected_loops`` — the names of loops that *should* be
      ticking. The classifier only complains about these — a
      future loop opt-in arrives by name without changing this
      module. Per-loop staleness thresholds derive from
      :data:`LOOP_CADENCES` (a 6h-cadence loop ticks every 6h, so
      its threshold is 12h — a single missed tick), with explicit
      env overrides via
      ``BOT_HEALTH_LOOP_STALE_<UPPER_NAME>_SECONDS``. Loops absent
      from :data:`LOOP_CADENCES` fall back to the legacy single
      knob ``BOT_HEALTH_LOOP_STALE_SECONDS`` (default 1800 s) so a
      future loop opt-in works without a code change.
    * ``process_start_epoch`` — epoch when the bot process
      started, used to grace-period a loop that hasn't ticked yet
      on a fresh deploy. A 24h-cadence loop like
      ``catalog_refresh`` legitimately won't have ticked in the
      first hour after boot — flagging it stale immediately would
      have a freshly-restarted bot show DEGRADED until the first
      catalog fetch, which is wrong. Defaults to
      :func:`get_process_start_epoch` (the bot_health module's
      load-time epoch, exposed for callers that want the same
      reference).
    * ``db_error`` — the dashboard's last DB-read exception
      message, or ``None``. Any non-empty value escalates to
      ``DOWN`` regardless of the other signals (the dashboard
      can't render its tiles, the admin panel is half-blind,
      and the bot's transactional path is broken).
    * ``login_throttle_active_keys`` — number of distinct IPs
      currently holding a login-throttle bucket. A spike here is
      almost always a brute-force login spray.
    * ``now`` — testable current epoch. Defaults to
      ``time.time()``.

    Severity ordering (highest wins):

    1. ``DOWN`` if ``db_error`` is set.
    2. ``UNDER_ATTACK`` if recent drop counters or login-throttle
       keys cross thresholds.
    3. ``DEGRADED`` if any expected loop is stale.
    4. ``BUSY`` if in-flight chat slots exceed the busy threshold.
    5. ``HEALTHY`` if there's any active load.
    6. ``IDLE`` otherwise.
    """
    now = now if now is not None else time.time()
    boot_epoch = (
        process_start_epoch
        if process_start_epoch is not None
        else _PROCESS_START_EPOCH
    )
    busy_inflight = _env_int(
        "BOT_HEALTH_BUSY_INFLIGHT", DEFAULT_BUSY_INFLIGHT
    )
    legacy_loop_stale_s = _env_int(
        "BOT_HEALTH_LOOP_STALE_SECONDS", DEFAULT_LOOP_STALE_SECONDS
    )
    ipn_attack_t = _env_int(
        "BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD",
        DEFAULT_IPN_DROP_ATTACK_THRESHOLD,
    )
    login_attack_t = _env_int(
        "BOT_HEALTH_LOGIN_THROTTLE_ATTACK_KEYS",
        DEFAULT_LOGIN_THROTTLE_ATTACK_KEYS,
    )

    # 1. DOWN — DB unreachable. Highest severity; everything else
    #    is moot if the dashboard's read just blew up.
    if db_error:
        msg = f"DB unavailable: {db_error}"
        return BotStatus(
            level=BotStatusLevel.DOWN,
            summary=_DEFAULT_SUMMARY[BotStatusLevel.DOWN] + " — " + msg,
            signals=(msg,),
            score=_LEVEL_SCORE[BotStatusLevel.DOWN],
        )

    # 2. UNDER_ATTACK — flood signals.
    attack_signals: list[str] = []
    if ipn_drops_recent >= ipn_attack_t:
        attack_signals.append(
            f"{ipn_drops_recent} IPN deliveries dropped in the recent "
            f"window (threshold {ipn_attack_t})"
        )
    if login_throttle_active_keys >= login_attack_t:
        attack_signals.append(
            f"{login_throttle_active_keys} distinct IPs hold a "
            f"login-throttle bucket (threshold {login_attack_t})"
        )
    if attack_signals:
        return BotStatus(
            level=BotStatusLevel.UNDER_ATTACK,
            summary=_DEFAULT_SUMMARY[BotStatusLevel.UNDER_ATTACK]
            + " — " + "; ".join(attack_signals),
            signals=tuple(attack_signals),
            score=_LEVEL_SCORE[BotStatusLevel.UNDER_ATTACK],
        )

    # 3. DEGRADED — at least one expected background loop is stale.
    #
    # Per-loop thresholds (see ``_stale_threshold_seconds``) so a
    # 6h-cadence loop isn't flagged DEGRADED 30 min after boot.
    # Never-ticked loops get a grace period equal to their stale
    # threshold (one full "missed tick" window from boot) — without
    # that, ``catalog_refresh`` (24h cadence, no timer driver, only
    # ticks on a successful fetch) would always show DEGRADED on a
    # freshly-deployed bot for the first 24h.
    stale: list[str] = []
    uptime = max(0.0, now - boot_epoch)
    for loop_name in expected_loops:
        threshold = _stale_threshold_seconds(
            loop_name, fallback=legacy_loop_stale_s
        )
        last_tick = loop_ticks.get(loop_name, 0.0) or 0.0
        if last_tick == 0.0:
            # Loop hasn't ticked yet. Grace it until uptime exceeds
            # one stale-window from boot — beyond that, it's a real
            # alarm because by definition every loop should have
            # ticked at least once within ``threshold`` seconds.
            if uptime > threshold:
                stale.append(
                    f"{loop_name} loop has not ticked in "
                    f"{int(uptime)}s since process start "
                    f"(threshold {threshold}s)"
                )
            continue
        delta = now - last_tick
        if delta > threshold:
            stale.append(
                f"{loop_name} loop last ticked {int(delta)}s ago "
                f"(threshold {threshold}s)"
            )
    if stale:
        # Trim long lists for the inline summary; the full list
        # is in ``signals`` for the panel template to render.
        head = stale[:3]
        tail = "; …" if len(stale) > 3 else ""
        return BotStatus(
            level=BotStatusLevel.DEGRADED,
            summary=_DEFAULT_SUMMARY[BotStatusLevel.DEGRADED]
            + " — " + "; ".join(head) + tail,
            signals=tuple(stale),
            score=_LEVEL_SCORE[BotStatusLevel.DEGRADED],
        )

    # 4. BUSY — high in-flight chat load.
    if inflight_count >= busy_inflight:
        msg = (
            f"{inflight_count} chat slots in flight "
            f"(threshold {busy_inflight})"
        )
        return BotStatus(
            level=BotStatusLevel.BUSY,
            summary=_DEFAULT_SUMMARY[BotStatusLevel.BUSY] + " — " + msg,
            signals=(msg,),
            score=_LEVEL_SCORE[BotStatusLevel.BUSY],
        )

    # 5. HEALTHY — there's some load but everything's fine.
    if inflight_count > 0 or ipn_drops_total > 0:
        bits: list[str] = []
        if inflight_count > 0:
            bits.append(f"{inflight_count} chat slot(s) in flight")
        if ipn_drops_total > 0:
            bits.append(f"{ipn_drops_total} IPN drop(s) since boot")
        return BotStatus(
            level=BotStatusLevel.HEALTHY,
            summary=_DEFAULT_SUMMARY[BotStatusLevel.HEALTHY],
            signals=tuple(bits),
            score=_LEVEL_SCORE[BotStatusLevel.HEALTHY],
        )

    # 6. IDLE — quiet bot.
    return BotStatus(
        level=BotStatusLevel.IDLE,
        summary=_DEFAULT_SUMMARY[BotStatusLevel.IDLE],
        signals=(),
        score=_LEVEL_SCORE[BotStatusLevel.IDLE],
    )


# ── Force-stop primitive ───────────────────────────────────────────


# Type alias for the kill function. ``os.kill`` is the production
# choice; tests inject a spy that records the call without
# delivering the signal.
KillFn = Callable[[int, int], None]


def request_force_stop(
    *,
    signal_number: int = signal.SIGTERM,
    kill_fn: KillFn | None = None,
    pid: int | None = None,
) -> None:
    """Signal the running bot process to terminate.

    Default is ``SIGTERM`` so ``main()``'s asyncio loop unwinds
    cleanly: cancel background tasks, close the DB pool, await
    the bot session close. The operator can flip to ``SIGKILL``
    by passing it explicitly when the process is so wedged that
    SIGTERM is ignored — but SIGKILL skips the unwind, which can
    leak DB connections / Redis state and isn't the first-line
    response.

    *kill_fn* and *pid* are injected only by tests. In production
    they default to ``os.kill`` and the current PID.
    """
    target_pid = pid if pid is not None else os.getpid()
    log.warning(
        "bot_health: force-stop requested — signalling pid=%d signal=%d",
        target_pid, signal_number,
    )
    fn = kill_fn if kill_fn is not None else os.kill
    fn(target_pid, signal_number)


__all__ = (
    "BotStatus",
    "BotStatusLevel",
    "DEFAULT_BUSY_INFLIGHT",
    "DEFAULT_IPN_DROP_ATTACK_THRESHOLD",
    "DEFAULT_LOGIN_THROTTLE_ATTACK_KEYS",
    "DEFAULT_LOOP_STALE_SECONDS",
    "compute_bot_status",
    "request_force_stop",
    "status_score",
)
