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


def _env_int(key: str, default: int) -> int:
    """Read a non-negative int from env, falling back to *default*.

    Empty / unset / malformed → *default*. Negative → *default*
    (the thresholds are all "at-or-above" gates; a negative would
    silently turn the gate into "always trip" which is the wrong
    fail-safe).
    """
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
    if value < 0:
        log.warning(
            "bot_health: invalid %s=%r (negative) — using default %d",
            key, raw, default,
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
      module.
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
    busy_inflight = _env_int(
        "BOT_HEALTH_BUSY_INFLIGHT", DEFAULT_BUSY_INFLIGHT
    )
    loop_stale_s = _env_int(
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
    stale: list[str] = []
    for loop_name in expected_loops:
        last_tick = loop_ticks.get(loop_name, 0.0) or 0.0
        if last_tick == 0.0:
            stale.append(
                f"{loop_name} loop has not ticked since process start"
            )
            continue
        delta = now - last_tick
        if delta > loop_stale_s:
            stale.append(
                f"{loop_name} loop last ticked {int(delta)}s ago "
                f"(threshold {loop_stale_s}s)"
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
