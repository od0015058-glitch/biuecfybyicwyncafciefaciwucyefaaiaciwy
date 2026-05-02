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
        # Bundled bug fix: previously ``(value or "").strip()`` would
        # AttributeError on a non-string-non-None row (e.g. an int
        # written by a future ``upsert_setting`` overload, or a
        # historical row left over from a different schema). The
        # whole refresh would then bubble up to the caller and
        # leave the override cache half-loaded — every key after
        # the bad row would silently fall through to env / default.
        # Coerce to ``str`` defensively and skip rows that won't
        # coerce so a single garbage row can't poison the rest of
        # the load.
        stripped = _coerce_setting_to_str(key, value).strip()
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


def _coerce_setting_to_str(key: str, value: object) -> str:
    """Coerce a ``system_settings`` row value to a str defensively.

    Historical rows / future schema changes / stub DBs in tests can
    legitimately return ``None``, ``int``, ``Decimal`` or other
    non-str types from ``list_settings_with_prefix``. The downstream
    parsers all expect a ``str`` they can ``.strip()`` and ``int(...)``,
    so a single non-str row would otherwise blow up the entire
    refresh path with ``AttributeError`` and revert every per-key
    override to env / default — including overrides that hadn't
    been touched since the last successful refresh. Returning ``""``
    for non-coercible values lets the loop continue and the row's
    ``not stripped`` skip clause filter it out cleanly.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return str(value)
    except Exception:
        log.warning(
            "bot_health: ignoring system_settings %s=%r "
            "(non-coercible to str)",
            key, value,
        )
        return ""


# ── Per-loop stale-threshold overrides (Stage-15-Step-E #10b row 11) ──
#
# Stage-15-Step-F's threshold editor ships four global knobs (busy
# inflight, legacy single-knob loop-stale, IPN drop attack, login
# throttle attack). Per-loop stale thresholds (``BOT_HEALTH_LOOP_STALE_
# <UPPER_NAME>_SECONDS``) are still env-only — operators wanting to
# extend the freshness window for a specific loop without redeploying
# (e.g. a new gateway is slow-syncing and ``zarinpal_backfill`` is
# legitimately late) had to live with false-DEGRADED on the panel
# until the next deploy.
#
# This second cache stores per-loop overrides keyed by the **loop
# name** (e.g. ``"fx_refresh"`` → ``600``). The DB-backed key is
# ``BOT_HEALTH_LOOP_STALE_<UPPER_NAME>_SECONDS`` so an operator who
# already knew the env var name finds the same shape on the panel.
#
# Resolution order in :func:`_stale_threshold_seconds`:
#
#   1. Per-loop in-process override (this cache, populated by the
#      panel + boot warm-up).
#   2. Per-loop env var ``BOT_HEALTH_LOOP_STALE_<UPPER_NAME>_SECONDS``.
#   3. Cadence-derived ``2 × LOOP_CADENCES[name] + 60``.
#   4. Legacy fallback (the caller's ``fallback`` arg, typically the
#      ``BOT_HEALTH_LOOP_STALE_SECONDS`` global with its own DB-backed
#      override layer).
#
# Bounds: ``LOOP_STALE_OVERRIDE_MINIMUM`` is 1 s (matches
# ``THRESHOLD_MINIMUMS`` for the global key) and the maximum is one
# week — wide enough for daily-cadence loops with multi-day backoff,
# but narrow enough that a typo of ``604800000`` (ms instead of s)
# is rejected at validation rather than silently disabling stale
# detection for a loop forever.
LOOP_STALE_OVERRIDE_MINIMUM: int = 1
LOOP_STALE_OVERRIDE_MAXIMUM: int = 86_400 * 7  # 1 week, in seconds
_LOOP_STALE_OVERRIDES: dict[str, int] = {}


def loop_stale_setting_key(loop_name: str) -> str:
    """Build the ``system_settings`` / env key for *loop_name*.

    Single source of truth for the key shape so the env path, the
    DB path, and the panel template can't drift. Mirrors the
    existing ``BOT_HEALTH_LOOP_STALE_<UPPER_NAME>_SECONDS`` shape
    documented in HANDOFF since the per-loop env override shipped.
    """
    if not isinstance(loop_name, str) or not loop_name:
        raise ValueError(
            f"loop_stale_setting_key: name must be a non-empty str, "
            f"got {loop_name!r}"
        )
    return f"BOT_HEALTH_LOOP_STALE_{loop_name.upper()}_SECONDS"


def _coerce_loop_stale_seconds(value: object) -> int | None:
    """Validate a per-loop stale-threshold candidate.

    Returns the parsed int when it's a positive int within
    ``[LOOP_STALE_OVERRIDE_MINIMUM, LOOP_STALE_OVERRIDE_MAXIMUM]``;
    ``None`` for anything else (so the caller can decide whether to
    log + reject or silently fall through). Booleans are refused
    explicitly — a stored ``"true"`` row would otherwise coerce to
    ``1`` and shrink every loop's freshness window to 1 s, painting
    the whole panel red.
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
    if candidate < LOOP_STALE_OVERRIDE_MINIMUM:
        return None
    if candidate > LOOP_STALE_OVERRIDE_MAXIMUM:
        return None
    return candidate


def set_loop_stale_override(loop_name: str, value: int) -> None:
    """Apply an in-process per-loop stale-threshold override.

    Defence-in-depth: re-validates via :func:`_coerce_loop_stale_seconds`
    so a future caller bypassing the web-UI's coercer (e.g. a
    direct-call from a script) still gets a clean rejection. The
    panel's POST handler validates *before* writing the DB so an
    invalid value never reaches this function in production.
    """
    coerced = _coerce_loop_stale_seconds(value)
    if coerced is None:
        raise ValueError(
            f"loop stale override for {loop_name!r} must be int in "
            f"[{LOOP_STALE_OVERRIDE_MINIMUM}, "
            f"{LOOP_STALE_OVERRIDE_MAXIMUM}], got {value!r}"
        )
    if not isinstance(loop_name, str) or not loop_name:
        raise ValueError(
            f"loop stale override: name must be a non-empty str, "
            f"got {loop_name!r}"
        )
    _LOOP_STALE_OVERRIDES[loop_name] = coerced


def clear_loop_stale_override(loop_name: str) -> bool:
    """Drop the in-process override for *loop_name*.

    Returns ``True`` if one existed (so the caller can short-circuit
    the audit-log "no-op" branch). Idempotent for unknown names.
    """
    return _LOOP_STALE_OVERRIDES.pop(loop_name, None) is not None


def get_loop_stale_override(loop_name: str) -> int | None:
    """Return the current in-process override for *loop_name* or ``None``."""
    return _LOOP_STALE_OVERRIDES.get(loop_name)


def get_loop_stale_overrides_snapshot() -> dict[str, int]:
    """Read-only copy of the per-loop overrides cache.

    Mirrors :func:`get_threshold_overrides_snapshot`. Returning a
    copy keeps the panel from mutating the live cache by accident
    while iterating its rows.
    """
    return dict(_LOOP_STALE_OVERRIDES)


def reset_loop_stale_overrides_for_tests() -> None:
    """Test-helper: drop every per-loop override slot.

    Mirrors :func:`reset_loop_registry_for_tests` so the autouse
    reset fixtures in ``test_bot_health.py`` / ``test_web_admin.py``
    can null the cache between cases without exporting the private
    dict.
    """
    _LOOP_STALE_OVERRIDES.clear()


async def refresh_loop_stale_overrides_from_db(db) -> dict[str, int]:
    """Reload the per-loop override cache from ``system_settings``.

    Mirrors :func:`refresh_threshold_overrides_from_db` shape but
    iterates every ``BOT_HEALTH_LOOP_STALE_*_SECONDS`` row and
    derives the loop name from the key (strip prefix + suffix,
    lowercase). Excludes the legacy ``BOT_HEALTH_LOOP_STALE_SECONDS``
    key which is owned by the global threshold cache (Stage-15-Step-F).

    Best-effort: a transient DB error keeps the cache in place
    (logged at ERROR). A malformed row clears just that row's
    override (logged at WARNING). A non-string value type
    coerces via :func:`_coerce_setting_to_str` so a single bad row
    can't blow up the whole refresh.
    """
    if db is None:
        return get_loop_stale_overrides_snapshot()
    try:
        raw = await db.list_settings_with_prefix(
            "BOT_HEALTH_LOOP_STALE_"
        )
    except Exception:
        log.exception(
            "bot_health: refresh_loop_stale_overrides_from_db failed; "
            "keeping previous cache"
        )
        return get_loop_stale_overrides_snapshot()
    if not isinstance(raw, dict):
        log.warning(
            "bot_health: list_settings_with_prefix returned %r "
            "(not a dict); keeping previous loop-stale cache",
            type(raw).__name__,
        )
        return get_loop_stale_overrides_snapshot()

    new_overrides: dict[str, int] = {}
    suffix = "_SECONDS"
    prefix = "BOT_HEALTH_LOOP_STALE_"
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        # Skip the legacy single-knob — owned by the global
        # threshold cache, not the per-loop cache.
        if key == "BOT_HEALTH_LOOP_STALE_SECONDS":
            continue
        if not key.startswith(prefix) or not key.endswith(suffix):
            continue
        loop_name = key[len(prefix):-len(suffix)].lower()
        if not loop_name:
            continue
        coerced_str = _coerce_setting_to_str(key, value).strip()
        if not coerced_str:
            continue
        coerced = _coerce_loop_stale_seconds(coerced_str)
        if coerced is None:
            log.warning(
                "bot_health: ignoring system_settings %s=%r "
                "(not in [%d, %d])",
                key, value,
                LOOP_STALE_OVERRIDE_MINIMUM,
                LOOP_STALE_OVERRIDE_MAXIMUM,
            )
            continue
        new_overrides[loop_name] = coerced

    # Atomic swap so a partial update can't leave half-loaded state.
    _LOOP_STALE_OVERRIDES.clear()
    _LOOP_STALE_OVERRIDES.update(new_overrides)
    return dict(_LOOP_STALE_OVERRIDES)


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
#
# Stage-15-Step-F follow-up #5: ``LOOP_CADENCES`` is no longer a
# hand-maintained literal. Each loop module calls
# :func:`register_loop` (decorator or direct call) at its loop's
# definition site to populate this dict + ``metrics._LOOP_METRIC_NAMES``
# at import time. The previous design had two separate hand-maintained
# tables (the cadence dict in this module and the metric-names tuple
# in ``metrics.py``) which had to stay in lockstep — a missing entry
# in either silently downgraded the loop's stale-detection behaviour
# (PR #157 fixed exactly that class of bug for ``zarinpal_backfill``;
# the decorator prevents it by construction).
LOOP_CADENCES: dict[str, int] = {}

# Stage-15-Step-F follow-up #6: per-loop "tick now" runners. Each
# runner is an async callable ``(app: aiohttp.web.Application) ->
# Awaitable[Any]`` that knows how to gather its dependencies (the
# bot, env-derived config, etc.) from the aiohttp application state
# and run a *single* iteration of the loop's work. Populated by
# :func:`register_loop` calls that pass ``runner=``. Not every loop
# has to register a runner — the manual "tick now" button is opt-in,
# and a missing runner just hides the button for that loop.
LOOP_RUNNERS: dict[str, Callable] = {}


def register_loop(
    name: str,
    *,
    cadence_seconds: int,
    runner: Callable | None = None,
):
    """Register a background loop's cadence + heartbeat metric name.

    Usable in two equivalent ways:

    1. As a decorator on a loop coroutine, co-locating the cadence
       with the loop's actual ``await asyncio.sleep(...)``::

           @register_loop("fx_refresh", cadence_seconds=600)
           async def _refresh_loop():
               ...

    2. As a direct call from module-init code, for tick sites that
       are not a single loop function (e.g. the TTL-gated
       ``catalog_refresh`` heartbeat lives inside ``get_catalog``)::

           register_loop("catalog_refresh", cadence_seconds=86_400)

    Side effects:

    * Adds ``name -> cadence_seconds`` to :data:`LOOP_CADENCES` so
      :func:`loop_cadence_seconds` and :func:`_stale_threshold_seconds`
      can answer questions about *name*.
    * Adds *name* to ``metrics._LOOP_METRIC_NAMES`` so the heartbeat
      gauge ``meowassist_<name>_last_run_epoch`` is exposed via
      ``/metrics`` and the panel can iterate every registered loop.
    * If *runner* is supplied, registers it in :data:`LOOP_RUNNERS`
      so the ``/admin/control`` "Tick now" button can drive a single
      iteration of the loop on demand. ``runner`` MUST be an async
      callable taking ``(app: aiohttp.web.Application)``; it should
      gather its own dependencies (bot, DB, env) from ``app`` rather
      than relying on closures over module-level state.

    Idempotent: calling twice with the same args is a no-op.
    Calling twice with mismatching cadence raises ``RuntimeError``
    so a stale literal in one place can't drift from the other.
    Re-registering a different *runner* for the same name overrides
    the previous one — useful for tests that swap in a stub.
    """
    if not isinstance(name, str) or not name:
        raise ValueError(
            f"register_loop: name must be a non-empty string, "
            f"got {name!r}"
        )
    if (
        isinstance(cadence_seconds, bool)
        or not isinstance(cadence_seconds, int)
        or cadence_seconds < 1
    ):
        raise ValueError(
            f"register_loop: cadence_seconds must be a positive int, "
            f"got {cadence_seconds!r}"
        )
    if runner is not None and not callable(runner):
        raise ValueError(
            f"register_loop: runner must be callable or None, "
            f"got {runner!r}"
        )

    existing = LOOP_CADENCES.get(name)
    if existing is not None and existing != cadence_seconds:
        raise RuntimeError(
            f"register_loop: cadence mismatch for {name!r} — "
            f"already registered at {existing}, "
            f"attempt to re-register at {cadence_seconds}. "
            f"Update the call site so both match."
        )
    LOOP_CADENCES[name] = cadence_seconds

    if runner is not None:
        LOOP_RUNNERS[name] = runner

    # Mirror to ``metrics._LOOP_METRIC_NAMES`` so the heartbeat
    # gauge is exposed and the panel iterates this loop. Local
    # import to keep ``bot_health`` / ``metrics`` from forming an
    # eager-import cycle (``metrics`` imports nothing from
    # ``bot_health`` at module load, but a future refactor that
    # adds such an import would otherwise deadlock).
    import metrics
    if name not in metrics._LOOP_METRIC_NAMES:
        metrics._LOOP_METRIC_NAMES = (
            *metrics._LOOP_METRIC_NAMES, name,
        )

    def _decorator(fn):
        return fn

    return _decorator


def loop_runner(name: str) -> Callable | None:
    """Return the registered "tick now" runner for *name* or ``None``.

    The ``/admin/control/loop/<name>/tick-now`` POST handler uses
    this to look up the runner for a given loop name. ``None``
    means "no manual tick available for this loop"; the panel
    hides the button in that case.
    """
    return LOOP_RUNNERS.get(name)


def update_loop_cadence(name: str, cadence_seconds: int) -> int:
    """Replace a loop's published cadence at runtime.

    Stage-15-Step-E #10b row 21 bundled bug fix. :func:`register_loop`
    is invariant-checked: a literal mismatch raises ``RuntimeError``
    so a stale ``cadence_seconds=`` literal at the call site can't
    drift from the loop's actual ``await asyncio.sleep(...)``.

    But some loops legitimately tune their cadence at runtime — the
    bot-health alert loop reads ``BOT_HEALTH_ALERT_INTERVAL_SECONDS``
    from env / DB on every iteration, and an operator who sets that
    to anything other than the compile-time default would otherwise
    leave the panel showing the *old* "stale threshold" (``2 × 60 +
    60 = 180s``) even though the loop is actually ticking every,
    say, 600 s. The panel marks the loop overdue at 180 s, the
    Prometheus heartbeat shows the loop is fine, the operator gets
    a confusing red badge for a healthy loop.

    This helper is the legitimate runtime-update path:

    * Refuses unknown / never-registered loop names with
      :class:`KeyError` (the loop must opt in by registering at
      module import).
    * Refuses non-positive / non-int / boolean cadence values with
      :class:`ValueError` (same shape as :func:`register_loop`).
    * Updates :data:`LOOP_CADENCES` in place.
    * Returns the new cadence so the caller can log it.
    * Idempotent: repeat calls with the same value are a no-op.

    The loop's ``runner`` and ``metrics._LOOP_METRIC_NAMES`` membership
    are NOT touched — only the cadence value the panel reads. Callers
    are expected to call :func:`register_loop` first (typically as a
    decorator at module import) to wire the runner / metric.
    """
    if not isinstance(name, str) or not name:
        raise ValueError(
            f"update_loop_cadence: name must be a non-empty string, "
            f"got {name!r}"
        )
    if (
        isinstance(cadence_seconds, bool)
        or not isinstance(cadence_seconds, int)
        or cadence_seconds < 1
    ):
        raise ValueError(
            f"update_loop_cadence: cadence_seconds must be a positive "
            f"int, got {cadence_seconds!r}"
        )
    if name not in LOOP_CADENCES:
        raise KeyError(
            f"update_loop_cadence: {name!r} is not registered. "
            f"Call register_loop({name!r}, ...) at module import "
            f"first."
        )
    LOOP_CADENCES[name] = cadence_seconds
    return cadence_seconds


def reset_loop_registry_for_tests() -> None:
    """Clear the loop registry. Tests-only.

    Used by tests that want to exercise :func:`register_loop` from
    a clean slate (e.g. mismatch-raises invariant). Production code
    populates the registry at module import via decorator calls in
    each loop module — calling this in production would empty
    :data:`LOOP_CADENCES` and the next ``compute_bot_status`` call
    would silently fall back to legacy thresholds for every loop.
    """
    LOOP_CADENCES.clear()
    LOOP_RUNNERS.clear()
    import metrics
    metrics._LOOP_METRIC_NAMES = ()

# Safety margin added on top of (2 × cadence) so a tick that lands
# just past its nominal window doesn't oscillate the panel between
# fresh and stale.
_STALE_THRESHOLD_MARGIN_SECONDS = 60


def loop_cadence_seconds(loop_name: str) -> int | None:
    """Public accessor: published cadence for *loop_name*.

    Returns the integer seconds-between-ticks for known loops, or
    ``None`` for loops that don't have a registered cadence (these
    fall back to the legacy ``BOT_HEALTH_LOOP_STALE_SECONDS`` knob
    in :func:`_stale_threshold_seconds`).

    Exposed so the ``/admin/control`` panel can surface each loop's
    expected cadence next to its actual last-tick age — operators
    can then tell at a glance whether a loop is overdue (cadence is
    the published "how often it should fire" number, the per-loop
    threshold ``loop_stale_threshold_seconds(name)`` is the
    "declared overdue" number which is roughly twice the cadence).
    """
    return LOOP_CADENCES.get(loop_name)


def loop_stale_threshold_seconds(loop_name: str) -> int:
    """Public accessor: stale threshold for *loop_name* in seconds.

    Same resolution order as the private :func:`_stale_threshold_seconds`
    used by :func:`compute_bot_status`, but with the legacy fallback
    bound at call time so the panel and the classifier agree by
    construction.

    Resolution order:

    1. Explicit env override
       ``BOT_HEALTH_LOOP_STALE_<UPPER_NAME>_SECONDS`` if set to a
       positive integer.
    2. Cadence-derived: ``2 × LOOP_CADENCES[name] +
       _STALE_THRESHOLD_MARGIN_SECONDS``.
    3. Legacy single-knob ``BOT_HEALTH_LOOP_STALE_SECONDS``
       (default :data:`DEFAULT_LOOP_STALE_SECONDS`) for unknown
       loop names.
    """
    legacy = _env_int(
        "BOT_HEALTH_LOOP_STALE_SECONDS", DEFAULT_LOOP_STALE_SECONDS
    )
    return _stale_threshold_seconds(loop_name, fallback=legacy)


def _stale_threshold_seconds(loop_name: str, *, fallback: int) -> int:
    """Per-loop stale threshold in seconds.

    Resolution order:

    1. In-process per-loop override
       (:data:`_LOOP_STALE_OVERRIDES`, populated by the
       ``/admin/control`` per-loop editor + boot warm-up). DB beats
       env so an operator's saved value can't be silently shadowed
       by a stale env override left behind from a previous deploy.
    2. Explicit env override
       ``BOT_HEALTH_LOOP_STALE_<UPPER_NAME>_SECONDS`` if set to a
       positive integer. Bad values fall through silently to the
       next layer (mirrors ``_env_int``'s fail-safe).
    3. Cadence-derived: ``2 × LOOP_CADENCES[name] +
       _STALE_THRESHOLD_MARGIN_SECONDS``.
    4. *fallback* (the caller's legacy single-knob value) for
       unknown loop names.
    """
    db_override = _LOOP_STALE_OVERRIDES.get(loop_name)
    if db_override is not None and db_override >= LOOP_STALE_OVERRIDE_MINIMUM:
        return db_override
    explicit_key = loop_stale_setting_key(loop_name) if loop_name else None
    if explicit_key is not None:
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


def loop_stale_source(loop_name: str) -> str:
    """Return the source label for *loop_name*'s stale threshold.

    One of ``"db" / "env" / "cadence" / "default"`` so the
    ``/admin/control`` panel can render a per-loop badge that
    matches the four-layer resolution order in
    :func:`_stale_threshold_seconds`. Mirrors the per-key ``source``
    column on the global threshold card.
    """
    if _LOOP_STALE_OVERRIDES.get(loop_name) is not None:
        return "db"
    if loop_name:
        env_key = loop_stale_setting_key(loop_name)
        raw = os.getenv(env_key, "").strip()
        if raw:
            try:
                if int(raw) > 0:
                    return "env"
            except ValueError:
                pass
    if loop_name in LOOP_CADENCES:
        return "cadence"
    return "default"


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
    "LOOP_CADENCES",
    "LOOP_RUNNERS",
    "LOOP_STALE_OVERRIDE_MAXIMUM",
    "LOOP_STALE_OVERRIDE_MINIMUM",
    "clear_loop_stale_override",
    "compute_bot_status",
    "get_loop_stale_override",
    "get_loop_stale_overrides_snapshot",
    "get_process_start_epoch",
    "loop_cadence_seconds",
    "loop_runner",
    "loop_stale_setting_key",
    "loop_stale_source",
    "loop_stale_threshold_seconds",
    "refresh_loop_stale_overrides_from_db",
    "register_loop",
    "request_force_stop",
    "reset_loop_registry_for_tests",
    "reset_loop_stale_overrides_for_tests",
    "set_loop_stale_override",
    "status_score",
    "update_loop_cadence",
)
