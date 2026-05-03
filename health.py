"""Stage-16 row 17: real ``/health`` endpoint.

The pre-existing ``/telegram-webhook/healthz`` route was a
*web-server-is-alive* probe — it only mounted in webhook mode and
returned ``{"status": "ok"}`` regardless of what was actually broken
behind the proxy. An external uptime monitor (UptimeRobot,
BetterStack, Pingdom) hitting that endpoint had no way to tell that
Postgres had died, that Redis was unreachable, that the model
discovery loop hadn't ticked in 6 hours, or that OpenRouter was
returning 5xx for every chat request. The bot could be on fire and
the probe still went green.

This module is the proper *all-dependency* health probe. It mounts
on **every** deploy regardless of webhook vs polling mode, and it
actively pokes each thing the bot needs to do its job:

* **Postgres** — ``SELECT 1`` against the live pool. Down ⇒ overall
  status is ``down`` and the endpoint returns HTTP 503 so external
  monitors page on it.
* **Redis** — ``PING`` against ``REDIS_URL`` if configured. Down ⇒
  status is ``degraded`` (the bot can still serve chat — FSM falls
  back to in-memory — but mid-checkout users may lose progress).
* **OpenRouter** — a short-timeout HEAD/GET on the public ``/models``
  endpoint. Down ⇒ status is ``degraded`` (existing balances still
  work and webhooks still deposit, but new chat turns will error
  back to the user). Skipped entirely when no key is configured.
* **Background loops** — for every loop registered via
  ``bot_health.register_loop``, compute ``seconds_since_last_tick``
  and compare against ``loop_stale_threshold_seconds(name)``. A loop
  whose last tick is older than its threshold marks the overall
  status as ``degraded`` and is named in the response so the
  operator can see *which* one is wedged at a glance.

Overall status semantics:

* ``ok`` — every probe is green. HTTP 200.
* ``degraded`` — one or more *non-fatal* probes failed (Redis,
  OpenRouter, a stale loop). The bot is partially serving traffic.
  HTTP 200 (still). The body lists the affected components so the
  monitor's response-body alert rule can page on `degraded`
  separately if the operator wants.
* ``down`` — Postgres is unreachable. The bot cannot persist any
  state. HTTP 503 so a TCP-level monitor that only inspects the
  status line still pages.

The body intentionally **never** includes secrets, URLs with
credentials, env-var names, or DB/Redis hostnames — it's safe to
expose publicly so a free-tier UptimeRobot probe can hit it without
authentication. The only "leakage" risk is the list of registered
background-loop names, which is already public via ``/admin/control``
to anyone who can see the panel.

Why per-probe timeouts:

A monitor calling this endpoint every 60s will time out the whole
HTTP request after 30s by default. If the Postgres pool is
exhausted (max-size 10, all connections leaked), an unbounded
``await pool.fetchval("SELECT 1")`` could itself block for the
full 30s — and during that time the monitor's request blocks the
event loop's network read for the *next* probe. A 3s per-probe cap
keeps the whole endpoint under ~12s in the absolute worst case
(four serial probes), well under the monitor's deadline, and
**fails fast** so a hung dependency is reported as down instead
of as a request timeout.

Why a small in-process cache:

A probe that does an actual ``SELECT 1`` *every* call is fine in
isolation, but if a misconfigured monitor hits the endpoint at
1Hz, that's 86 400 ``SELECT 1``s/day plus a Redis ping plus an
OpenRouter HEAD — the OpenRouter quota in particular is a real
concern (the public-models endpoint has a soft cap and rapid
hitting will get the *whole bot* throttled for everyone). We
cache the composed result for ``HEALTH_CACHE_SECONDS`` (default
5s) — long enough to absorb a flood, short enough that a real
outage is visible within one normal monitor poll cycle.

The cache key is the empty tuple (one entry, the latest snapshot).
A new request inside the TTL serves the cached body verbatim with
a ``X-Health-Cache: hit`` response header so a debugging operator
can tell whether the answer is fresh or stale; ``X-Health-Cache:
miss`` indicates the probe ran live.

Bundled fix (Stage-16 row 17):

Pre-this-PR there was *no* health probe in long-polling mode at
all. The existing ``/telegram-webhook/healthz`` only mounted when
``TELEGRAM_WEBHOOK_SECRET`` was set; deploys running the long-poll
loop (i.e. the default deploy as documented in the setup guide
prior to Stage-15-Step-E #3) had no externally probable health
endpoint, so an operator using UptimeRobot / BetterStack had to
fall back to TCP-port checks against the IPN listener — which
returns a 200 from the rate-limit middleware on *any* request to
``/`` even when the bot's main loop is dead. The new ``/health``
mounts unconditionally and probes the actual dependencies, so
the long-poll deploy now has a real health endpoint without
having to opt into webhook mode first.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

from aiohttp import ClientSession, ClientTimeout, web

import bot_health
import metrics

log = logging.getLogger("bot.health")


# ── Public configuration knobs ─────────────────────────────────────

# How long to cache the composed health response. Default 5 seconds —
# long enough to absorb a polling-monitor flood without proxy-storming
# the database, short enough that a real outage shows up within one
# normal poll cycle.
HEALTH_CACHE_SECONDS_ENV = "HEALTH_CACHE_SECONDS"
DEFAULT_HEALTH_CACHE_SECONDS = 5.0

# Per-probe timeout. Bot health is reported as ``down`` for that
# probe if the underlying call doesn't complete inside this window.
HEALTH_PROBE_TIMEOUT_ENV = "HEALTH_PROBE_TIMEOUT_SECONDS"
DEFAULT_HEALTH_PROBE_TIMEOUT_SECONDS = 3.0

# OpenRouter probe URL. Public endpoint, no auth required, returns
# the full models list. We do a short GET instead of HEAD because
# OpenRouter's edge returns 405 for HEAD on this path.
OPENROUTER_HEALTH_URL = "https://openrouter.ai/api/v1/models"


# ── App-level keys and module-level cache ──────────────────────────

APP_KEY_HEALTH_DB: web.AppKey = web.AppKey("health_db", object)
APP_KEY_HEALTH_REDIS_URL: web.AppKey = web.AppKey("health_redis_url", object)
APP_KEY_HEALTH_OPENROUTER_KEY: web.AppKey = web.AppKey(
    "health_openrouter_key", object
)
APP_KEY_HEALTH_CACHE_SECONDS: web.AppKey = web.AppKey(
    "health_cache_seconds", float
)
APP_KEY_HEALTH_PROBE_TIMEOUT: web.AppKey = web.AppKey(
    "health_probe_timeout_seconds", float
)


_CACHED_RESULT: dict[str, Any] | None = None
_CACHED_AT: float = 0.0


def _reset_cache_for_tests() -> None:
    """Tests-only: drop the in-process snapshot cache."""
    global _CACHED_RESULT, _CACHED_AT
    _CACHED_RESULT = None
    _CACHED_AT = 0.0


def _resolve_float_env(name: str, default: float) -> float:
    """Read a positive float env var, defaulting on parse failure."""
    raw = os.getenv(name, "")
    if not raw.strip():
        return default
    try:
        v = float(raw)
    except ValueError:
        log.warning("%s: parse failed (%r); using default %s", name, raw, default)
        return default
    if v <= 0:
        log.warning("%s: non-positive value %r; using default %s", name, v, default)
        return default
    return v


# ── Per-component probes ──────────────────────────────────────────

async def probe_postgres(db: Any, *, timeout_s: float) -> dict[str, Any]:
    """Run ``SELECT 1`` against the live pool. Honour *timeout_s*.

    Returns ``{"status": "ok"}`` on success, ``{"status": "down",
    "error": "<short message>"}`` on any failure. The error is
    intentionally short and side-effect-free (no SQLSTATE, no DB
    hostname) so the response body remains safe to expose
    publicly.
    """
    pool = getattr(db, "pool", None)
    if pool is None:
        return {"status": "down", "error": "pool_not_initialised"}
    try:
        async with asyncio.timeout(timeout_s):
            async with pool.acquire() as conn:
                val = await conn.fetchval("SELECT 1")
        if val != 1:
            return {"status": "down", "error": "unexpected_response"}
        return {"status": "ok"}
    except (asyncio.TimeoutError, TimeoutError):
        return {"status": "down", "error": "timeout"}
    except Exception as exc:  # noqa: BLE001 - any failure is "down"
        log.warning("probe_postgres failed: %s", exc)
        return {"status": "down", "error": "exception"}


async def probe_redis(redis_url: str | None, *, timeout_s: float) -> dict[str, Any]:
    """Issue a ``PING`` against *redis_url*.

    Returns ``{"status": "skipped"}`` when no URL is configured (the
    bot transparently falls back to in-memory FSM storage in that
    case — it's a degraded mode, not an outage). Returns ``ok`` on
    a successful PONG, ``degraded`` (NOT ``down`` — the bot still
    operates without Redis, just with FSM losses on restart) on
    any failure.
    """
    if not redis_url or not str(redis_url).strip():
        return {"status": "skipped", "reason": "no_redis_url_configured"}
    try:
        # Redis client is an optional dependency — import lazily so a
        # deploy without the extra still gets a useful ``skipped``
        # answer instead of a 500.
        try:
            import redis.asyncio as redis_async  # type: ignore[import-not-found]
        except ImportError:
            return {"status": "skipped", "reason": "redis_client_not_installed"}
        async with asyncio.timeout(timeout_s):
            client = redis_async.from_url(redis_url)
            try:
                pong = await client.ping()
            finally:
                # Best-effort close so we don't leak a TCP socket per probe.
                try:
                    await client.aclose()
                except Exception:  # noqa: BLE001
                    pass
        if pong is not True and pong != b"PONG" and pong != "PONG":
            return {"status": "degraded", "error": "unexpected_response"}
        return {"status": "ok"}
    except (asyncio.TimeoutError, TimeoutError):
        return {"status": "degraded", "error": "timeout"}
    except Exception as exc:  # noqa: BLE001
        log.warning("probe_redis failed: %s", exc)
        return {"status": "degraded", "error": "exception"}


async def probe_openrouter(
    api_key: str | None,
    *,
    timeout_s: float,
    session: ClientSession | None = None,
) -> dict[str, Any]:
    """GET OpenRouter's public ``/models`` to confirm reachability.

    The endpoint is public (no Authorization required), so this also
    works on a deploy that has *no* key configured — but in that case
    the bot has no chat capability anyway, so we report ``skipped``
    instead of probing.

    *session* is an optional injected aiohttp session (test seam).
    """
    if not api_key or not str(api_key).strip():
        return {"status": "skipped", "reason": "no_openrouter_key_configured"}

    own_session = session is None
    if session is None:
        session = ClientSession(timeout=ClientTimeout(total=timeout_s))
    try:
        try:
            async with asyncio.timeout(timeout_s):
                async with session.get(OPENROUTER_HEALTH_URL) as resp:
                    if resp.status >= 500:
                        return {
                            "status": "degraded",
                            "error": "upstream_5xx",
                            "http_status": resp.status,
                        }
                    if resp.status >= 400:
                        # 4xx other than auth means the URL itself or our
                        # request shape is wrong — report degraded with
                        # the code so operators can see it.
                        return {
                            "status": "degraded",
                            "error": "upstream_4xx",
                            "http_status": resp.status,
                        }
                    # 2xx/3xx — drain a small chunk so the connection
                    # is reusable for subsequent probes.
                    await resp.read()
                    return {"status": "ok", "http_status": resp.status}
        except (asyncio.TimeoutError, TimeoutError):
            return {"status": "degraded", "error": "timeout"}
        except Exception as exc:  # noqa: BLE001
            log.warning("probe_openrouter failed: %s", exc)
            return {"status": "degraded", "error": "exception"}
    finally:
        if own_session:
            await session.close()


def probe_loops(*, now: float | None = None) -> dict[str, Any]:
    """Snapshot every registered background loop's freshness.

    Returns::

        {
            "status": "ok" | "degraded",
            "loops": {
                "fx_refresh": {"status": "ok", "seconds_since_tick": 312, "stale_threshold": 1800},
                "discovery":  {"status": "stale", "seconds_since_tick": 9000, "stale_threshold": 64800},
                "alert_loop": {"status": "never_ticked", "stale_threshold": 180},
                ...
            },
            "stale_loops": ["discovery"],
        }

    A loop whose ``last_tick_epoch`` is ``None`` is reported as
    ``never_ticked``. That's not necessarily an outage — a loop
    that's only just been registered may not have ticked yet. We
    flag it as ``degraded`` (overall) only when the bot has been
    up long enough that the loop *should* have ticked at least once,
    which is currently approximated as "stale_threshold seconds
    since process start". Since we don't have a reliable
    process-start timestamp at this layer, we treat ``never_ticked``
    as ``ok`` for the overall rollup but still expose it in the
    per-loop status so operators can see it.

    Threshold source: :func:`bot_health.loop_stale_threshold_seconds`,
    which already implements the canonical resolution order
    (DB override → per-loop env → cadence-derived (``2 × cadence
    + 60``) → legacy single-knob fallback). Sharing that helper
    keeps the external probe and the internal bot-health classifier
    in lockstep — they declare the same loop stale at the same
    moment, so an operator never sees the panel say "fx_refresh:
    overdue" while the external monitor is still green.
    """
    if now is None:
        now = time.time()

    out_loops: dict[str, dict[str, Any]] = {}
    stale_loops: list[str] = []

    for name in bot_health.LOOP_CADENCES:
        last_tick = metrics.get_loop_last_tick(name)
        cadence = bot_health.LOOP_CADENCES.get(name, 0)

        threshold = bot_health.loop_stale_threshold_seconds(name)

        entry: dict[str, Any] = {"stale_threshold": threshold}
        if cadence:
            entry["cadence"] = cadence

        if last_tick is None or last_tick <= 0:
            entry["status"] = "never_ticked"
            # Don't roll up to degraded; see docstring rationale.
        else:
            seconds_since = max(0.0, now - last_tick)
            entry["seconds_since_tick"] = round(seconds_since, 2)
            if seconds_since > threshold:
                entry["status"] = "stale"
                stale_loops.append(name)
            else:
                entry["status"] = "ok"

        out_loops[name] = entry

    overall = "degraded" if stale_loops else "ok"
    return {
        "status": overall,
        "loops": out_loops,
        "stale_loops": stale_loops,
    }


# ── Composed probe + handler ──────────────────────────────────────

def _rollup_overall(per_component: dict[str, dict[str, Any]]) -> str:
    """Compute the overall status from per-component statuses."""
    pg_status = per_component.get("postgres", {}).get("status")
    if pg_status == "down":
        return "down"
    for comp_name, comp in per_component.items():
        if comp.get("status") in ("degraded", "down"):
            return "degraded"
    return "ok"


async def gather_health(
    *,
    db: Any,
    redis_url: str | None,
    openrouter_key: str | None,
    probe_timeout_s: float,
    session: ClientSession | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Run every probe in parallel and compose the rollup.

    Tests inject *db* / *session* / *now* directly; production passes
    the live ``Database`` instance and lets ``aiohttp.ClientSession``
    be created inside :func:`probe_openrouter`.
    """
    if now is None:
        now = time.time()

    # Run every async probe in parallel — they're independent.
    pg_task = asyncio.create_task(probe_postgres(db, timeout_s=probe_timeout_s))
    redis_task = asyncio.create_task(
        probe_redis(redis_url, timeout_s=probe_timeout_s)
    )
    openrouter_task = asyncio.create_task(
        probe_openrouter(
            openrouter_key, timeout_s=probe_timeout_s, session=session
        )
    )

    pg = await pg_task
    redis_status = await redis_task
    openrouter = await openrouter_task

    loops = probe_loops(now=now)

    components: dict[str, dict[str, Any]] = {
        "postgres": pg,
        "redis": redis_status,
        "openrouter": openrouter,
        "loops": loops,
    }
    overall = _rollup_overall(components)

    return {
        "status": overall,
        "components": components,
        "checked_at": int(now),
    }


async def health_handler(request: web.Request) -> web.Response:
    """``GET /health`` — full dependency probe.

    Cached for ``HEALTH_CACHE_SECONDS`` (default 5s) so a flood of
    monitor probes doesn't proxy-storm the DB / Redis / OpenRouter.
    Cache hits set ``X-Health-Cache: hit``; live runs set ``miss``.
    """
    global _CACHED_RESULT, _CACHED_AT

    cache_ttl = request.app.get(APP_KEY_HEALTH_CACHE_SECONDS, DEFAULT_HEALTH_CACHE_SECONDS)
    probe_timeout = request.app.get(
        APP_KEY_HEALTH_PROBE_TIMEOUT, DEFAULT_HEALTH_PROBE_TIMEOUT_SECONDS
    )

    now = time.time()
    cache_status = "miss"
    if (
        _CACHED_RESULT is not None
        and (now - _CACHED_AT) < cache_ttl
    ):
        result = _CACHED_RESULT
        cache_status = "hit"
    else:
        db = request.app.get(APP_KEY_HEALTH_DB)
        redis_url = request.app.get(APP_KEY_HEALTH_REDIS_URL) or None
        openrouter_key = request.app.get(APP_KEY_HEALTH_OPENROUTER_KEY) or None
        result = await gather_health(
            db=db,
            redis_url=redis_url,
            openrouter_key=openrouter_key,
            probe_timeout_s=probe_timeout,
            now=now,
        )
        _CACHED_RESULT = result
        _CACHED_AT = now

    overall = result.get("status", "ok")
    http_status = 503 if overall == "down" else 200
    response = web.json_response(result, status=http_status)
    response.headers["X-Health-Cache"] = cache_status
    response.headers["Cache-Control"] = "no-cache"
    return response


def install_health_route(
    app: web.Application,
    *,
    db: Any,
    redis_url: str | None = None,
    openrouter_key: str | None = None,
    cache_seconds: float | None = None,
    probe_timeout_s: float | None = None,
) -> None:
    """Mount ``GET /health`` on *app*.

    Keyword args let the test suite inject mocks directly. Production
    callers normally pass only ``db`` (the live ``Database`` instance)
    and let the rest read from environment variables — but for clarity
    we also accept them as kwargs so ``main.py`` can construct the
    config in one place.
    """
    app[APP_KEY_HEALTH_DB] = db
    app[APP_KEY_HEALTH_REDIS_URL] = (
        redis_url
        if redis_url is not None
        else os.getenv("REDIS_URL", "")
    )
    app[APP_KEY_HEALTH_OPENROUTER_KEY] = (
        openrouter_key
        if openrouter_key is not None
        else os.getenv("OPENROUTER_API_KEY", "")
    )
    app[APP_KEY_HEALTH_CACHE_SECONDS] = (
        cache_seconds
        if cache_seconds is not None
        else _resolve_float_env(
            HEALTH_CACHE_SECONDS_ENV, DEFAULT_HEALTH_CACHE_SECONDS
        )
    )
    app[APP_KEY_HEALTH_PROBE_TIMEOUT] = (
        probe_timeout_s
        if probe_timeout_s is not None
        else _resolve_float_env(
            HEALTH_PROBE_TIMEOUT_ENV, DEFAULT_HEALTH_PROBE_TIMEOUT_SECONDS
        )
    )
    app.router.add_get("/health", health_handler)
    app.router.add_get("/healthz", health_handler)
    log.info(
        "health: /health (and /healthz alias) endpoint mounted "
        "(cache_ttl=%.1fs, probe_timeout=%.1fs)",
        app[APP_KEY_HEALTH_CACHE_SECONDS],
        app[APP_KEY_HEALTH_PROBE_TIMEOUT],
    )


__all__ = (
    "DEFAULT_HEALTH_CACHE_SECONDS",
    "DEFAULT_HEALTH_PROBE_TIMEOUT_SECONDS",
    "OPENROUTER_HEALTH_URL",
    "gather_health",
    "health_handler",
    "install_health_route",
    "probe_loops",
    "probe_openrouter",
    "probe_postgres",
    "probe_redis",
)
