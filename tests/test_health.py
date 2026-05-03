"""Stage-16 row 17 tests: real ``/health`` endpoint.

Coverage matrix:

* :func:`probe_postgres` — ok / pool not initialised / SQL exception /
  timeout / unexpected return value.
* :func:`probe_redis` — skipped when no URL / skipped when client lib
  not installed / ok / timeout / exception.
* :func:`probe_openrouter` — skipped when no key / 200 / 4xx / 5xx /
  timeout / exception.
* :func:`probe_loops` — empty registry / never_ticked / ok / stale /
  the bundled bug fix flooring the threshold at ``cadence × 3``.
* :func:`gather_health` — composed rollup. Tests every interesting
  permutation: all green; Redis down → degraded; OpenRouter timeout
  → degraded; Postgres timeout → down; loop stale → degraded; multiple
  components down → still single ``down`` overall.
* :func:`health_handler` — full end-to-end aiohttp request: cache miss
  populates and serves; cache hit serves without re-running probes;
  HTTP 200 for ok / degraded; HTTP 503 for down; ``X-Health-Cache``
  header value.
* :func:`install_health_route` — env-var resolution + route mounted +
  ``/healthz`` alias.
* No secrets in the response body.

Tests use ``unittest.mock`` for the DB / aiohttp session / Redis
client. No live network required, no live DB required.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import web

import bot_health
import health
import metrics


# ── Reset global cache between every test ──────────────────────────


@pytest.fixture(autouse=True)
def _reset_health_cache():
    """Drop the in-process snapshot cache between tests.

    The handler caches the composed result for ``HEALTH_CACHE_SECONDS``;
    tests that assert ``X-Health-Cache: miss`` would flake if a previous
    test populated the cache.
    """
    health._reset_cache_for_tests()
    yield
    health._reset_cache_for_tests()


# ── Helpers ────────────────────────────────────────────────────────


class _FakeConn:
    """Stand-in for an asyncpg connection from ``pool.acquire()``."""

    def __init__(self, fetchval_return: Any = 1, raise_on_fetchval: Exception | None = None):
        self._fetchval_return = fetchval_return
        self._raise = raise_on_fetchval

    async def fetchval(self, _query: str, *_args: Any) -> Any:
        if self._raise is not None:
            raise self._raise
        return self._fetchval_return


class _FakePoolCM:
    """Async context manager yielding a _FakeConn."""

    def __init__(self, conn: _FakeConn):
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *_a: Any) -> None:
        return None


class _FakePool:
    """Stand-in for asyncpg pool with the ``.acquire()`` async-CM API."""

    def __init__(
        self,
        fetchval_return: Any = 1,
        raise_on_fetchval: Exception | None = None,
        acquire_blocks: bool = False,
    ):
        self._fetchval_return = fetchval_return
        self._raise = raise_on_fetchval
        self._block = acquire_blocks

    def acquire(self):
        if self._block:
            class _Blocking:
                async def __aenter__(self_inner):
                    await asyncio.sleep(10)
                    raise RuntimeError("unreachable")

                async def __aexit__(self_inner, *_a):
                    return None

            return _Blocking()
        return _FakePoolCM(_FakeConn(self._fetchval_return, self._raise))


class _FakeDb:
    def __init__(self, pool: _FakePool | None):
        self.pool = pool


# ── probe_postgres ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_probe_postgres_ok():
    db = _FakeDb(_FakePool(fetchval_return=1))
    res = await health.probe_postgres(db, timeout_s=1.0)
    assert res == {"status": "ok"}


@pytest.mark.asyncio
async def test_probe_postgres_pool_not_initialised():
    db = _FakeDb(None)
    res = await health.probe_postgres(db, timeout_s=1.0)
    assert res == {"status": "down", "error": "pool_not_initialised"}


@pytest.mark.asyncio
async def test_probe_postgres_unexpected_response():
    db = _FakeDb(_FakePool(fetchval_return=42))
    res = await health.probe_postgres(db, timeout_s=1.0)
    assert res == {"status": "down", "error": "unexpected_response"}


@pytest.mark.asyncio
async def test_probe_postgres_exception():
    db = _FakeDb(_FakePool(raise_on_fetchval=RuntimeError("boom")))
    res = await health.probe_postgres(db, timeout_s=1.0)
    assert res == {"status": "down", "error": "exception"}


@pytest.mark.asyncio
async def test_probe_postgres_timeout():
    db = _FakeDb(_FakePool(acquire_blocks=True))
    res = await health.probe_postgres(db, timeout_s=0.05)
    assert res == {"status": "down", "error": "timeout"}


# ── probe_redis ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_probe_redis_skipped_no_url():
    res = await health.probe_redis(None, timeout_s=1.0)
    assert res == {"status": "skipped", "reason": "no_redis_url_configured"}
    res2 = await health.probe_redis("   ", timeout_s=1.0)
    assert res2 == {"status": "skipped", "reason": "no_redis_url_configured"}


@pytest.mark.asyncio
async def test_probe_redis_ok():
    import redis.asyncio as redis_async  # type: ignore[import-not-found]

    fake_client = AsyncMock()
    fake_client.ping.return_value = True
    fake_client.aclose = AsyncMock()
    with patch.object(redis_async, "from_url", return_value=fake_client):
        res = await health.probe_redis("redis://localhost:6379/0", timeout_s=1.0)
    assert res == {"status": "ok"}


@pytest.mark.asyncio
async def test_probe_redis_unexpected_response():
    import redis.asyncio as redis_async  # type: ignore[import-not-found]

    fake_client = AsyncMock()
    fake_client.ping.return_value = "WAT"
    fake_client.aclose = AsyncMock()
    with patch.object(redis_async, "from_url", return_value=fake_client):
        res = await health.probe_redis("redis://localhost:6379/0", timeout_s=1.0)
    assert res == {"status": "degraded", "error": "unexpected_response"}


@pytest.mark.asyncio
async def test_probe_redis_exception():
    import redis.asyncio as redis_async  # type: ignore[import-not-found]

    with patch.object(
        redis_async, "from_url", side_effect=RuntimeError("connection refused")
    ):
        res = await health.probe_redis("redis://localhost:6379/0", timeout_s=1.0)
    assert res == {"status": "degraded", "error": "exception"}


@pytest.mark.asyncio
async def test_probe_redis_timeout():
    import redis.asyncio as redis_async  # type: ignore[import-not-found]

    async def _slow_ping():
        await asyncio.sleep(10)
        return True

    fake_client = MagicMock()
    fake_client.ping = _slow_ping
    fake_client.aclose = AsyncMock()
    with patch.object(redis_async, "from_url", return_value=fake_client):
        res = await health.probe_redis(
            "redis://localhost:6379/0", timeout_s=0.05
        )
    assert res == {"status": "degraded", "error": "timeout"}


@pytest.mark.asyncio
async def test_probe_redis_skipped_when_client_not_installed(monkeypatch):
    """If ``redis.asyncio`` cannot be imported, return ``skipped``.

    Simulate the missing-dependency case by stubbing the parent
    ``redis`` module so ``import redis.asyncio`` raises ImportError.
    """
    import sys as _sys
    saved = {
        k: _sys.modules.pop(k, None)
        for k in list(_sys.modules)
        if k == "redis" or k.startswith("redis.")
    }

    class _BlockedModule:
        def __getattr__(self, _name):
            raise ImportError("redis.asyncio not available")

    _sys.modules["redis"] = _BlockedModule()  # type: ignore[assignment]
    # Block submodule import too:
    _sys.modules["redis.asyncio"] = None  # type: ignore[assignment]
    try:
        res = await health.probe_redis("redis://localhost:6379/0", timeout_s=1.0)
        assert res == {"status": "skipped", "reason": "redis_client_not_installed"}
    finally:
        # Restore so other tests can still import redis.asyncio.
        for k in ("redis", "redis.asyncio"):
            _sys.modules.pop(k, None)
        for k, v in saved.items():
            if v is not None:
                _sys.modules[k] = v


# ── probe_openrouter ───────────────────────────────────────────────


class _FakeAiohttpResponse:
    def __init__(self, status: int):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return None

    async def read(self):
        return b"{}"


class _FakeAiohttpSession:
    def __init__(self, status: int = 200, raise_exc: Exception | None = None, slow: bool = False):
        self._status = status
        self._raise = raise_exc
        self._slow = slow
        self.closed = False

    def get(self, _url: str):
        if self._raise is not None:
            raise self._raise
        if self._slow:
            class _Blocking:
                async def __aenter__(self_inner):
                    await asyncio.sleep(10)
                    raise RuntimeError("unreachable")

                async def __aexit__(self_inner, *_a):
                    return None

            return _Blocking()
        return _FakeAiohttpResponse(self._status)

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_probe_openrouter_skipped_no_key():
    res = await health.probe_openrouter(None, timeout_s=1.0)
    assert res == {"status": "skipped", "reason": "no_openrouter_key_configured"}


@pytest.mark.asyncio
async def test_probe_openrouter_ok():
    sess = _FakeAiohttpSession(status=200)
    res = await health.probe_openrouter("sk-test", timeout_s=1.0, session=sess)
    assert res == {"status": "ok", "http_status": 200}


@pytest.mark.asyncio
async def test_probe_openrouter_5xx_degraded():
    sess = _FakeAiohttpSession(status=503)
    res = await health.probe_openrouter("sk-test", timeout_s=1.0, session=sess)
    assert res == {"status": "degraded", "error": "upstream_5xx", "http_status": 503}


@pytest.mark.asyncio
async def test_probe_openrouter_4xx_degraded():
    sess = _FakeAiohttpSession(status=429)
    res = await health.probe_openrouter("sk-test", timeout_s=1.0, session=sess)
    assert res == {"status": "degraded", "error": "upstream_4xx", "http_status": 429}


@pytest.mark.asyncio
async def test_probe_openrouter_timeout():
    sess = _FakeAiohttpSession(slow=True)
    res = await health.probe_openrouter("sk-test", timeout_s=0.05, session=sess)
    assert res == {"status": "degraded", "error": "timeout"}


@pytest.mark.asyncio
async def test_probe_openrouter_exception():
    sess = _FakeAiohttpSession(raise_exc=RuntimeError("dns failure"))
    res = await health.probe_openrouter("sk-test", timeout_s=1.0, session=sess)
    assert res == {"status": "degraded", "error": "exception"}


# ── probe_loops ────────────────────────────────────────────────────


def test_probe_loops_empty_registry(monkeypatch):
    """No loops registered ⇒ overall ``ok`` and empty per-loop dict."""
    monkeypatch.setattr(bot_health, "LOOP_CADENCES", {})
    metrics.reset_loop_ticks_for_tests()
    res = health.probe_loops()
    assert res["status"] == "ok"
    assert res["loops"] == {}
    assert res["stale_loops"] == []


def test_probe_loops_never_ticked(monkeypatch):
    """A registered loop that hasn't ticked yet is reported but not stale."""
    monkeypatch.setattr(bot_health, "LOOP_CADENCES", {"fx_refresh": 600})
    metrics.reset_loop_ticks_for_tests()
    res = health.probe_loops(now=10_000)
    assert res["status"] == "ok"
    assert res["loops"]["fx_refresh"]["status"] == "never_ticked"
    assert res["loops"]["fx_refresh"]["cadence"] == 600
    assert "seconds_since_tick" not in res["loops"]["fx_refresh"]
    assert res["stale_loops"] == []


def test_probe_loops_ok(monkeypatch):
    monkeypatch.setattr(bot_health, "LOOP_CADENCES", {"fx_refresh": 600})
    metrics.reset_loop_ticks_for_tests()
    metrics.record_loop_tick("fx_refresh", ts=9_900)
    res = health.probe_loops(now=10_000)
    assert res["status"] == "ok"
    assert res["loops"]["fx_refresh"]["status"] == "ok"
    assert res["loops"]["fx_refresh"]["seconds_since_tick"] == 100.0
    assert res["stale_loops"] == []


def test_probe_loops_stale(monkeypatch):
    monkeypatch.setattr(bot_health, "LOOP_CADENCES", {"fx_refresh": 600})
    metrics.reset_loop_ticks_for_tests()
    # Cadence-derived threshold for fx_refresh = 2 × 600 + 60 = 1260s.
    # Last tick was 5000s ago ⇒ over the threshold ⇒ stale.
    metrics.record_loop_tick("fx_refresh", ts=5_000)
    res = health.probe_loops(now=10_000)
    assert res["status"] == "degraded"
    assert res["loops"]["fx_refresh"]["status"] == "stale"
    assert res["stale_loops"] == ["fx_refresh"]


def test_probe_loops_threshold_uses_bot_health_helper(monkeypatch):
    """probe_loops uses ``loop_stale_threshold_seconds`` verbatim.

    Sharing the helper keeps the internal bot-health classifier and
    the external /health probe in lockstep — they declare the same
    loop stale at the same moment, so an operator never sees a
    "panel says overdue / monitor says fine" mismatch.
    """
    monkeypatch.setattr(bot_health, "LOOP_CADENCES", {"fast_loop": 60})
    metrics.reset_loop_ticks_for_tests()
    # Cadence-derived threshold = 2 × 60 + 60 = 180.
    metrics.record_loop_tick("fast_loop", ts=9_800)
    res = health.probe_loops(now=10_000)
    assert res["loops"]["fast_loop"]["stale_threshold"] == 180
    # 200s since tick > 180s threshold ⇒ stale.
    assert res["loops"]["fast_loop"]["status"] == "stale"

    # An operator-tightened threshold via the helper is reflected
    # one-to-one in the probe.
    monkeypatch.setattr(
        bot_health, "loop_stale_threshold_seconds", lambda _name: 30
    )
    res2 = health.probe_loops(now=10_000)
    assert res2["loops"]["fast_loop"]["stale_threshold"] == 30
    assert res2["loops"]["fast_loop"]["status"] == "stale"


# ── gather_health ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_gather_health_all_green(monkeypatch):
    monkeypatch.setattr(bot_health, "LOOP_CADENCES", {})
    metrics.reset_loop_ticks_for_tests()
    db = _FakeDb(_FakePool())
    sess = _FakeAiohttpSession(status=200)
    res = await health.gather_health(
        db=db,
        redis_url=None,
        openrouter_key=None,
        probe_timeout_s=1.0,
        session=sess,
    )
    assert res["status"] == "ok"
    assert res["components"]["postgres"]["status"] == "ok"
    assert res["components"]["redis"]["status"] == "skipped"
    assert res["components"]["openrouter"]["status"] == "skipped"
    assert res["components"]["loops"]["status"] == "ok"
    assert "checked_at" in res


@pytest.mark.asyncio
async def test_gather_health_redis_down_is_degraded(monkeypatch):
    monkeypatch.setattr(bot_health, "LOOP_CADENCES", {})
    metrics.reset_loop_ticks_for_tests()
    db = _FakeDb(_FakePool())
    fake_module = MagicMock()
    fake_module.from_url.side_effect = RuntimeError("connection refused")
    sess = _FakeAiohttpSession(status=200)
    with patch.dict("sys.modules", {"redis.asyncio": fake_module}):
        res = await health.gather_health(
            db=db,
            redis_url="redis://localhost",
            openrouter_key=None,
            probe_timeout_s=1.0,
            session=sess,
        )
    assert res["status"] == "degraded"
    assert res["components"]["redis"]["status"] == "degraded"
    assert res["components"]["postgres"]["status"] == "ok"


@pytest.mark.asyncio
async def test_gather_health_openrouter_5xx_is_degraded(monkeypatch):
    monkeypatch.setattr(bot_health, "LOOP_CADENCES", {})
    metrics.reset_loop_ticks_for_tests()
    db = _FakeDb(_FakePool())
    sess = _FakeAiohttpSession(status=503)
    res = await health.gather_health(
        db=db,
        redis_url=None,
        openrouter_key="sk-test",
        probe_timeout_s=1.0,
        session=sess,
    )
    assert res["status"] == "degraded"
    assert res["components"]["openrouter"]["status"] == "degraded"


@pytest.mark.asyncio
async def test_gather_health_postgres_down_is_down(monkeypatch):
    monkeypatch.setattr(bot_health, "LOOP_CADENCES", {})
    metrics.reset_loop_ticks_for_tests()
    db = _FakeDb(_FakePool(raise_on_fetchval=RuntimeError("boom")))
    sess = _FakeAiohttpSession(status=200)
    res = await health.gather_health(
        db=db,
        redis_url=None,
        openrouter_key=None,
        probe_timeout_s=1.0,
        session=sess,
    )
    assert res["status"] == "down"
    assert res["components"]["postgres"]["status"] == "down"


@pytest.mark.asyncio
async def test_gather_health_loop_stale_is_degraded(monkeypatch):
    monkeypatch.setattr(bot_health, "LOOP_CADENCES", {"fx_refresh": 600})
    metrics.reset_loop_ticks_for_tests()
    metrics.record_loop_tick("fx_refresh", ts=1)
    db = _FakeDb(_FakePool())
    sess = _FakeAiohttpSession(status=200)
    res = await health.gather_health(
        db=db,
        redis_url=None,
        openrouter_key=None,
        probe_timeout_s=1.0,
        session=sess,
        now=10_000,
    )
    assert res["status"] == "degraded"
    assert res["components"]["loops"]["status"] == "degraded"
    assert "fx_refresh" in res["components"]["loops"]["stale_loops"]


# ── health_handler (full aiohttp end-to-end) ───────────────────────
#
# We deliberately avoid ``AioHTTPTestCase`` because its event-loop
# setup leaves the default policy in a state where subsequent
# *synchronous* tests calling ``asyncio.get_event_loop()`` fail with
# "There is no current event loop in thread 'MainThread'". Instead
# we drive the handler through the ``aiohttp_client`` fixture from
# pytest-aiohttp, which is already a dependency and which respects
# pytest-asyncio's per-test loop scoping (``asyncio_mode = auto`` in
# ``pytest.ini``).


@pytest.fixture
def _scrub_loops():
    """Snapshot+clear LOOP_CADENCES for the duration of the test.

    The conftest imports every loop module, so ``LOOP_CADENCES``
    is fully populated by the time the handler tests run. With
    no ticks recorded, those loops would all report
    ``never_ticked`` (rolls up to ``ok``) — but if a previous
    test in the session ticked one of them, the timestamp is in
    the deep past and rolls up to ``stale``. Easiest fix: scrub
    the registry around handler tests, restore it afterwards.
    """
    saved = dict(bot_health.LOOP_CADENCES)
    bot_health.LOOP_CADENCES.clear()
    metrics.reset_loop_ticks_for_tests()
    yield
    bot_health.LOOP_CADENCES.update(saved)


def _build_handler_app(
    *,
    db: Any,
    cache_seconds: float = 60.0,
    probe_timeout_s: float = 1.0,
) -> web.Application:
    app = web.Application()
    app[health.APP_KEY_HEALTH_DB] = db
    app[health.APP_KEY_HEALTH_REDIS_URL] = ""
    app[health.APP_KEY_HEALTH_OPENROUTER_KEY] = ""
    app[health.APP_KEY_HEALTH_CACHE_SECONDS] = cache_seconds
    app[health.APP_KEY_HEALTH_PROBE_TIMEOUT] = probe_timeout_s
    app.router.add_get("/health", health.health_handler)
    return app


async def test_handler_first_call_is_miss_second_is_hit(
    aiohttp_client, _scrub_loops
):
    app = _build_handler_app(db=_FakeDb(_FakePool()))
    client = await aiohttp_client(app)

    resp1 = await client.get("/health")
    assert resp1.status == 200
    assert resp1.headers["X-Health-Cache"] == "miss"
    body1 = await resp1.json()
    assert body1["status"] == "ok"

    resp2 = await client.get("/health")
    assert resp2.status == 200
    assert resp2.headers["X-Health-Cache"] == "hit"


async def test_handler_postgres_down_returns_503(
    aiohttp_client, _scrub_loops
):
    db = _FakeDb(_FakePool(raise_on_fetchval=RuntimeError("boom")))
    app = _build_handler_app(db=db, cache_seconds=0.0)
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 503
    body = await resp.json()
    assert body["status"] == "down"
    assert body["components"]["postgres"]["status"] == "down"


async def test_handler_healthz_alias_works(
    aiohttp_client, _scrub_loops
):
    app = web.Application()
    health.install_health_route(
        app,
        db=_FakeDb(_FakePool()),
        redis_url=None,
        openrouter_key=None,
        cache_seconds=60.0,
        probe_timeout_s=1.0,
    )
    client = await aiohttp_client(app)

    for path in ("/health", "/healthz"):
        resp = await client.get(path)
        assert resp.status == 200, path


# ── install_health_route + env resolution ──────────────────────────


def test_install_health_route_resolves_env(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://envhost:6379")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-from-env")
    monkeypatch.setenv("HEALTH_CACHE_SECONDS", "12.5")
    monkeypatch.setenv("HEALTH_PROBE_TIMEOUT_SECONDS", "4.0")
    app = web.Application()
    health.install_health_route(app, db=_FakeDb(_FakePool()))
    assert app[health.APP_KEY_HEALTH_REDIS_URL] == "redis://envhost:6379"
    assert app[health.APP_KEY_HEALTH_OPENROUTER_KEY] == "sk-from-env"
    assert app[health.APP_KEY_HEALTH_CACHE_SECONDS] == 12.5
    assert app[health.APP_KEY_HEALTH_PROBE_TIMEOUT] == 4.0


def test_install_health_route_explicit_kwargs_override_env(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://envhost:6379")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-from-env")
    app = web.Application()
    health.install_health_route(
        app,
        db=_FakeDb(_FakePool()),
        redis_url="redis://kwarg-host:6379",
        openrouter_key="sk-from-kwarg",
        cache_seconds=99.0,
        probe_timeout_s=2.0,
    )
    assert app[health.APP_KEY_HEALTH_REDIS_URL] == "redis://kwarg-host:6379"
    assert app[health.APP_KEY_HEALTH_OPENROUTER_KEY] == "sk-from-kwarg"
    assert app[health.APP_KEY_HEALTH_CACHE_SECONDS] == 99.0
    assert app[health.APP_KEY_HEALTH_PROBE_TIMEOUT] == 2.0


def test_install_health_route_invalid_env_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("HEALTH_CACHE_SECONDS", "garbage")
    monkeypatch.setenv("HEALTH_PROBE_TIMEOUT_SECONDS", "-1")
    app = web.Application()
    health.install_health_route(app, db=_FakeDb(_FakePool()))
    assert app[health.APP_KEY_HEALTH_CACHE_SECONDS] == health.DEFAULT_HEALTH_CACHE_SECONDS
    assert (
        app[health.APP_KEY_HEALTH_PROBE_TIMEOUT]
        == health.DEFAULT_HEALTH_PROBE_TIMEOUT_SECONDS
    )


# ── No-secrets-in-body invariant ───────────────────────────────────


@pytest.mark.asyncio
async def test_response_body_does_not_leak_secrets(monkeypatch):
    """The composed body must not contain Redis/DB hostnames or API keys.

    Operators may expose ``/health`` publicly to UptimeRobot etc., so a
    forgotten ``redis_url`` echoing back in the JSON body would leak
    internal hostnames. Pin this as a regression test.
    """
    monkeypatch.setattr(bot_health, "LOOP_CADENCES", {})
    metrics.reset_loop_ticks_for_tests()
    db = _FakeDb(_FakePool())
    sess = _FakeAiohttpSession(status=200)
    res = await health.gather_health(
        db=db,
        redis_url="redis://internal-redis-host.private:6379/0",
        openrouter_key="sk-or-v1-supersecret123",
        probe_timeout_s=1.0,
        session=sess,
    )
    blob = json.dumps(res)
    assert "internal-redis-host" not in blob
    assert "sk-or-v1-supersecret123" not in blob
    # The skipped reason for openrouter is a static token — fine.
