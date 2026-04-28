"""Tests for rate_limit.py.

Covers TokenBucket math, the LRU bucket cache, the per-user
``consume_chat_token`` helper, and the aiohttp webhook middleware.
"""

from __future__ import annotations

import pytest
from aiohttp import web

import rate_limit as rl
from rate_limit import (
    WEBHOOK_RATE_LIMIT_CACHE_KEY,
    TokenBucket,
    _LRUBucketCache,
    configure_chat_rate_limiter,
    consume_chat_token,
    install_webhook_rate_limit,
)


# ---- TokenBucket -----------------------------------------------------


class FakeClock:
    """Replaces time.monotonic with a deterministic clock the test
    can advance manually."""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def fake_clock(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(rl.time, "monotonic", clock)
    return clock


def test_bucket_starts_full(fake_clock):
    b = TokenBucket(capacity=3, refill_rate=1)
    assert b.try_consume() is True
    assert b.try_consume() is True
    assert b.try_consume() is True
    assert b.try_consume() is False  # 4th drained


def test_bucket_refill_over_time(fake_clock):
    b = TokenBucket(capacity=2, refill_rate=1)  # 1 token/sec
    assert b.try_consume() is True
    assert b.try_consume() is True
    fake_clock.advance(0.5)
    assert b.try_consume() is False  # only 0.5 tokens accumulated
    fake_clock.advance(2.0)
    assert b.try_consume() is True  # capped at 2 tokens
    assert b.try_consume() is True
    assert b.try_consume() is False


def test_bucket_capacity_capped(fake_clock):
    """Idle period should NOT let bucket grow above capacity."""
    b = TokenBucket(capacity=2, refill_rate=1_000)
    fake_clock.advance(1_000_000.0)
    assert b.try_consume() is True
    assert b.try_consume() is True
    assert b.try_consume() is False


def test_bucket_validates_args():
    with pytest.raises(ValueError):
        TokenBucket(capacity=0, refill_rate=1)
    with pytest.raises(ValueError):
        TokenBucket(capacity=1, refill_rate=0)


# ---- _LRUBucketCache ------------------------------------------------


@pytest.mark.asyncio
async def test_cache_separates_keys(fake_clock):
    cache = _LRUBucketCache(capacity=1, refill_rate=1)
    assert await cache.consume("alice") is True
    assert await cache.consume("alice") is False  # alice's bucket empty
    assert await cache.consume("bob") is True  # bob has his own


@pytest.mark.asyncio
async def test_cache_lru_eviction(fake_clock):
    cache = _LRUBucketCache(capacity=1, refill_rate=1, max_size=2)
    assert await cache.consume("a") is True  # a's bucket: 0 tokens
    assert await cache.consume("b") is True  # b's bucket: 0 tokens; cache=[a, b]
    # Inserting c should evict the LRU (a).
    assert await cache.consume("c") is True  # cache=[b, c]
    # b is still depleted because we never advanced the clock.
    assert await cache.consume("b") is False
    # a was evicted, so its next consume creates a fresh bucket.
    assert await cache.consume("a") is True


# ---- consume_chat_token ---------------------------------------------


@pytest.fixture
def tight_chat_limiter(fake_clock):
    """Replace the module-level chat limiter with a tight one for
    testing, then restore the default afterwards so other tests aren't
    affected."""
    configure_chat_rate_limiter(capacity=2, refill_rate=0.001)
    yield
    configure_chat_rate_limiter()  # restore defaults


@pytest.mark.asyncio
async def test_consume_chat_token_under_cap(tight_chat_limiter):
    assert await consume_chat_token(user_id=1) is True
    assert await consume_chat_token(user_id=1) is True


@pytest.mark.asyncio
async def test_consume_chat_token_throttles_over_cap(tight_chat_limiter):
    assert await consume_chat_token(user_id=1) is True
    assert await consume_chat_token(user_id=1) is True
    assert await consume_chat_token(user_id=1) is False


@pytest.mark.asyncio
async def test_consume_chat_token_separates_users(tight_chat_limiter):
    assert await consume_chat_token(user_id=1) is True
    assert await consume_chat_token(user_id=1) is True
    assert await consume_chat_token(user_id=1) is False
    # User 2 starts with a full bucket.
    assert await consume_chat_token(user_id=2) is True
    assert await consume_chat_token(user_id=2) is True
    assert await consume_chat_token(user_id=2) is False


@pytest.mark.asyncio
async def test_consume_chat_token_default_capacity(fake_clock):
    """Defaults are (capacity=5, refill_rate=1) — burst 5 should pass,
    6th should fail."""
    configure_chat_rate_limiter()  # reset to defaults
    user = 12345
    for _ in range(5):
        assert await consume_chat_token(user) is True
    assert await consume_chat_token(user) is False
    configure_chat_rate_limiter()  # leave clean for siblings


@pytest.mark.asyncio
async def test_chat_handler_uses_consume_chat_token():
    """Smoke test: the AI catch-all handler MUST call
    ``consume_chat_token`` so commands / FSM state inputs aren't
    throttled. Anchors the design decision in code (see Devin Review
    feedback on PR #47)."""
    import inspect

    import handlers as h

    src = inspect.getsource(h.process_chat)
    assert "consume_chat_token" in src, (
        "process_chat must call consume_chat_token directly. Do NOT "
        "reintroduce a dp.message middleware — that throttles "
        "unrelated handlers like /start and FSM state inputs."
    )


# ---- webhook_rate_limit_middleware ---------------------------------


async def _echo(request):
    return web.Response(text="ok")


@pytest.mark.asyncio
async def test_webhook_under_cap_passes(aiohttp_client, fake_clock):
    """5 requests well under the default cap (30 tokens) sail through."""
    app = web.Application()
    install_webhook_rate_limit(app)
    app.router.add_post("/x", _echo)
    client = await aiohttp_client(app)
    for _ in range(5):
        resp = await client.post("/x", data="x")
        assert resp.status == 200


@pytest.mark.asyncio
async def test_webhook_burst_throttles(aiohttp_client, fake_clock):
    """Tight cap → small burst trips 429."""
    app = web.Application()
    install_webhook_rate_limit(app, capacity=2, refill_rate=0.001)
    app.router.add_post("/x", _echo)
    client = await aiohttp_client(app)

    r1 = await client.post("/x", data="a")
    r2 = await client.post("/x", data="b")
    r3 = await client.post("/x", data="c")
    assert r1.status == 200
    assert r2.status == 200
    assert r3.status == 429


@pytest.mark.asyncio
async def test_webhook_separates_keys_via_cache(fake_clock):
    """The cache itself separates buckets by key. Combined with the
    middleware test above this covers same-remote = throttled while
    different remotes wouldn't be."""
    cache = _LRUBucketCache(capacity=1, refill_rate=0.001)
    assert await cache.consume("remote_a") is True
    assert await cache.consume("remote_a") is False
    assert await cache.consume("remote_b") is True
    assert await cache.consume("remote_b") is False


@pytest.mark.asyncio
async def test_install_webhook_rate_limit_seeds_cache():
    """Helper should both register the middleware and pre-seed the cache
    so the middleware can do a frozen-app lookup."""
    app = web.Application()
    install_webhook_rate_limit(app)
    assert WEBHOOK_RATE_LIMIT_CACHE_KEY in app
    cache = app[WEBHOOK_RATE_LIMIT_CACHE_KEY]
    assert isinstance(cache, _LRUBucketCache)
    # The middleware tuple should now contain our function.
    from rate_limit import webhook_rate_limit_middleware

    assert webhook_rate_limit_middleware in tuple(app.middlewares)
