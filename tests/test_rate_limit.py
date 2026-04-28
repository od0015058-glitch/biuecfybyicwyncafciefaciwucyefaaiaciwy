"""Tests for rate_limit.py.

Covers TokenBucket math, the LRU bucket cache, the aiogram chat
middleware, and the aiohttp webhook middleware.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from aiohttp import web

import rate_limit as rl
from rate_limit import (
    WEBHOOK_RATE_LIMIT_CACHE_KEY,
    ChatRateLimitMiddleware,
    TokenBucket,
    _LRUBucketCache,
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


# ---- ChatRateLimitMiddleware ---------------------------------------


def _fake_message(user_id: int = 1, text: str = "hi"):
    """Minimal stub that quacks like aiogram.types.Message."""
    msg = AsyncMock()
    msg.from_user = type("User", (), {"id": user_id})()
    msg.text = text
    return msg


@pytest.mark.asyncio
async def test_chat_middleware_lets_through_under_cap(monkeypatch, fake_clock):
    mw = ChatRateLimitMiddleware(capacity=3, refill_rate=1)
    handler = AsyncMock(return_value="ok")

    msg = _fake_message()
    monkeypatch.setattr(rl, "Message", type(msg))

    for _ in range(3):
        result = await mw(handler, msg, {})
        assert result == "ok"
    assert handler.await_count == 3


@pytest.mark.asyncio
async def test_chat_middleware_throttles_over_cap(monkeypatch, fake_clock):
    mw = ChatRateLimitMiddleware(capacity=2, refill_rate=0.001)
    handler = AsyncMock(return_value="ok")
    msg = _fake_message()

    monkeypatch.setattr(rl, "Message", type(msg))

    assert await mw(handler, msg, {}) == "ok"
    assert await mw(handler, msg, {}) == "ok"
    # Third call: bucket empty, should short-circuit and call answer().
    assert await mw(handler, msg, {}) is None
    assert handler.await_count == 2  # not called the 3rd time
    msg.answer.assert_awaited()


@pytest.mark.asyncio
async def test_chat_middleware_separates_users(monkeypatch, fake_clock):
    mw = ChatRateLimitMiddleware(capacity=1, refill_rate=0.001)
    handler = AsyncMock(return_value="ok")

    msg_a = _fake_message(user_id=1)
    msg_b = _fake_message(user_id=2)

    monkeypatch.setattr(rl, "Message", type(msg_a))

    assert await mw(handler, msg_a, {}) == "ok"
    assert await mw(handler, msg_a, {}) is None
    assert await mw(handler, msg_b, {}) == "ok"  # b's bucket untouched


@pytest.mark.asyncio
async def test_chat_middleware_passes_non_message(fake_clock):
    """Callback queries / unknown event types skip the middleware
    entirely (we left rate_limit.Message alone, so the mock isn't an
    instance and the early-return path fires)."""
    mw = ChatRateLimitMiddleware(capacity=1, refill_rate=0.001)
    handler = AsyncMock(return_value="ok")

    not_a_message = AsyncMock()
    not_a_message.from_user = type("User", (), {"id": 1})()

    for _ in range(5):
        assert await mw(handler, not_a_message, {}) == "ok"


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
