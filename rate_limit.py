"""Rate limiting primitives.

Two consumers:

* aiogram middleware on the chat handler — bounds how fast a single
  Telegram user can fire prompts at OpenRouter (cost control + DoS).
* aiohttp middleware on the NowPayments webhook — bounds per-IP
  request rate (DoS defence; the legitimate IPN retry rhythm is well
  under any sane cap).

Both use a simple in-memory **token bucket** keyed by the caller's
identity. We deliberately don't share the limiter across processes —
in production there's a single bot process anyway. If we ever scale
out, this is the obvious thing to swap with a Redis-backed limiter,
but the interface stays the same.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from typing import Any, Awaitable, Callable, Dict, Hashable

from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject
from aiohttp import web

log = logging.getLogger("bot.rate_limit")


class TokenBucket:
    """Classic token bucket.

    `capacity` tokens accumulate at `refill_rate` tokens/sec, capped
    at capacity. ``try_consume()`` returns True if there was at least
    one token and decrements; False otherwise.

    Time source is ``time.monotonic`` so the bucket isn't perturbed
    by wall-clock jumps (NTP, leap seconds, ...).
    """

    __slots__ = ("capacity", "refill_rate", "_tokens", "_last_refill")

    def __init__(self, capacity: float, refill_rate: float) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        if refill_rate <= 0:
            raise ValueError("refill_rate must be > 0")
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()

    def _refill(self, now: float) -> None:
        elapsed = now - self._last_refill
        if elapsed <= 0:
            return
        self._tokens = min(
            self.capacity, self._tokens + elapsed * self.refill_rate
        )
        self._last_refill = now

    def try_consume(self, tokens: float = 1.0) -> bool:
        now = time.monotonic()
        self._refill(now)
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False


class _LRUBucketCache:
    """Bounded cache mapping identity -> TokenBucket.

    We don't want a long-running bot to retain a bucket for every
    Telegram user / IP that ever talked to it, so cap at ``max_size``
    via LRU eviction. ``capacity`` and ``refill_rate`` are fixed for
    every bucket the cache vends.
    """

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        max_size: int = 10_000,
    ) -> None:
        self._capacity = capacity
        self._refill_rate = refill_rate
        self._max_size = max_size
        self._buckets: OrderedDict[Hashable, TokenBucket] = OrderedDict()
        self._lock = asyncio.Lock()

    async def consume(self, key: Hashable, tokens: float = 1.0) -> bool:
        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(self._capacity, self._refill_rate)
                self._buckets[key] = bucket
            else:
                self._buckets.move_to_end(key)
            ok = bucket.try_consume(tokens)
            while len(self._buckets) > self._max_size:
                self._buckets.popitem(last=False)
            return ok


# Typed AppKey for the per-process bucket cache stashed on the aiohttp
# Application. aiohttp 3.10+ warns (and we error on warnings in
# pytest.ini) when plain string keys are used.
WEBHOOK_RATE_LIMIT_CACHE_KEY: web.AppKey = web.AppKey(
    "_webhook_rate_limit_cache", _LRUBucketCache
)


class ChatRateLimitMiddleware(BaseMiddleware):
    """Aiogram middleware: per-user token bucket on chat messages.

    Defaults: 5 message tokens, refilling at 1/sec. So a user can burst
    5 messages, then has to wait ~1 sec for each subsequent prompt.
    Anything else (callbacks, command handlers, FSM-state handlers)
    is unaffected — only this exact catch-all chat path costs OpenRouter
    money and so is worth gating.

    Call site: ``dp.message.middleware(ChatRateLimitMiddleware())``
    registered AFTER ``UserUpsertMiddleware`` so we still upsert the
    user before potentially throttling their message.
    """

    def __init__(
        self,
        capacity: float = 5.0,
        refill_rate: float = 1.0,
        warn_text: str = (
            "⏳ You're sending messages too quickly. Please wait a moment."
        ),
    ) -> None:
        self._cache = _LRUBucketCache(capacity, refill_rate)
        self._warn_text = warn_text

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        # Only gate generic Message events that look like real prompts.
        # Callbacks (button taps) skip this entirely — they're cheap.
        if not isinstance(event, Message):
            return await handler(event, data)
        from_user = getattr(event, "from_user", None)
        if from_user is None or from_user.id is None:
            return await handler(event, data)

        ok = await self._cache.consume(from_user.id)
        if ok:
            return await handler(event, data)

        log.info(
            "chat rate-limited telegram_id=%s text=%r",
            from_user.id,
            (event.text or "")[:40],
        )
        try:
            await event.answer(self._warn_text)
        except Exception:
            # Don't let a downstream Telegram error mask the throttle.
            log.exception("failed to send rate-limit notice")
        return None  # short-circuit — handler chain stops here.


@web.middleware
async def webhook_rate_limit_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    """aiohttp middleware: per-IP token bucket on POSTs.

    Reasonable cap: 30 tokens, refilling at 5/sec. That's well above
    NowPayments' real IPN retry rhythm (a handful per minute per
    payment) but bounds DoS bursts. We deliberately rate-limit BEFORE
    the body is read so a flood of 1MB POSTs can't pin the loop.

    The bucket cache lives on the application itself so it survives
    request boundaries.
    """
    # Cache is pre-seeded by ``install_webhook_rate_limit`` before the
    # app is started. We don't lazy-init here because aiohttp freezes
    # the app on startup and modifying app state after that raises.
    cache = request.app[WEBHOOK_RATE_LIMIT_CACHE_KEY]

    # remote can be None in tests / behind weird proxies; bucket key
    # falls back to the URL path so at least the limiter's still
    # bounded.
    key = request.remote or request.path or "_unknown_"
    ok = await cache.consume(key)
    if not ok:
        log.warning(
            "webhook rate-limited remote=%s path=%s",
            request.remote,
            request.path,
        )
        return web.Response(status=429, text="Too Many Requests")
    return await handler(request)


def install_webhook_rate_limit(
    app: web.Application,
    capacity: float = 30.0,
    refill_rate: float = 5.0,
) -> None:
    """Wire the per-IP rate limiter into an aiohttp app.

    Call this at app-construction time, BEFORE adding routes / starting
    the runner — it both registers the middleware and pre-seeds the
    cache so the middleware can do an unconditional lookup at request
    time without fighting aiohttp's frozen-app state.

    Defaults: 30 tokens / 5 per-second refill. NowPayments' real IPN
    retry rhythm is well below this; the cap exists to bound DoS
    bursts.
    """
    app.middlewares.append(webhook_rate_limit_middleware)
    app[WEBHOOK_RATE_LIMIT_CACHE_KEY] = _LRUBucketCache(
        capacity=capacity, refill_rate=refill_rate
    )
