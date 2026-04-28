"""Rate limiting primitives.

Two consumers:

* ``consume_chat_token(user_id)`` called from the AI-chat handler —
  bounds how fast a single Telegram user can fire prompts at OpenRouter
  (cost control + DoS). Deliberately NOT a dispatcher-wide middleware,
  because that would also throttle ``/start``, ``waiting_custom_amount``
  input, promo-code input, and reply-keyboard handlers — none of which
  cost OpenRouter money.
* ``webhook_rate_limit_middleware`` mounted on the aiohttp app for the
  NowPayments IPN endpoint — per-IP DoS defence. The legitimate IPN
  retry rhythm is well under any sane cap.

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
from typing import Awaitable, Callable, Hashable

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


# Module-level chat limiter. Defaults: 5 message tokens, refilling at
# 1/sec — burst 5 prompts, then ~1/sec sustained. Tweak via
# ``configure_chat_rate_limiter()`` at startup if you need different
# caps. Single shared instance because the bot is single-process.
_chat_rate_limiter: _LRUBucketCache = _LRUBucketCache(
    capacity=5.0, refill_rate=1.0
)


def configure_chat_rate_limiter(
    capacity: float = 5.0, refill_rate: float = 1.0
) -> None:
    """Replace the module-level chat limiter with one configured to
    the given capacity / refill_rate. Call at startup BEFORE any
    request hits ``consume_chat_token``. Mostly useful for tests."""
    global _chat_rate_limiter
    _chat_rate_limiter = _LRUBucketCache(
        capacity=capacity, refill_rate=refill_rate
    )


async def consume_chat_token(user_id: int) -> bool:
    """Try to take 1 chat token for this Telegram user.

    Returns True if the prompt should proceed, False if it should be
    short-circuited with a "slow down" message. Call this at the very
    top of the AI-chat handler ONLY — never on commands, FSM states,
    or callback queries, because those don't cost OpenRouter money
    and shouldn't be throttled.

    Why this is a function and not a middleware: an inner middleware
    on ``dp.message`` would fire for *every* matched message handler
    (``/start``, ``waiting_custom_amount``, promo input, the legacy
    reply-keyboard buttons), not just the AI catch-all. Putting the
    check inside the chat handler scopes the throttle to the path
    that actually costs money.
    """
    return await _chat_rate_limiter.consume(user_id)


# The NowPayments IPN path. Exported so tests — and the middleware —
# agree on the single URL whose traffic this limiter is meant to bound.
# Kept as a module-level constant rather than inlined so a future
# rename of the endpoint only has to change in one place.
WEBHOOK_PATH = "/nowpayments-webhook"


@web.middleware
async def webhook_rate_limit_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    """Per-IP token bucket on the NowPayments IPN endpoint only.

    Reasonable cap: 30 tokens, refilling at 5/sec. Well above
    NowPayments' real IPN retry rhythm (a handful per minute per
    payment) but bounds DoS bursts. We rate-limit BEFORE the body is
    read so a flood of 1MB POSTs can't pin the loop.

    Only the webhook path is rate-limited. Other routes mounted on the
    same aiohttp app — notably the Stage-8 web admin panel under
    ``/admin/`` — pass through untouched. Previously this middleware
    consumed a token for **every** request, so an admin browsing the
    panel (or the broadcast-progress page polling for status, added in
    Stage-8-Part-5) could exhaust the bucket and lock NowPayments IPNs
    out of the webhook — or, conversely, a legitimate IPN burst could
    throttle the admin UI. The scope is now narrowed to the endpoint
    whose DoS exposure this limiter was designed to defend.

    The bucket cache lives on the application itself so it survives
    request boundaries.
    """
    if request.path != WEBHOOK_PATH:
        return await handler(request)

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
