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


# Environment variable that opts the rate-limit keying into trusting
# the X-Forwarded-For header. See ``client_ip_for_rate_limit`` for
# the full discussion. Default is OFF (trust only ``request.remote``)
# so a direct-exposure deploy stays safe.
TRUST_PROXY_HEADERS_ENV = "TRUST_PROXY_HEADERS"


def _is_trusting_proxy() -> bool:
    """Read ``TRUST_PROXY_HEADERS`` from the process env. Truthy values
    are ``1``, ``true``, ``yes`` (case-insensitive); everything else
    is False.

    Read fresh on every call so tests can ``monkeypatch.setenv`` /
    ``delenv`` without restarting the process. Cheap — it's just
    ``os.environ.get``.
    """
    import os

    return os.environ.get(TRUST_PROXY_HEADERS_ENV, "").strip().lower() in {
        "1", "true", "yes", "on",
    }


def client_ip_for_rate_limit(request: web.Request) -> str:
    """Pick a rate-limit bucket key for ``request``.

    The default ``request.remote`` is the TCP peer, which in every
    production deploy of this bot is the reverse proxy (Cloudflare
    Tunnel, nginx, Caddy) — NOT the real client. Bucketing all
    admin-panel traffic onto one proxy IP turns "per-IP login
    throttle" from a brute-force defence into either (a) a no-op
    (the tunnel IP is fine, the single bucket never drains) or
    (b) a self-DoS (one attacker spamming the bucket locks out
    every legitimate admin behind the same tunnel).

    When ``TRUST_PROXY_HEADERS=1`` is set in the env, prefer the
    leftmost IP in ``X-Forwarded-For`` — that's the convention the
    major CDNs and reverse proxies all follow. We explicitly don't
    trust the header by default because on a direct-to-internet
    deploy an attacker could inject a spoofed header to evade the
    per-IP limiter entirely.

    Returns a non-empty string; falls back to ``request.path`` +
    ``"_unknown_"`` as a last resort so the limiter key is always
    stable.
    """
    if _is_trusting_proxy():
        xff = request.headers.get("X-Forwarded-For", "")
        if xff:
            # Leftmost entry is the original client; everything
            # after is the proxy chain. Strip whitespace that some
            # proxies insert.
            first = xff.split(",", 1)[0].strip()
            if first:
                return first
    return request.remote or request.path or "_unknown_"


# ---------------------------------------------------------------------
# /admin/login per-IP throttle
# ---------------------------------------------------------------------
#
# Keyed by client IP (via ``client_ip_for_rate_limit``), so a Cloudflare
# tunnel deploy doesn't bucket every admin-panel visitor onto one IP
# the moment ``TRUST_PROXY_HEADERS=1`` is set. Separate from both the
# chat limiter and the webhook limiter because a password-guessing
# attacker is a fundamentally different traffic shape from either —
# slow, persistent, and deserving of its own budget.
#
# Defaults: 10-token burst with a 1 token / 30 sec refill. A
# password-spraying attacker from one IP can therefore check ~10
# passwords immediately, then only ~1 every 30 seconds. Combined
# with ``ADMIN_PASSWORD`` being a random 32-char string, this makes
# brute-force infeasible.
LOGIN_RATE_LIMIT_CACHE_KEY: web.AppKey = web.AppKey(
    "_login_rate_limit_cache", _LRUBucketCache
)


def install_login_rate_limit(
    app: web.Application,
    capacity: float = 10.0,
    refill_rate: float = 1.0 / 30.0,
) -> None:
    """Pre-seed the per-IP login rate-limit cache on *app*.

    Called from ``web_admin.setup_admin_routes`` so the admin mount
    gets one bucket cache regardless of how many times the login
    route is hit. Idempotent by design — re-running on the same
    app replaces the cache (useful for tests).
    """
    app[LOGIN_RATE_LIMIT_CACHE_KEY] = _LRUBucketCache(
        capacity=capacity, refill_rate=refill_rate
    )


async def consume_login_token(app: web.Application, client_key: str) -> bool:
    """Try to take one login-attempt token for ``client_key``.

    Returns True if the login attempt should proceed, False if the
    handler should short-circuit with 429.

    Takes the app + key directly (rather than pulling from the
    request) so ``login_post`` can key on the sanitised client IP
    while remaining cheap to unit-test.
    """
    cache = app.get(LOGIN_RATE_LIMIT_CACHE_KEY)
    if cache is None:
        # Not installed — fail open rather than denying all logins
        # from a misconfigured deploy. ``install_login_rate_limit``
        # is called from ``setup_admin_routes`` so this branch is
        # only reachable in tests that mount routes manually.
        return True
    return await cache.consume(client_key)


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

    # Use the shared client-IP helper so a reverse-proxy deploy with
    # ``TRUST_PROXY_HEADERS=1`` actually buckets per real client IP
    # rather than collapsing every IPN onto the proxy's TCP address.
    # For webhook traffic specifically this matters less than for
    # login (NowPayments sends from a known range, the cap is much
    # higher than the real IPN rhythm) but using the helper keeps
    # the two limiters in lock-step — when the env var flips, both
    # limiters gain real-client granularity at once.
    key = client_ip_for_rate_limit(request)
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
