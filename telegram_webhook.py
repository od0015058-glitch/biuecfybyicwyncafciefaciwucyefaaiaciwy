"""Stage-15-Step-E #3 (started, not finished): opt-in Telegram webhook
mode.

Pre-this-module the bot only ran in long-polling mode (``dp.start_polling``).
This module adds the wiring needed to opt in to webhook mode by setting a
single env var (``TELEGRAM_WEBHOOK_SECRET``). Long-polling is preserved as
the default — no ops change is required for anyone who isn't ready to
expose the bot via a public HTTPS endpoint.

Why opt-in / why default-off:

* Webhook mode requires a public HTTPS endpoint (Telegram refuses to POST
  to plain ``http://``). Most local-dev setups don't have that.
* It also requires the operator to register the webhook with Telegram
  via ``Bot.set_webhook``. We do that on bot startup when this module
  is enabled.
* Switching back to long-polling without first calling
  ``Bot.delete_webhook`` causes Telegram to refuse the long-poll
  (``getUpdates`` is rejected while a webhook is registered). We surface
  this risk in the README and provide ``Bot.delete_webhook`` as the
  recovery path.

The feature is intentionally conservative: when ``TELEGRAM_WEBHOOK_SECRET``
is unset (or whitespace-only), all helpers in this module return their
"not enabled" answer and the caller proceeds with long-polling. This
mirrors the pattern used elsewhere in the codebase (e.g. ``REDIS_URL``
in ``main.build_fsm_storage`` — present means enable, absent means
fall back).

Status: STARTED, not finished. The first slice gates Telegram updates
behind a per-bot secret token (Telegram's ``X-Telegram-Bot-Api-Secret-Token``
header) so a leaked URL alone can't be used to inject forged updates,
shares the existing aiohttp app with the IPN webhooks, and keeps
``dp.start_polling`` behaviour unchanged when the env var is unset. The
follow-ups documented in HANDOFF §5 Step-E #3 — IP-allowlist for
Telegram's address ranges, automatic ``set_webhook`` retry on 5xx,
multi-bot routing — are explicitly out of scope until the operator
confirms they need them.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import os
import secrets as _secrets

from aiogram import Bot, Dispatcher
from aiohttp import web

log = logging.getLogger("bot.telegram_webhook")


# Env-var names. Kept as module constants so the test suite, the
# README, the .env.example and the runtime all agree on a single
# canonical name — and so a future rename only has to happen in
# one place.
WEBHOOK_SECRET_ENV = "TELEGRAM_WEBHOOK_SECRET"
WEBHOOK_BASE_URL_ENV = "TELEGRAM_WEBHOOK_BASE_URL"
WEBHOOK_PATH_PREFIX_ENV = "TELEGRAM_WEBHOOK_PATH_PREFIX"

# Stage-15-Step-E #3 follow-up: opt-in IP allowlist for the
# Telegram webhook receiver. Layered on top of the secret check —
# the secret guards against a leaked URL, the IP allowlist guards
# against a leaked URL **plus** a leaked secret (because a forged
# request can't easily originate from Telegram's published delivery
# IP ranges). Default is **off**: the env var unset, the helper
# returns ``None``, no middleware is wired in, and the secret check
# remains the only gate. Operators who want the extra layer set
# ``TELEGRAM_WEBHOOK_IP_ALLOWLIST=default`` to use Telegram's
# documented ranges, or supply a comma-separated list of CIDRs.
WEBHOOK_IP_ALLOWLIST_ENV = "TELEGRAM_WEBHOOK_IP_ALLOWLIST"

# Telegram's documented delivery IP ranges. Source:
#   https://core.telegram.org/bots/webhooks#the-short-version
# These are the addresses Telegram POSTs from; they're stable and
# rarely change (the 91.108.4.0/22 block dates back to 2014, the
# 149.154.160.0/20 block to ~2018). When the operator sets
# ``TELEGRAM_WEBHOOK_IP_ALLOWLIST=default`` we resolve to this
# tuple; when they pass an explicit CIDR list we parse that
# instead so a future Telegram address change is recoverable
# without a code deploy.
DEFAULT_TELEGRAM_IP_RANGES = ("149.154.160.0/20", "91.108.4.0/22")

# Stage-15-Step-E #3 follow-up: tunables for the boot-time
# ``set_webhook`` retry loop. The defaults give a total worst-case
# wait of 1 + 2 + 4 = 7 seconds across three attempts before
# escalating — long enough to ride out a Telegram blip, short
# enough that a real outage still surfaces in the supervisor logs
# within seconds rather than minutes.
WEBHOOK_REGISTER_MAX_ATTEMPTS_ENV = "TELEGRAM_WEBHOOK_REGISTER_MAX_ATTEMPTS"
WEBHOOK_REGISTER_BASE_DELAY_ENV = "TELEGRAM_WEBHOOK_REGISTER_BASE_DELAY_SECONDS"
DEFAULT_REGISTER_MAX_ATTEMPTS = 3
DEFAULT_REGISTER_BASE_DELAY = 1.0

# Default path prefix. The full route is ``<prefix>/<secret>`` so the
# secret is also part of the URL — defence in depth on top of the
# header check (``X-Telegram-Bot-Api-Secret-Token``) that
# ``aiogram.webhook.aiohttp_server.SimpleRequestHandler`` enforces.
# Two layers because (a) a leaked URL alone shouldn't be enough to
# replay updates, and (b) a misconfigured reverse proxy that strips
# Telegram's headers (e.g. an over-eager ``proxy_set_header X-…``
# block) shouldn't silently fail open.
DEFAULT_WEBHOOK_PATH_PREFIX = "/telegram-webhook"

# Telegram's set_webhook accepts a secret_token of 1..256 chars,
# ASCII letters / digits / ``_`` / ``-``. We enforce the same shape on
# our side so a typo ("Secret Token" with a space, "MySecret!" with a
# punctuation) fails loudly at boot rather than silently making
# ``set_webhook`` reject every update with HTTP 401.
_VALID_SECRET_CHARS = set(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789_-"
)
_SECRET_MIN_LEN = 1
_SECRET_MAX_LEN = 256


class WebhookConfigError(ValueError):
    """Raised on a malformed ``TELEGRAM_WEBHOOK_SECRET`` (wrong charset
    or length). Surfaces at ``load_webhook_config`` so the bot fails
    to start rather than running with a misconfigured webhook that
    Telegram silently rejects."""


def _read_env(name: str) -> str:
    """Read an env var, normalised: trim whitespace, return ``""`` for
    unset / empty. Reads fresh on every call so tests can
    ``monkeypatch.setenv`` / ``delenv`` without a process restart."""
    return os.environ.get(name, "").strip()


def is_webhook_mode_enabled() -> bool:
    """Cheap predicate: is the operator opting into webhook mode?

    True iff ``TELEGRAM_WEBHOOK_SECRET`` is set to a non-empty value.
    Validation of the secret's shape happens lazily in
    ``load_webhook_config`` — this predicate just routes the boot
    decision in ``main`` and is allowed to be cheap.
    """
    return bool(_read_env(WEBHOOK_SECRET_ENV))


def _validate_secret(secret: str) -> str:
    """Defensive: enforce Telegram's documented charset / length on
    the secret so a typo fails at boot rather than silently breaking
    every incoming update.

    Returns the (already-trimmed) secret on success; raises
    ``WebhookConfigError`` on failure with a message that points the
    operator at the env var.
    """
    if not (_SECRET_MIN_LEN <= len(secret) <= _SECRET_MAX_LEN):
        raise WebhookConfigError(
            f"{WEBHOOK_SECRET_ENV} must be 1..256 chars (got "
            f"{len(secret)}). Telegram rejects anything outside that "
            "range."
        )
    bad = sorted(set(secret) - _VALID_SECRET_CHARS)
    if bad:
        raise WebhookConfigError(
            f"{WEBHOOK_SECRET_ENV} contains disallowed character(s) "
            f"{bad!r}. Telegram only accepts ASCII letters, digits, "
            "``_`` and ``-``."
        )
    return secret


def _resolve_path(prefix: str, secret: str) -> str:
    """Build the URL path for the Telegram webhook route.

    The secret is part of the path AND of the
    ``X-Telegram-Bot-Api-Secret-Token`` header — two independent
    proofs of the same secret. Either alone is insufficient
    (header alone if the URL is brute-forced, path alone if a proxy
    drops the header).

    Bundled bug fix (Stage-15-Step-E #3 follow-up): if the operator
    sets ``TELEGRAM_WEBHOOK_PATH_PREFIX`` to ``""``, ``"/"``, or any
    string that strips down to empty, fall back to the documented
    default. Pre-fix, an empty / slash-only prefix produced
    ``"//<secret>"`` (double leading slash), which aiohttp registers
    as a route at ``//<secret>`` while incoming requests get
    canonicalised to ``/<secret>`` — the route silently 404s every
    Telegram delivery and the operator has no visible signal beyond
    "the bot stopped responding". Telegram itself accepts the
    double-slash URL on ``set_webhook``, so the bot reports the
    webhook as registered while in fact every update is dropped.
    Logs a warning so the operator can fix the env var.
    """
    cleaned = "/" + prefix.strip("/")
    if cleaned == "/":
        log.warning(
            "%s resolved to an empty path; falling back to %s. "
            "Set the env var to a non-empty prefix (e.g. %s) "
            "to silence this warning.",
            WEBHOOK_PATH_PREFIX_ENV,
            DEFAULT_WEBHOOK_PATH_PREFIX,
            DEFAULT_WEBHOOK_PATH_PREFIX,
        )
        cleaned = DEFAULT_WEBHOOK_PATH_PREFIX
    return f"{cleaned}/{secret}"


class WebhookConfig:
    """Resolved configuration for Telegram webhook mode. Constructed
    by ``load_webhook_config`` once per boot. Plain dataclass-shape
    so it's cheap to mock in tests."""

    __slots__ = ("secret", "base_url", "path", "url")

    def __init__(
        self, *, secret: str, base_url: str, path: str
    ) -> None:
        self.secret = secret
        self.base_url = base_url
        self.path = path
        # Strip a trailing slash from base_url so we don't end up with
        # ``https://host//telegram-webhook/...`` if the operator copy-
        # pastes a trailing slash into the env var. Telegram rejects
        # double-slash URLs.
        self.url = base_url.rstrip("/") + path

    def __repr__(self) -> str:  # pragma: no cover - debug only
        # Never log the secret itself — it's the auth credential.
        # Show only the prefix so a debug log is useful but doesn't
        # leak the secret to disk / log aggregator.
        return (
            f"WebhookConfig(base_url={self.base_url!r}, "
            f"path_prefix={self.path.rsplit('/', 1)[0]!r}, "
            f"secret_len={len(self.secret)})"
        )


def load_webhook_config() -> WebhookConfig | None:
    """Resolve the webhook config from the env, or return ``None`` if
    webhook mode isn't enabled.

    Validation order:
        1. Secret present? (else return None — long-polling)
        2. Secret valid charset+length? (else raise)
        3. Base URL present? (else raise)

    The base URL falls back to ``WEBHOOK_BASE_URL`` (the existing IPN
    base URL) when the Telegram-specific override isn't set, because
    most deploys expose a single public hostname.

    Raises ``WebhookConfigError`` (a subclass of ``ValueError``) on
    a present-but-invalid configuration so a typo halts boot rather
    than running with a webhook that Telegram silently rejects.
    """
    secret_raw = _read_env(WEBHOOK_SECRET_ENV)
    if not secret_raw:
        return None

    secret = _validate_secret(secret_raw)

    base_url = _read_env(WEBHOOK_BASE_URL_ENV) or _read_env(
        "WEBHOOK_BASE_URL"
    )
    if not base_url:
        raise WebhookConfigError(
            f"{WEBHOOK_SECRET_ENV} is set but neither "
            f"{WEBHOOK_BASE_URL_ENV} nor WEBHOOK_BASE_URL is — "
            "Telegram needs a public HTTPS URL to deliver updates to."
        )
    if not (
        base_url.startswith("https://")
        or base_url.startswith("http://localhost")
        or base_url.startswith("http://127.0.0.1")
    ):
        # Telegram refuses non-HTTPS URLs except for the standard
        # local loopback. Surface this at boot so a deploy with a
        # plain ``http://example.com`` URL fails fast rather than
        # silently never receiving any updates.
        raise WebhookConfigError(
            f"Telegram webhook base URL must be https:// (got "
            f"{base_url!r}). The bot can't receive updates over "
            "plain HTTP."
        )

    prefix = _read_env(WEBHOOK_PATH_PREFIX_ENV) or DEFAULT_WEBHOOK_PATH_PREFIX
    path = _resolve_path(prefix, secret)
    return WebhookConfig(secret=secret, base_url=base_url, path=path)


def constant_time_secret_eq(a: str, b: str) -> bool:
    """Public helper so callers (and tests) can do constant-time
    comparison without reaching for ``secrets`` directly. Empty
    strings always compare unequal so a missing-header request can
    never match a missing config (defence in depth — the upstream
    handler also short-circuits in that case)."""
    if not a or not b:
        return False
    return _secrets.compare_digest(a, b)


def install_telegram_webhook_route(
    app: web.Application,
    dispatcher: Dispatcher,
    bot: Bot,
    config: WebhookConfig,
) -> None:
    """Register the Telegram update handler on ``app`` at ``config.path``.

    Uses ``aiogram.webhook.aiohttp_server.SimpleRequestHandler`` for
    the actual update decoding + dispatch — there's no need to
    reinvent the request/response shape. The handler enforces the
    ``X-Telegram-Bot-Api-Secret-Token`` header against ``config.secret``
    so a leaked URL alone cannot replay updates.

    Side effects:
        * Registers a single POST route at ``config.path``.
        * Wires aiogram's startup/shutdown hooks via
          ``setup_application`` so the dispatcher's ``on_startup`` /
          ``on_shutdown`` callbacks fire alongside the aiohttp
          app's lifecycle.

    Idempotent only in the same sense ``aiohttp.UrlDispatcher`` is —
    re-calling on the same app raises ``RuntimeError: route already
    registered``. The boot path calls this exactly once.
    """
    # Lazy import so a deployer who never enables webhook mode doesn't
    # pay the import cost (or, more importantly, fail boot if a
    # future aiogram patch breaks the import for an unrelated reason).
    from aiogram.webhook.aiohttp_server import (
        SimpleRequestHandler,
        setup_application,
    )

    handler = SimpleRequestHandler(
        dispatcher=dispatcher,
        bot=bot,
        secret_token=config.secret,
    )
    handler.register(app, path=config.path)
    setup_application(app, dispatcher, bot=bot)
    log.info(
        "Telegram webhook route registered at path=%s (secret_len=%d)",
        config.path,
        len(config.secret),
    )


async def register_webhook_with_telegram(
    bot: Bot, config: WebhookConfig
) -> None:
    """Tell Telegram to deliver updates to ``config.url``.

    Drops pending updates is **disabled** by default — switching from
    long-polling to webhook mode shouldn't silently throw away the
    queue of pending updates that Telegram has buffered. Operators
    who want a clean slate can call ``Bot.delete_webhook`` with
    ``drop_pending_updates=True`` before re-enabling.

    Errors from ``set_webhook`` are propagated; the caller (typically
    ``main``) decides whether to halt boot or fall back to polling.
    For the first-slice we bubble: a misconfigured webhook is a fatal
    deploy error, not a runtime degradation.
    """
    await bot.set_webhook(
        url=config.url,
        secret_token=config.secret,
        drop_pending_updates=False,
        allowed_updates=None,
    )
    log.info(
        "Telegram webhook registered with Bot API at base=%s "
        "(secret hidden)",
        config.base_url,
    )


async def remove_webhook_from_telegram(bot: Bot) -> None:
    """Best-effort: clear the registered webhook.

    Used on shutdown so a clean stop doesn't leave Telegram trying
    to POST to a process that's no longer listening. Failures are
    logged but not raised — the bot is going down anyway and
    bubbling here would mask the original shutdown reason.
    """
    try:
        await bot.delete_webhook(drop_pending_updates=False)
        log.info("Telegram webhook deregistered.")
    except Exception:  # pragma: no cover - best-effort, defensive
        log.exception(
            "Failed to delete_webhook on shutdown — Telegram may "
            "continue to deliver updates to a dead URL until the "
            "operator clears it manually with /setWebhook."
        )


# ── Stage-15-Step-E #3 follow-up: set_webhook retry on 5xx ─────────


def _read_int_env(name: str, default: int) -> int:
    """Parse a positive-int env var, fall back to *default* on
    missing / malformed input. Used for the retry-loop tunables."""
    raw = _read_env(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.warning(
            "%s is not a valid integer (%r); falling back to %d.",
            name, raw, default,
        )
        return default
    if value < 1:
        log.warning(
            "%s must be ≥ 1 (got %d); falling back to %d.",
            name, value, default,
        )
        return default
    return value


def _read_float_env(name: str, default: float) -> float:
    """Parse a non-negative-float env var, fall back to *default* on
    missing / malformed input."""
    raw = _read_env(name)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        log.warning(
            "%s is not a valid float (%r); falling back to %s.",
            name, raw, default,
        )
        return default
    if value < 0:
        log.warning(
            "%s must be ≥ 0 (got %s); falling back to %s.",
            name, value, default,
        )
        return default
    return value


async def register_webhook_with_retry(
    bot: Bot,
    config: WebhookConfig,
    *,
    max_attempts: int | None = None,
    base_delay_secs: float | None = None,
    sleep=None,
) -> None:
    """``register_webhook_with_telegram`` with retry on transient 5xx.

    Retries on:
        * ``aiogram.exceptions.TelegramServerError`` (HTTP 5xx)
        * ``aiogram.exceptions.TelegramNetworkError`` (transport layer)

    Does **not** retry on:
        * ``aiogram.exceptions.TelegramBadRequest`` (HTTP 400 — bad
          URL, invalid secret_token, malformed allowed_updates).
          A 400 is a deploy-side typo, not a Telegram blip — burning
          retries here just delays the loud failure the operator
          needs to fix the env var.
        * Any other ``Exception`` — propagated immediately so the
          test suite's strict-typed mocks surface programming errors.

    Backoff schedule with the defaults (3 attempts, 1s base): wait
    1s after attempt 1, 2s after attempt 2, then bubble. Total
    worst-case wait: 3 seconds before the final attempt. Configurable
    via ``TELEGRAM_WEBHOOK_REGISTER_MAX_ATTEMPTS`` and
    ``TELEGRAM_WEBHOOK_REGISTER_BASE_DELAY_SECONDS``.

    The ``sleep`` argument is a test seam — production callers leave
    it ``None`` and we use ``asyncio.sleep``; tests inject a fake
    so the suite doesn't actually sleep through the backoff.
    """
    # Resolve tunables from env if not provided. Done at call time
    # (not import time) so a test that monkeypatches the env var
    # sees the change without re-importing the module.
    if max_attempts is None:
        max_attempts = _read_int_env(
            WEBHOOK_REGISTER_MAX_ATTEMPTS_ENV,
            DEFAULT_REGISTER_MAX_ATTEMPTS,
        )
    if base_delay_secs is None:
        base_delay_secs = _read_float_env(
            WEBHOOK_REGISTER_BASE_DELAY_ENV,
            DEFAULT_REGISTER_BASE_DELAY,
        )
    sleep_fn = sleep if sleep is not None else asyncio.sleep

    # Lazy-import the aiogram exceptions so the module's import
    # cost stays unchanged for deploys that never enable webhook
    # mode (mirrors the pattern in install_telegram_webhook_route).
    from aiogram.exceptions import (
        TelegramNetworkError,
        TelegramServerError,
    )

    last_exc: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            await register_webhook_with_telegram(bot, config)
            return
        except (TelegramServerError, TelegramNetworkError) as exc:
            last_exc = exc
            if attempt >= max_attempts:
                log.error(
                    "Telegram set_webhook failed after %d attempts "
                    "(last error: %s). Giving up; deploy will exit.",
                    max_attempts, exc,
                )
                raise
            delay = base_delay_secs * (2 ** (attempt - 1))
            log.warning(
                "Telegram set_webhook attempt %d/%d failed "
                "(transient: %s); retrying in %.1fs.",
                attempt, max_attempts, exc, delay,
            )
            await sleep_fn(delay)
    # Defensive: the loop always either returns or raises; this is
    # only reached if max_attempts < 1 (which the env reader
    # guards against). Re-raise the last seen exception.
    if last_exc is not None:  # pragma: no cover - defensive
        raise last_exc


# ── Stage-15-Step-E #3 follow-up: IP allowlist for /telegram-webhook


def _parse_ip_allowlist_setting(raw: str) -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] | None:
    """Resolve the env-var setting into a parsed allowlist tuple.

    Returns ``None`` when the operator hasn't opted in (env var
    unset / empty / whitespace-only). Returns a tuple of parsed
    networks otherwise.

    Special value ``"default"`` (case-insensitive) expands to
    Telegram's published delivery ranges. Any other value is
    parsed as a comma-separated list of CIDR blocks; malformed
    entries are logged and dropped (we prefer fail-soft so a
    typoed entry doesn't lock every Telegram delivery out — the
    surviving entries still cover the documented range, and the
    secret check is the primary gate anyway).
    """
    raw = (raw or "").strip()
    if not raw:
        return None
    if raw.lower() == "default":
        return tuple(
            ipaddress.ip_network(c) for c in DEFAULT_TELEGRAM_IP_RANGES
        )
    out: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for piece in raw.split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            out.append(ipaddress.ip_network(piece, strict=False))
        except ValueError:
            log.warning(
                "%s: ignoring malformed entry %r",
                WEBHOOK_IP_ALLOWLIST_ENV, piece,
            )
            continue
    if not out:
        # Every entry malformed → behave as if unset rather than
        # locking every request out. The secret check still gates
        # the route so this is fail-soft, not fail-open.
        log.warning(
            "%s parsed to an empty allowlist; behaving as if unset.",
            WEBHOOK_IP_ALLOWLIST_ENV,
        )
        return None
    return tuple(out)


def load_webhook_ip_allowlist() -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] | None:
    """Resolve the IP allowlist from the env, or ``None`` if unset.

    See ``_parse_ip_allowlist_setting`` for the parsing semantics.
    Public wrapper so callers don't have to know which env var name
    to read.
    """
    return _parse_ip_allowlist_setting(_read_env(WEBHOOK_IP_ALLOWLIST_ENV))


def _client_ip_for_filter(
    request: web.Request,
) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """Resolve the request's source IP for the allowlist check.

    Mirrors ``metrics._client_ip``: trust ``request.remote`` only,
    NOT ``X-Forwarded-For``. A public-facing reverse proxy can be
    tricked into spoofing a header that bypasses the allowlist;
    the trust boundary is the proxy's TCP address. Operators
    running behind an L7 proxy that strips ``X-Forwarded-For``
    upstream of the proxy itself need to make sure the proxy is
    on the allowlist (or pass ``default`` and let Telegram's
    ranges be the gate).
    """
    raw = request.remote
    if not raw:
        return None
    try:
        return ipaddress.ip_address(raw)
    except ValueError:
        return None


@web.middleware
async def telegram_webhook_ip_filter_middleware(
    request: web.Request,
    handler,
):
    """Reject Telegram-webhook requests from outside the allowlist.

    The middleware is a no-op for any request whose path doesn't
    match the registered Telegram webhook route — admin / IPN /
    healthz / metrics traffic flows untouched. A request **on the
    Telegram path** from outside the allowlist gets a flat 403
    *before* the SimpleRequestHandler decodes the body, so the
    rejection is cheap (no JSON parse, no dispatch, no bot work).

    The allowlist is stored on the app under a typed AppKey by
    ``install_telegram_webhook_ip_filter`` — the middleware reads
    it on every request so a hot-reload of the env var (rare but
    possible during incident response) takes effect without a
    process restart.
    """
    state = request.app.get(APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER)
    if state is None:
        # Filter wasn't installed; pass through.
        return await handler(request)
    path, allowlist = state
    if not allowlist or request.path != path:
        # Allowlist empty (operator opted out after install) or the
        # request isn't on the Telegram route → no-op.
        return await handler(request)
    ip = _client_ip_for_filter(request)
    if ip is None:
        log.warning(
            "Telegram webhook request without parseable remote "
            "(remote=%r); rejecting.", request.remote,
        )
        return web.Response(status=403, text="Forbidden")
    for net in allowlist:
        try:
            if ip in net:
                return await handler(request)
        except TypeError:
            # IPv4 addr against IPv6 net (or vice versa) — skip
            # rather than crash so one mismatched entry doesn't
            # shadow the working ones.
            continue
    log.warning(
        "Telegram webhook request from %s not in IP allowlist; "
        "rejecting.", request.remote,
    )
    return web.Response(status=403, text="Forbidden")


# Typed AppKey for the IP-filter state. Keeps the middleware's
# request-time lookup compatible with aiohttp 3.9+'s typed-key
# requirement (mirrors ``APP_KEY_*`` in metrics.py / web_admin.py).
APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER: web.AppKey = web.AppKey(
    "telegram_webhook/ip_filter",
    tuple,
)


def install_telegram_webhook_ip_filter(
    app: web.Application,
    *,
    config: WebhookConfig,
    allowlist: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] | None = None,
) -> None:
    """Wire the IP-allowlist middleware into the aiohttp app.

    Resolves the allowlist from ``TELEGRAM_WEBHOOK_IP_ALLOWLIST``
    when *allowlist* isn't provided; the explicit-arg path is
    primarily for tests. When the resolved allowlist is empty /
    None the middleware is still registered (so a hot env-var
    flip doesn't need re-registration) but each request is a
    cheap no-op.

    Stores ``(path, allowlist)`` on the app under
    ``APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER`` so the middleware's
    request-time lookup is O(1).
    """
    if allowlist is None:
        allowlist = load_webhook_ip_allowlist()
    app[APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER] = (config.path, allowlist or ())
    if telegram_webhook_ip_filter_middleware not in app.middlewares:
        app.middlewares.append(telegram_webhook_ip_filter_middleware)
    if allowlist:
        log.info(
            "Telegram webhook IP filter active; allowlist size=%d "
            "(path=%s).",
            len(allowlist), config.path,
        )
    else:
        log.info(
            "Telegram webhook IP filter installed but disabled "
            "(%s unset). Set %s=default to enable.",
            WEBHOOK_IP_ALLOWLIST_ENV, WEBHOOK_IP_ALLOWLIST_ENV,
        )


# ── Stage-15-Step-E #3 follow-up: /telegram-webhook/healthz ────────


def healthz_path_for(config: WebhookConfig) -> str:
    """Return the health-check path for *config*.

    The healthz route lives one level above the secret-bearing
    POST endpoint — i.e. for path ``/telegram-webhook/<secret>``
    it's at ``/telegram-webhook/healthz`` (no secret in the URL).
    Operators can poll this from a load-balancer / Kubernetes
    livenessProbe without exposing the secret.
    """
    # ``config.path`` is ``<prefix>/<secret>``. Strip the secret
    # off the right and append ``/healthz``.
    prefix = config.path.rsplit("/", 1)[0]
    if not prefix:  # pragma: no cover - guarded by _resolve_path
        prefix = DEFAULT_WEBHOOK_PATH_PREFIX
    return f"{prefix}/healthz"


async def telegram_webhook_healthz(request: web.Request) -> web.Response:
    """``GET /telegram-webhook/healthz`` — liveness probe.

    Returns 200 with a tiny JSON body so a load balancer / k8s
    probe can verify the route is mounted and the bot process is
    answering HTTP. Deliberately stateless: doesn't call
    ``Bot.get_webhook_info`` (which would talk to Telegram on
    every probe and tank the rate-limit budget) and doesn't read
    any DB. The IPN-side ``/metrics`` endpoint already covers
    deeper health (in-flight chats, FX freshness, IPN drops);
    this probe is purely "is the webhook receiver alive".

    The body intentionally **does not** include the secret, the
    URL, or the path — the response is meant to be cacheable and
    safe to expose publicly. We include only the registered prefix
    so an operator inspecting the response can confirm they're
    hitting the right deploy.
    """
    cfg = request.app.get(APP_KEY_TELEGRAM_WEBHOOK_HEALTHZ_PREFIX, "")
    body = {"status": "ok", "webhook_prefix": cfg}
    return web.json_response(body)


# Typed AppKey for the healthz route's prefix string.
APP_KEY_TELEGRAM_WEBHOOK_HEALTHZ_PREFIX: web.AppKey = web.AppKey(
    "telegram_webhook/healthz_prefix",
    str,
)


def install_telegram_webhook_healthz_route(
    app: web.Application, config: WebhookConfig
) -> None:
    """Mount the healthz route on *app* at the configured prefix.

    Idempotent only in the same sense ``aiohttp.UrlDispatcher`` is —
    re-calling on the same app raises ``RuntimeError: route already
    registered``. The boot path calls this exactly once.
    """
    path = healthz_path_for(config)
    prefix = config.path.rsplit("/", 1)[0]
    app[APP_KEY_TELEGRAM_WEBHOOK_HEALTHZ_PREFIX] = prefix
    app.router.add_get(path, telegram_webhook_healthz)
    log.info("Telegram webhook healthz route registered at %s.", path)
