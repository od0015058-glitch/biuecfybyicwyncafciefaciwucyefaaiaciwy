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
    """
    cleaned = "/" + prefix.strip("/")
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
