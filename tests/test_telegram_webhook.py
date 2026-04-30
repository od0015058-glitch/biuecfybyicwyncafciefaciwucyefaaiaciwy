"""Tests for ``telegram_webhook`` (Stage-15-Step-E #3, opt-in
webhook mode) + the bundled rate-limit fix that extends the
per-IP token bucket to TetraPay (and the new Telegram webhook
path)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from telegram_webhook import (
    DEFAULT_WEBHOOK_PATH_PREFIX,
    WEBHOOK_BASE_URL_ENV,
    WEBHOOK_PATH_PREFIX_ENV,
    WEBHOOK_SECRET_ENV,
    WebhookConfig,
    WebhookConfigError,
    constant_time_secret_eq,
    install_telegram_webhook_route,
    is_webhook_mode_enabled,
    load_webhook_config,
    register_webhook_with_telegram,
    remove_webhook_from_telegram,
)


# ---------------------------------------------------------------------
# is_webhook_mode_enabled / load_webhook_config — env validation
# ---------------------------------------------------------------------


def test_disabled_when_env_unset(monkeypatch):
    for env in (
        WEBHOOK_SECRET_ENV,
        WEBHOOK_BASE_URL_ENV,
        WEBHOOK_PATH_PREFIX_ENV,
        "WEBHOOK_BASE_URL",
    ):
        monkeypatch.delenv(env, raising=False)
    assert is_webhook_mode_enabled() is False
    assert load_webhook_config() is None


def test_disabled_when_env_whitespace_only(monkeypatch):
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "   ")
    assert is_webhook_mode_enabled() is False
    assert load_webhook_config() is None


def test_load_webhook_config_happy_path(monkeypatch):
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "abc123-XYZ_secret")
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://example.com")
    monkeypatch.delenv(WEBHOOK_BASE_URL_ENV, raising=False)
    monkeypatch.delenv(WEBHOOK_PATH_PREFIX_ENV, raising=False)

    cfg = load_webhook_config()
    assert cfg is not None
    assert cfg.secret == "abc123-XYZ_secret"
    assert cfg.base_url == "https://example.com"
    assert cfg.path == "/telegram-webhook/abc123-XYZ_secret"
    assert cfg.url == "https://example.com/telegram-webhook/abc123-XYZ_secret"


def test_load_webhook_config_telegram_specific_url_overrides_generic(
    monkeypatch,
):
    """``TELEGRAM_WEBHOOK_BASE_URL`` lets ops point Telegram at a
    different hostname (e.g. a separate subdomain or a port-forward)
    without disturbing the IPN base URL."""
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "secret_xyz")
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://ipns.example.com")
    monkeypatch.setenv(
        WEBHOOK_BASE_URL_ENV, "https://tg.example.com"
    )
    cfg = load_webhook_config()
    assert cfg is not None
    assert cfg.base_url == "https://tg.example.com"


def test_load_webhook_config_strips_trailing_slash_from_base_url(
    monkeypatch,
):
    """A trailing slash on the env value would produce a double-
    slashed URL (``https://x.com//telegram-webhook/...``) — Telegram
    rejects double slashes, so the constructor strips."""
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "secret_xyz")
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://example.com/")
    cfg = load_webhook_config()
    assert cfg is not None
    assert "//" not in cfg.url.split("://", 1)[1]


def test_load_webhook_config_custom_path_prefix(monkeypatch):
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "secret_xyz")
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://example.com")
    monkeypatch.setenv(WEBHOOK_PATH_PREFIX_ENV, "custom/inbound")
    cfg = load_webhook_config()
    assert cfg is not None
    assert cfg.path == "/custom/inbound/secret_xyz"
    assert cfg.url == "https://example.com/custom/inbound/secret_xyz"


def test_load_webhook_config_default_path_prefix_is_documented(
    monkeypatch,
):
    """Pin the default prefix so a future rename can't silently
    invalidate every operator's deployed Telegram webhook URL."""
    assert DEFAULT_WEBHOOK_PATH_PREFIX == "/telegram-webhook"


def test_load_webhook_config_rejects_missing_base_url(monkeypatch):
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "secret_xyz")
    monkeypatch.delenv("WEBHOOK_BASE_URL", raising=False)
    monkeypatch.delenv(WEBHOOK_BASE_URL_ENV, raising=False)
    with pytest.raises(WebhookConfigError):
        load_webhook_config()


def test_load_webhook_config_rejects_plain_http_base_url(monkeypatch):
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "secret_xyz")
    monkeypatch.setenv("WEBHOOK_BASE_URL", "http://example.com")
    with pytest.raises(WebhookConfigError):
        load_webhook_config()


def test_load_webhook_config_allows_localhost_http_for_dev(monkeypatch):
    """Telegram refuses non-HTTPS in production but the standard
    ``http://localhost`` and ``http://127.0.0.1`` are conventions
    we let through for local CI/dev. (In real life Telegram still
    can't reach localhost, but the boot-time validator shouldn't
    fight a deployer who's stubbing things out.)"""
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "secret_xyz")
    monkeypatch.setenv(
        "WEBHOOK_BASE_URL", "http://localhost:8080"
    )
    cfg = load_webhook_config()
    assert cfg is not None


def test_load_webhook_config_rejects_secret_with_spaces(monkeypatch):
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "bad secret")
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://example.com")
    with pytest.raises(WebhookConfigError):
        load_webhook_config()


def test_load_webhook_config_rejects_secret_with_punctuation(monkeypatch):
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "bad!secret")
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://example.com")
    with pytest.raises(WebhookConfigError):
        load_webhook_config()


def test_load_webhook_config_rejects_oversize_secret(monkeypatch):
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "x" * 257)
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://example.com")
    with pytest.raises(WebhookConfigError):
        load_webhook_config()


def test_webhook_config_repr_does_not_leak_secret():
    cfg = WebhookConfig(
        secret="my-very-secret-value",
        base_url="https://example.com",
        path="/telegram-webhook/my-very-secret-value",
    )
    rendered = repr(cfg)
    assert "my-very-secret-value" not in rendered
    assert "secret_len=20" in rendered


# ---------------------------------------------------------------------
# constant_time_secret_eq
# ---------------------------------------------------------------------


def test_constant_time_secret_eq_returns_true_on_match():
    assert constant_time_secret_eq("abc", "abc") is True


def test_constant_time_secret_eq_returns_false_on_mismatch():
    assert constant_time_secret_eq("abc", "abd") is False


def test_constant_time_secret_eq_handles_empty_input():
    """An empty string must NEVER match (defence in depth — a
    request that arrived with no header should always fail
    secret-check, even against an empty configured secret)."""
    assert constant_time_secret_eq("", "") is False
    assert constant_time_secret_eq("", "abc") is False
    assert constant_time_secret_eq("abc", "") is False


# ---------------------------------------------------------------------
# install_telegram_webhook_route
# ---------------------------------------------------------------------


def test_install_telegram_webhook_route_registers_post_route():
    """Smoke test: after install, the aiohttp router has a POST
    handler at the configured path."""
    app = web.Application()
    cfg = WebhookConfig(
        secret="testsecret",
        base_url="https://example.com",
        path="/telegram-webhook/testsecret",
    )
    bot = MagicMock()
    bot.session = MagicMock()
    bot.session.close = AsyncMock()
    dp = MagicMock()
    dp.workflow_data = {}
    install_telegram_webhook_route(app, dispatcher=dp, bot=bot, config=cfg)

    paths = [
        (r.method, r.resource.canonical)
        for r in app.router.routes()
        if hasattr(r.resource, "canonical")
    ]
    # Two routes get registered by SimpleRequestHandler (POST + an
    # OPTIONS / HEAD aiohttp adds in some versions). The POST is the
    # one we care about.
    assert any(
        method == "POST" and path == "/telegram-webhook/testsecret"
        for method, path in paths
    ), f"POST route missing, saw {paths!r}"


# ---------------------------------------------------------------------
# register_webhook_with_telegram / remove_webhook_from_telegram
# ---------------------------------------------------------------------


async def test_register_webhook_calls_set_webhook_with_secret_token():
    cfg = WebhookConfig(
        secret="testsecret",
        base_url="https://example.com",
        path="/telegram-webhook/testsecret",
    )
    bot = MagicMock()
    bot.set_webhook = AsyncMock()
    await register_webhook_with_telegram(bot, cfg)
    bot.set_webhook.assert_awaited_once()
    kwargs = bot.set_webhook.await_args.kwargs
    assert kwargs["url"] == "https://example.com/telegram-webhook/testsecret"
    assert kwargs["secret_token"] == "testsecret"
    assert kwargs["drop_pending_updates"] is False


async def test_remove_webhook_swallows_errors_on_shutdown(caplog):
    """Best-effort delete: if Telegram is unreachable on shutdown
    we must NOT raise — the bot is going down anyway and bubbling
    the exception would mask whatever caused the shutdown."""
    bot = MagicMock()
    bot.delete_webhook = AsyncMock(side_effect=RuntimeError("network"))
    # Should not raise.
    await remove_webhook_from_telegram(bot)


# ---------------------------------------------------------------------
# Bundled bug fix: extend webhook_rate_limit_middleware to TetraPay
# ---------------------------------------------------------------------


def test_default_rate_limited_paths_includes_tetrapay():
    """Pin the bug fix: TETRAPAY_WEBHOOK_PATH must be in the default
    set so a fresh ``install_webhook_rate_limit(app)`` call protects
    the TetraPay endpoint without requiring the caller to opt in."""
    from rate_limit import (
        TETRAPAY_WEBHOOK_PATH,
        WEBHOOK_PATH,
        _DEFAULT_RATE_LIMITED_PATHS,
    )

    assert TETRAPAY_WEBHOOK_PATH == "/tetrapay-webhook"
    assert TETRAPAY_WEBHOOK_PATH in _DEFAULT_RATE_LIMITED_PATHS
    assert WEBHOOK_PATH in _DEFAULT_RATE_LIMITED_PATHS


def test_install_webhook_rate_limit_seeds_path_set():
    from rate_limit import (
        TETRAPAY_WEBHOOK_PATH,
        WEBHOOK_PATH,
        WEBHOOK_RATE_LIMITED_PATHS_KEY,
        install_webhook_rate_limit,
    )

    app = web.Application()
    install_webhook_rate_limit(app)
    paths = app[WEBHOOK_RATE_LIMITED_PATHS_KEY]
    assert WEBHOOK_PATH in paths
    assert TETRAPAY_WEBHOOK_PATH in paths


def test_register_rate_limited_webhook_path_extends_set():
    """The Telegram webhook helper passes its dynamic
    ``/telegram-webhook/<secret>`` path through this hook so the
    rate limiter covers it too."""
    from rate_limit import (
        WEBHOOK_RATE_LIMITED_PATHS_KEY,
        install_webhook_rate_limit,
        register_rate_limited_webhook_path,
    )

    app = web.Application()
    install_webhook_rate_limit(app)
    register_rate_limited_webhook_path(app, "/telegram-webhook/abc")
    assert "/telegram-webhook/abc" in app[WEBHOOK_RATE_LIMITED_PATHS_KEY]


def test_register_rate_limited_webhook_path_is_idempotent():
    from rate_limit import (
        WEBHOOK_RATE_LIMITED_PATHS_KEY,
        install_webhook_rate_limit,
        register_rate_limited_webhook_path,
    )

    app = web.Application()
    install_webhook_rate_limit(app)
    register_rate_limited_webhook_path(app, "/telegram-webhook/abc")
    register_rate_limited_webhook_path(app, "/telegram-webhook/abc")
    paths = app[WEBHOOK_RATE_LIMITED_PATHS_KEY]
    matches = [p for p in paths if p == "/telegram-webhook/abc"]
    assert len(matches) == 1


def test_register_rate_limited_webhook_path_raises_before_install():
    """Calling the helper before ``install_webhook_rate_limit`` runs
    is a bug — surface it loudly so the misordered boot path can be
    fixed rather than silently failing closed."""
    from rate_limit import register_rate_limited_webhook_path

    app = web.Application()
    with pytest.raises(RuntimeError):
        register_rate_limited_webhook_path(app, "/telegram-webhook/abc")


async def test_rate_limit_middleware_now_filters_tetrapay():
    """Regression test for the bundled bug fix: a request to
    ``/tetrapay-webhook`` now consumes a token from the same bucket
    as ``/nowpayments-webhook``. Pre-fix the request would have
    bypassed the limiter entirely (no token consumed, no 429 ever
    returned)."""
    from rate_limit import (
        install_webhook_rate_limit,
        webhook_rate_limit_middleware,
    )

    app = web.Application()
    install_webhook_rate_limit(app, capacity=1, refill_rate=0.0001)

    async def fake_handler(_req: web.Request) -> web.Response:
        return web.Response(text="ok")

    # First TetraPay request consumes the token, second is rate-limited.
    req1 = make_mocked_request(
        "POST", "/tetrapay-webhook", app=app
    )
    req2 = make_mocked_request(
        "POST", "/tetrapay-webhook", app=app
    )

    resp1 = await webhook_rate_limit_middleware(req1, fake_handler)
    resp2 = await webhook_rate_limit_middleware(req2, fake_handler)
    assert resp1.status == 200
    assert resp2.status == 429


async def test_rate_limit_middleware_still_passes_through_admin_traffic():
    """Pre-existing invariant must survive the refactor: admin-panel
    requests are NOT rate-limited (the bucket is webhook-only)."""
    from rate_limit import (
        install_webhook_rate_limit,
        webhook_rate_limit_middleware,
    )

    app = web.Application()
    install_webhook_rate_limit(app, capacity=1, refill_rate=0.0001)

    async def fake_handler(_req: web.Request) -> web.Response:
        return web.Response(text="ok")

    # Burn many admin requests — none should consume from the bucket.
    for _ in range(50):
        req = make_mocked_request("GET", "/admin/", app=app)
        resp = await webhook_rate_limit_middleware(req, fake_handler)
        assert resp.status == 200
