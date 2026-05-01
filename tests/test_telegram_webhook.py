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


# ---------------------------------------------------------------------
# Stage-15-Step-E #3 follow-up: bundled bug fix in _resolve_path
# ---------------------------------------------------------------------


def test_resolve_path_falls_back_when_prefix_strips_to_empty(caplog):
    """Bundled bug fix: an empty / slash-only prefix used to produce
    ``//<secret>`` (double slash) which aiohttp registers as a route
    that doesn't match incoming canonical ``/<secret>`` requests.
    Now we fall back to the documented default and log a warning so
    the operator can fix the env var."""
    from telegram_webhook import _resolve_path

    caplog.set_level("WARNING", logger="bot.telegram_webhook")
    # An empty prefix strips to "" which would build "//secret".
    assert _resolve_path("", "abc") == f"{DEFAULT_WEBHOOK_PATH_PREFIX}/abc"
    # Slash-only prefix has the same defect.
    assert _resolve_path("/", "abc") == f"{DEFAULT_WEBHOOK_PATH_PREFIX}/abc"
    # Multi-slash prefix likewise.
    assert _resolve_path("///", "abc") == f"{DEFAULT_WEBHOOK_PATH_PREFIX}/abc"
    # Warning was logged each time so the operator notices.
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) >= 3
    for record in warnings:
        assert WEBHOOK_PATH_PREFIX_ENV in record.getMessage()


def test_resolve_path_does_not_warn_for_explicit_prefix(caplog):
    """The fallback only kicks in for a degenerate empty prefix —
    a normal explicit prefix must NOT log a warning (otherwise the
    log spam would defeat the point of having a documented default)."""
    from telegram_webhook import _resolve_path

    caplog.set_level("WARNING", logger="bot.telegram_webhook")
    assert _resolve_path("/telegram-webhook", "abc") == "/telegram-webhook/abc"
    assert _resolve_path("custom-path", "xyz") == "/custom-path/xyz"
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert warnings == []


def test_load_webhook_config_normalises_empty_prefix(monkeypatch):
    """Wired-up regression for the bundled bug fix:
    ``TELEGRAM_WEBHOOK_PATH_PREFIX="/"`` no longer results in a
    double-slash path."""
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "validsecret")
    monkeypatch.setenv(WEBHOOK_BASE_URL_ENV, "https://example.com")
    monkeypatch.setenv(WEBHOOK_PATH_PREFIX_ENV, "/")
    cfg = load_webhook_config()
    assert cfg is not None
    assert cfg.path == f"{DEFAULT_WEBHOOK_PATH_PREFIX}/validsecret"
    assert "//" not in cfg.url.split("://", 1)[1]


# ---------------------------------------------------------------------
# Stage-15-Step-E #3 follow-up: register_webhook_with_retry
# ---------------------------------------------------------------------


@pytest.fixture
def _retry_cfg():
    return WebhookConfig(
        secret="retrysecret",
        base_url="https://example.com",
        path="/telegram-webhook/retrysecret",
    )


async def test_register_with_retry_succeeds_on_first_attempt(_retry_cfg):
    """Happy path: ``set_webhook`` succeeds on the first try, no
    retries occur, no sleep is invoked."""
    from telegram_webhook import register_webhook_with_retry

    bot = MagicMock()
    bot.set_webhook = AsyncMock(return_value=True)
    sleeps: list[float] = []
    async def fake_sleep(secs: float) -> None:
        sleeps.append(secs)

    await register_webhook_with_retry(
        bot, _retry_cfg, max_attempts=3, base_delay_secs=1.0,
        sleep=fake_sleep,
    )
    assert bot.set_webhook.await_count == 1
    assert sleeps == []


async def test_register_with_retry_recovers_from_transient_5xx(
    _retry_cfg, caplog,
):
    """A ``TelegramServerError`` on the first attempt is retried
    after the configured backoff; a subsequent success exits the
    loop."""
    from aiogram.exceptions import TelegramServerError
    from telegram_webhook import register_webhook_with_retry

    caplog.set_level("WARNING", logger="bot.telegram_webhook")
    method = MagicMock(__name__="setWebhook")
    bot = MagicMock()
    call_log = []
    async def flaky_set_webhook(**kwargs):
        call_log.append(kwargs)
        if len(call_log) == 1:
            raise TelegramServerError(method=method, message="503 Service Unavailable")
        return True
    bot.set_webhook = flaky_set_webhook
    sleeps: list[float] = []
    async def fake_sleep(secs: float) -> None:
        sleeps.append(secs)

    await register_webhook_with_retry(
        bot, _retry_cfg, max_attempts=3, base_delay_secs=1.0,
        sleep=fake_sleep,
    )
    assert len(call_log) == 2
    # First retry: 1s backoff.
    assert sleeps == [1.0]
    # Warning about the transient retry was logged.
    assert any(
        "transient" in r.getMessage() for r in caplog.records
    )


async def test_register_with_retry_uses_exponential_backoff(_retry_cfg):
    """Sleep durations follow the documented 1s / 2s / 4s schedule
    for max_attempts=3, base=1.0."""
    from aiogram.exceptions import TelegramServerError
    from telegram_webhook import register_webhook_with_retry

    method = MagicMock(__name__="setWebhook")
    bot = MagicMock()
    bot.set_webhook = AsyncMock(
        side_effect=TelegramServerError(method=method, message="503"),
    )
    sleeps: list[float] = []
    async def fake_sleep(secs: float) -> None:
        sleeps.append(secs)

    with pytest.raises(TelegramServerError):
        await register_webhook_with_retry(
            bot, _retry_cfg, max_attempts=3, base_delay_secs=1.0,
            sleep=fake_sleep,
        )
    # 3 attempts → 2 backoffs (after attempt 1 and 2).
    assert sleeps == [1.0, 2.0]
    assert bot.set_webhook.await_count == 3


async def test_register_with_retry_does_not_retry_400(_retry_cfg):
    """``TelegramBadRequest`` is a deploy-side typo (bad URL,
    invalid secret_token shape). Burning retries on it just delays
    the loud failure; we must propagate the error immediately."""
    from aiogram.exceptions import TelegramBadRequest
    from telegram_webhook import register_webhook_with_retry

    method = MagicMock(__name__="setWebhook")
    bot = MagicMock()
    bot.set_webhook = AsyncMock(
        side_effect=TelegramBadRequest(method=method, message="bad url"),
    )
    sleeps: list[float] = []
    async def fake_sleep(secs: float) -> None:
        sleeps.append(secs)

    with pytest.raises(TelegramBadRequest):
        await register_webhook_with_retry(
            bot, _retry_cfg, max_attempts=5, base_delay_secs=1.0,
            sleep=fake_sleep,
        )
    # No retries — single attempt.
    assert bot.set_webhook.await_count == 1
    assert sleeps == []


async def test_register_with_retry_recovers_from_network_error(_retry_cfg):
    """A transport-layer error (DNS hiccup, TCP reset) is also
    retried — distinct from a 5xx but equally transient."""
    from aiogram.exceptions import TelegramNetworkError
    from telegram_webhook import register_webhook_with_retry

    bot = MagicMock()
    call_log = []
    async def flaky(**kwargs):
        call_log.append(kwargs)
        if len(call_log) == 1:
            raise TelegramNetworkError(
                method=MagicMock(__name__="setWebhook"),
                message="connection reset",
            )
        return True
    bot.set_webhook = flaky
    sleeps: list[float] = []
    async def fake_sleep(secs: float) -> None:
        sleeps.append(secs)

    await register_webhook_with_retry(
        bot, _retry_cfg, max_attempts=3, base_delay_secs=0.5,
        sleep=fake_sleep,
    )
    assert len(call_log) == 2
    assert sleeps == [0.5]


async def test_register_with_retry_reads_max_attempts_from_env(
    _retry_cfg, monkeypatch,
):
    """Env override is read at call time so a test (or a future
    operator hot-reload) can change the cap without re-importing."""
    from aiogram.exceptions import TelegramServerError
    from telegram_webhook import (
        WEBHOOK_REGISTER_BASE_DELAY_ENV,
        WEBHOOK_REGISTER_MAX_ATTEMPTS_ENV,
        register_webhook_with_retry,
    )

    monkeypatch.setenv(WEBHOOK_REGISTER_MAX_ATTEMPTS_ENV, "2")
    monkeypatch.setenv(WEBHOOK_REGISTER_BASE_DELAY_ENV, "0.0")
    method = MagicMock(__name__="setWebhook")
    bot = MagicMock()
    bot.set_webhook = AsyncMock(
        side_effect=TelegramServerError(method=method, message="503"),
    )
    async def fake_sleep(_secs: float) -> None:
        return None

    with pytest.raises(TelegramServerError):
        await register_webhook_with_retry(
            bot, _retry_cfg, sleep=fake_sleep,
        )
    # max_attempts=2 from env → exactly 2 attempts.
    assert bot.set_webhook.await_count == 2


def test_int_env_helper_falls_back_on_garbage(monkeypatch):
    """Defence-in-depth: a malformed env value falls back to the
    default rather than crashing boot."""
    from telegram_webhook import _read_int_env

    monkeypatch.setenv("FOO_INT", "not-a-number")
    assert _read_int_env("FOO_INT", 7) == 7
    monkeypatch.setenv("FOO_INT", "0")
    assert _read_int_env("FOO_INT", 7) == 7
    monkeypatch.setenv("FOO_INT", "-5")
    assert _read_int_env("FOO_INT", 7) == 7
    monkeypatch.delenv("FOO_INT", raising=False)
    assert _read_int_env("FOO_INT", 7) == 7
    monkeypatch.setenv("FOO_INT", "12")
    assert _read_int_env("FOO_INT", 7) == 12


# ---------------------------------------------------------------------
# Stage-15-Step-E #3 follow-up: IP allowlist
# ---------------------------------------------------------------------


def test_load_webhook_ip_allowlist_returns_none_when_unset(monkeypatch):
    """Default-off: the IP filter is opt-in."""
    from telegram_webhook import (
        WEBHOOK_IP_ALLOWLIST_ENV,
        load_webhook_ip_allowlist,
    )

    monkeypatch.delenv(WEBHOOK_IP_ALLOWLIST_ENV, raising=False)
    assert load_webhook_ip_allowlist() is None


def test_load_webhook_ip_allowlist_default_resolves_telegram_ranges(
    monkeypatch,
):
    """The literal ``default`` value expands to Telegram's
    documented delivery ranges."""
    import ipaddress

    from telegram_webhook import (
        DEFAULT_TELEGRAM_IP_RANGES,
        WEBHOOK_IP_ALLOWLIST_ENV,
        load_webhook_ip_allowlist,
    )

    monkeypatch.setenv(WEBHOOK_IP_ALLOWLIST_ENV, "default")
    parsed = load_webhook_ip_allowlist()
    assert parsed is not None
    expected = tuple(ipaddress.ip_network(c) for c in DEFAULT_TELEGRAM_IP_RANGES)
    assert parsed == expected


def test_load_webhook_ip_allowlist_default_case_insensitive(monkeypatch):
    """Operator copy-pasted ``Default`` from a doc — should still work."""
    from telegram_webhook import (
        WEBHOOK_IP_ALLOWLIST_ENV,
        load_webhook_ip_allowlist,
    )

    monkeypatch.setenv(WEBHOOK_IP_ALLOWLIST_ENV, "DEFAULT")
    assert load_webhook_ip_allowlist() is not None
    monkeypatch.setenv(WEBHOOK_IP_ALLOWLIST_ENV, "Default")
    assert load_webhook_ip_allowlist() is not None


def test_load_webhook_ip_allowlist_explicit_cidrs(monkeypatch):
    """Operator can supply an explicit CIDR list — useful for a
    private deploy fronted by a reverse proxy on a known IP."""
    import ipaddress

    from telegram_webhook import (
        WEBHOOK_IP_ALLOWLIST_ENV,
        load_webhook_ip_allowlist,
    )

    monkeypatch.setenv(
        WEBHOOK_IP_ALLOWLIST_ENV, "10.0.0.0/8, 192.168.1.5",
    )
    parsed = load_webhook_ip_allowlist()
    assert parsed is not None
    assert ipaddress.ip_network("10.0.0.0/8") in parsed
    assert ipaddress.ip_network("192.168.1.5/32") in parsed


def test_load_webhook_ip_allowlist_drops_malformed_entries(
    monkeypatch, caplog,
):
    """Fail-soft: a typoed entry is logged and dropped, surviving
    entries still apply. Better than failing closed (which would
    block every Telegram delivery for a single typo) since the
    secret check is the primary gate."""
    import ipaddress

    from telegram_webhook import (
        WEBHOOK_IP_ALLOWLIST_ENV,
        load_webhook_ip_allowlist,
    )

    caplog.set_level("WARNING", logger="bot.telegram_webhook")
    monkeypatch.setenv(
        WEBHOOK_IP_ALLOWLIST_ENV, "10.0.0.0/8, not-an-ip, 192.168.1.0/24",
    )
    parsed = load_webhook_ip_allowlist()
    assert parsed is not None
    assert ipaddress.ip_network("10.0.0.0/8") in parsed
    assert ipaddress.ip_network("192.168.1.0/24") in parsed
    # Three entries in env, one malformed → two survived.
    assert len(parsed) == 2
    assert any(
        "not-an-ip" in r.getMessage() for r in caplog.records
    )


def test_load_webhook_ip_allowlist_all_malformed_returns_none(
    monkeypatch, caplog,
):
    """If every entry was malformed, behave as if unset (None)
    rather than locking everything out — fail-soft."""
    from telegram_webhook import (
        WEBHOOK_IP_ALLOWLIST_ENV,
        load_webhook_ip_allowlist,
    )

    caplog.set_level("WARNING", logger="bot.telegram_webhook")
    monkeypatch.setenv(
        WEBHOOK_IP_ALLOWLIST_ENV, "garbage, also-garbage",
    )
    assert load_webhook_ip_allowlist() is None


def _mock_request_with_remote(
    method: str, path: str, *, app, remote: str | None,
):
    """Helper: build a ``make_mocked_request`` whose
    ``request.remote`` returns the given string. ``request.remote``
    is derived from ``transport.get_extra_info("peername")``, so we
    inject a MagicMock transport that returns ``(ip, port)`` (or
    ``None`` for the missing-remote case)."""
    transport = MagicMock()
    if remote is None:
        transport.get_extra_info = MagicMock(return_value=None)
    else:
        transport.get_extra_info = MagicMock(return_value=(remote, 12345))
    return make_mocked_request(method, path, app=app, transport=transport)


async def test_ip_filter_middleware_passes_through_admin_traffic():
    """Admin / non-webhook requests must pass through untouched
    even when the filter is active. The filter scopes to the
    Telegram path only."""
    import ipaddress

    from telegram_webhook import (
        APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER,
        telegram_webhook_ip_filter_middleware,
    )

    app = web.Application()
    app[APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER] = (
        "/telegram-webhook/abc",
        (ipaddress.ip_network("149.154.160.0/20"),),
    )

    async def handler(_req):
        return web.Response(status=200, text="ok")

    # Admin request from a non-allowlisted IP — must pass through.
    req = make_mocked_request("GET", "/admin/dashboard", app=app)
    resp = await telegram_webhook_ip_filter_middleware(req, handler)
    assert resp.status == 200


async def test_ip_filter_middleware_allows_telegram_ip():
    """A request on the Telegram path from inside the allowlist
    is forwarded to the handler."""
    import ipaddress

    from telegram_webhook import (
        APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER,
        telegram_webhook_ip_filter_middleware,
    )

    app = web.Application()
    app[APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER] = (
        "/telegram-webhook/abc",
        (ipaddress.ip_network("149.154.160.0/20"),),
    )

    async def handler(_req):
        return web.Response(status=200, text="ok")

    req = _mock_request_with_remote(
        "POST", "/telegram-webhook/abc",
        app=app, remote="149.154.167.50",
    )
    resp = await telegram_webhook_ip_filter_middleware(req, handler)
    assert resp.status == 200


async def test_ip_filter_middleware_rejects_outside_ip(caplog):
    """A POST from outside the allowlist gets a 403 *before* the
    handler runs. Cheap rejection (no body parse, no dispatch)."""
    import ipaddress

    from telegram_webhook import (
        APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER,
        telegram_webhook_ip_filter_middleware,
    )

    caplog.set_level("WARNING", logger="bot.telegram_webhook")
    app = web.Application()
    app[APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER] = (
        "/telegram-webhook/abc",
        (ipaddress.ip_network("149.154.160.0/20"),),
    )

    handler_called = []
    async def handler(_req):
        handler_called.append(True)
        return web.Response(status=200)

    req = _mock_request_with_remote(
        "POST", "/telegram-webhook/abc",
        app=app, remote="8.8.8.8",
    )
    resp = await telegram_webhook_ip_filter_middleware(req, handler)
    assert resp.status == 403
    assert handler_called == []
    assert any(
        "not in IP allowlist" in r.getMessage() for r in caplog.records
    )


async def test_ip_filter_middleware_rejects_unparseable_remote():
    """A request without ``request.remote`` (or with garbage) gets
    a 403 — defence in depth."""
    import ipaddress

    from telegram_webhook import (
        APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER,
        telegram_webhook_ip_filter_middleware,
    )

    app = web.Application()
    app[APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER] = (
        "/telegram-webhook/abc",
        (ipaddress.ip_network("149.154.160.0/20"),),
    )

    async def handler(_req):
        return web.Response(status=200)

    req = _mock_request_with_remote(
        "POST", "/telegram-webhook/abc",
        app=app, remote=None,
    )
    resp = await telegram_webhook_ip_filter_middleware(req, handler)
    assert resp.status == 403


async def test_ip_filter_middleware_no_op_when_state_missing():
    """If the filter wasn't installed (state key absent), every
    request passes through — defensive default-allow because the
    secret check is the primary gate."""
    from telegram_webhook import telegram_webhook_ip_filter_middleware

    app = web.Application()

    async def handler(_req):
        return web.Response(status=200, text="ok")

    req = _mock_request_with_remote(
        "POST", "/telegram-webhook/abc", app=app, remote="8.8.8.8",
    )
    resp = await telegram_webhook_ip_filter_middleware(req, handler)
    assert resp.status == 200


def test_install_telegram_webhook_ip_filter_no_op_when_unset(monkeypatch):
    """Installing with no env var configured stores an empty
    allowlist — middleware becomes a no-op without removing it
    from the chain (so a hot-reload doesn't need re-registration)."""
    from telegram_webhook import (
        APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER,
        WEBHOOK_IP_ALLOWLIST_ENV,
        install_telegram_webhook_ip_filter,
        telegram_webhook_ip_filter_middleware,
    )

    monkeypatch.delenv(WEBHOOK_IP_ALLOWLIST_ENV, raising=False)
    cfg = WebhookConfig(
        secret="s", base_url="https://x", path="/telegram-webhook/s",
    )
    app = web.Application()
    install_telegram_webhook_ip_filter(app, config=cfg)
    state = app[APP_KEY_TELEGRAM_WEBHOOK_IP_FILTER]
    assert state[0] == cfg.path
    assert state[1] == ()
    # Middleware was registered (so a future opt-in via env hot-reload
    # works without re-registration).
    assert telegram_webhook_ip_filter_middleware in app.middlewares


# ---------------------------------------------------------------------
# Stage-15-Step-E #3 follow-up: /telegram-webhook/healthz
# ---------------------------------------------------------------------


def test_healthz_path_strips_secret():
    """The healthz path must NOT include the secret. An operator
    polling the health endpoint shouldn't need the secret material."""
    from telegram_webhook import healthz_path_for

    cfg = WebhookConfig(
        secret="topsecret",
        base_url="https://example.com",
        path="/telegram-webhook/topsecret",
    )
    healthz = healthz_path_for(cfg)
    assert healthz == "/telegram-webhook/healthz"
    assert "topsecret" not in healthz


def test_healthz_path_respects_custom_prefix():
    """A custom prefix is preserved on the healthz path."""
    from telegram_webhook import healthz_path_for

    cfg = WebhookConfig(
        secret="s",
        base_url="https://example.com",
        path="/bot/updates/s",
    )
    assert healthz_path_for(cfg) == "/bot/updates/healthz"


async def test_healthz_route_returns_200(aiohttp_client):
    """The route returns 200 with a tiny JSON body. The body
    includes the prefix (so an operator inspecting the response
    can confirm they hit the right deploy) but **not** the
    secret or the URL."""
    from telegram_webhook import (
        install_telegram_webhook_healthz_route,
    )

    cfg = WebhookConfig(
        secret="topsecret",
        base_url="https://example.com",
        path="/telegram-webhook/topsecret",
    )
    app = web.Application()
    install_telegram_webhook_healthz_route(app, cfg)
    client = await aiohttp_client(app)

    resp = await client.get("/telegram-webhook/healthz")
    assert resp.status == 200
    body = await resp.json()
    assert body["status"] == "ok"
    assert body["webhook_prefix"] == "/telegram-webhook"
    # No secret leak.
    payload = await resp.text()
    assert "topsecret" not in payload


async def test_healthz_route_does_not_require_secret_in_query(aiohttp_client):
    """The probe is intentionally unauthenticated — a load
    balancer / k8s liveness probe should work without any
    credentials. Confirm a plain GET succeeds."""
    from telegram_webhook import (
        install_telegram_webhook_healthz_route,
    )

    cfg = WebhookConfig(
        secret="s",
        base_url="https://example.com",
        path="/telegram-webhook/s",
    )
    app = web.Application()
    install_telegram_webhook_healthz_route(app, cfg)
    client = await aiohttp_client(app)

    resp = await client.get("/telegram-webhook/healthz")
    assert resp.status == 200


async def test_healthz_route_does_not_consume_rate_limit_token(
    aiohttp_client,
):
    """The healthz path must not be in the rate-limited set —
    a load balancer probing every 5s would otherwise fight the
    bucket against real Telegram delivery."""
    from rate_limit import (
        WEBHOOK_RATE_LIMITED_PATHS_KEY,
        install_webhook_rate_limit,
    )
    from telegram_webhook import (
        install_telegram_webhook_healthz_route,
    )

    cfg = WebhookConfig(
        secret="s",
        base_url="https://example.com",
        path="/telegram-webhook/s",
    )
    app = web.Application()
    install_webhook_rate_limit(app)
    install_telegram_webhook_healthz_route(app, cfg)
    rate_limited = app[WEBHOOK_RATE_LIMITED_PATHS_KEY]
    assert "/telegram-webhook/healthz" not in rate_limited
