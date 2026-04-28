"""Tests for the web admin scaffold (Stage-8-Part-1).

Two flavours of tests:

* Pure-function tests for the cookie sign / verify helpers — fast,
  deterministic, no I/O.
* Integration tests that spin up an aiohttp app via the
  ``aiohttp_client`` fixture (from pytest-aiohttp, already in
  requirements-dev.txt for the webhook rate-limit suite) and
  exercise the login flow end-to-end. We do NOT spin up Postgres —
  ``setup_admin_routes`` accepts a stub ``db`` whose
  ``get_system_metrics()`` returns canned data.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from web_admin import (
    COOKIE_NAME,
    setup_admin_routes,
    sign_cookie,
    verify_cookie,
)


# ---------------------------------------------------------------------
# sign_cookie / verify_cookie
# ---------------------------------------------------------------------


def test_sign_cookie_round_trip():
    secret = "test-secret-1234567890"
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    cookie = sign_cookie(expires, secret=secret)
    assert verify_cookie(cookie, secret=secret) is True


def test_verify_cookie_rejects_none():
    assert verify_cookie(None, secret="s") is False


def test_verify_cookie_rejects_empty_string():
    assert verify_cookie("", secret="s") is False


def test_verify_cookie_rejects_no_dot_separator():
    assert verify_cookie("just-a-blob", secret="s") is False


def test_verify_cookie_rejects_bad_base64():
    assert verify_cookie("!!!.!!!", secret="s") is False


def test_verify_cookie_rejects_tampered_signature():
    secret = "test-secret-1234567890"
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    good = sign_cookie(expires, secret=secret)
    iso, sig = good.split(".", 1)
    # Flip a byte in the sig
    bad_sig = "A" + sig[1:] if sig[0] != "A" else "B" + sig[1:]
    tampered = f"{iso}.{bad_sig}"
    assert verify_cookie(tampered, secret=secret) is False


def test_verify_cookie_rejects_tampered_payload():
    """Changing the iso payload invalidates the HMAC."""
    secret = "test-secret-1234567890"
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    good = sign_cookie(expires, secret=secret)
    iso, sig = good.split(".", 1)
    # Mutate the iso bytes by flipping a char
    iso_bad = iso[:-1] + ("X" if iso[-1] != "X" else "Y")
    tampered = f"{iso_bad}.{sig}"
    assert verify_cookie(tampered, secret=secret) is False


def test_verify_cookie_rejects_wrong_secret():
    secret = "right-secret"
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    cookie = sign_cookie(expires, secret=secret)
    assert verify_cookie(cookie, secret="wrong-secret") is False


def test_verify_cookie_rejects_expired():
    secret = "test-secret-1234567890"
    expires = datetime.now(timezone.utc) - timedelta(seconds=1)
    cookie = sign_cookie(expires, secret=secret)
    assert verify_cookie(cookie, secret=secret) is False


def test_verify_cookie_with_explicit_now():
    secret = "test-secret-1234567890"
    expires = datetime(2026, 1, 1, tzinfo=timezone.utc)
    cookie = sign_cookie(expires, secret=secret)

    before = datetime(2025, 12, 31, tzinfo=timezone.utc)
    after = datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert verify_cookie(cookie, secret=secret, now=before) is True
    assert verify_cookie(cookie, secret=secret, now=after) is False


def test_sign_cookie_rejects_empty_secret():
    with pytest.raises(ValueError):
        sign_cookie(
            datetime.now(timezone.utc) + timedelta(hours=1),
            secret="",
        )


def test_signed_cookie_contains_no_secret():
    """Sanity check: the cookie value should never echo the secret."""
    secret = "this-secret-must-not-leak-9999"
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    cookie = sign_cookie(expires, secret=secret)
    assert secret not in cookie
    assert "this-secret" not in cookie


# ---------------------------------------------------------------------
# Integration tests via aiohttp_client
# ---------------------------------------------------------------------


def _stub_db(
    metrics: dict | None = None,
    promo_rows: list | None = None,
    create_promo_result: bool | Exception = True,
    revoke_promo_result: bool | Exception = True,
    gift_rows: list | None = None,
    create_gift_result: bool | Exception = True,
    revoke_gift_result: bool | Exception = True,
    search_users_result: list | Exception | None = None,
    user_summary_result: dict | None | Exception = None,
    adjust_balance_result: dict | None | Exception = None,
    broadcast_recipients: list | Exception | None = None,
    list_transactions_result: dict | Exception | None = None,
):
    """A minimal Database stub that exposes the methods web_admin uses.

    The default ``metrics`` shape MUST match what
    ``Database.get_system_metrics`` actually returns in production
    (``database.py:1088-1101``) — Devin Review caught a key-name
    mismatch (PR #54) where the template wanted ``user_count`` but
    the real DB returned ``users_total``, which would 500 every
    dashboard load. Keep this stub's keys aligned with the real
    method.
    """
    db = AsyncMock()
    db.get_system_metrics = AsyncMock(
        return_value=metrics
        or {
            "users_total": 42,
            "users_active_7d": 7,
            "revenue_usd": 123.45,
            "spend_usd": 67.89,
            "top_models": [
                {
                    "model": "openrouter/auto",
                    "count": 100,
                    "cost_usd": 3.21,
                }
            ],
        }
    )
    db.list_promo_codes = AsyncMock(
        return_value=promo_rows if promo_rows is not None else []
    )
    if isinstance(create_promo_result, Exception):
        db.create_promo_code = AsyncMock(side_effect=create_promo_result)
    else:
        db.create_promo_code = AsyncMock(return_value=create_promo_result)
    if isinstance(revoke_promo_result, Exception):
        db.revoke_promo_code = AsyncMock(side_effect=revoke_promo_result)
    else:
        db.revoke_promo_code = AsyncMock(return_value=revoke_promo_result)
    db.list_gift_codes = AsyncMock(
        return_value=gift_rows if gift_rows is not None else []
    )
    if isinstance(create_gift_result, Exception):
        db.create_gift_code = AsyncMock(side_effect=create_gift_result)
    else:
        db.create_gift_code = AsyncMock(return_value=create_gift_result)
    if isinstance(revoke_gift_result, Exception):
        db.revoke_gift_code = AsyncMock(side_effect=revoke_gift_result)
    else:
        db.revoke_gift_code = AsyncMock(return_value=revoke_gift_result)
    if isinstance(search_users_result, Exception):
        db.search_users = AsyncMock(side_effect=search_users_result)
    else:
        db.search_users = AsyncMock(
            return_value=search_users_result if search_users_result is not None else []
        )
    if isinstance(user_summary_result, Exception):
        db.get_user_admin_summary = AsyncMock(side_effect=user_summary_result)
    else:
        db.get_user_admin_summary = AsyncMock(return_value=user_summary_result)
    if isinstance(adjust_balance_result, Exception):
        db.admin_adjust_balance = AsyncMock(side_effect=adjust_balance_result)
    else:
        db.admin_adjust_balance = AsyncMock(return_value=adjust_balance_result)
    if isinstance(broadcast_recipients, Exception):
        db.iter_broadcast_recipients = AsyncMock(
            side_effect=broadcast_recipients
        )
    else:
        db.iter_broadcast_recipients = AsyncMock(
            return_value=broadcast_recipients
            if broadcast_recipients is not None
            else []
        )
    # Stage-8-Part-6: transactions browser.
    default_list_tx = {
        "rows": [],
        "total": 0,
        "page": 1,
        "per_page": 50,
        "total_pages": 0,
    }
    if isinstance(list_transactions_result, Exception):
        db.list_transactions = AsyncMock(
            side_effect=list_transactions_result
        )
    else:
        db.list_transactions = AsyncMock(
            return_value=list_transactions_result
            if list_transactions_result is not None
            else default_list_tx
        )
    return db


@pytest.fixture
def make_admin_app():
    """Factory: build a fresh aiohttp app with the admin routes mounted."""

    def _build(
        password: str = "letmein",
        session_secret: str = "x" * 32,
        db=None,
        cookie_secure: bool = False,
        bot=None,
    ):
        app = web.Application()
        setup_admin_routes(
            app,
            db=db if db is not None else _stub_db(),
            password=password,
            session_secret=session_secret,
            ttl_hours=24,
            cookie_secure=cookie_secure,
            bot=bot,
        )
        return app

    return _build


async def test_login_get_renders_form(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app())
    resp = await client.get("/admin/login")
    assert resp.status == 200
    body = await resp.text()
    assert "Sign in" in body
    assert 'name="password"' in body


async def test_login_post_wrong_password(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app(password="letmein"))
    resp = await client.post(
        "/admin/login", data={"password": "nope"}, allow_redirects=False
    )
    assert resp.status == 401
    body = await resp.text()
    assert "Wrong password" in body
    # No cookie set on a failed login
    assert COOKIE_NAME not in resp.cookies


async def test_login_post_right_password_redirects_and_sets_cookie(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="letmein"))
    resp = await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/"
    cookie = resp.cookies.get(COOKIE_NAME)
    assert cookie is not None
    assert cookie.value  # non-empty signed payload


async def test_dashboard_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app())
    resp = await client.get("/admin/", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/login"


async def test_dashboard_with_auth_renders_metrics(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        {
            "users_total": 1234,
            "users_active_7d": 5,
            "revenue_usd": 99.50,
            "spend_usd": 12.3456,
            "top_models": [],
        }
    )
    client = await aiohttp_client(
        make_admin_app(password="letmein", db=db)
    )
    # Log in
    login = await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    assert login.status == 302
    # aiohttp's TestClient persists cookies across requests by default.
    resp = await client.get("/admin/")
    assert resp.status == 200
    body = await resp.text()
    assert "1,234" in body
    assert "$99.50" in body
    # ``spend_usd`` is rendered with 4dp precision (matches OpenRouter
    # token-cost granularity) — render check pins both the format AND
    # the key spelling.
    assert "$12.3456" in body
    db.get_system_metrics.assert_awaited()


async def test_dashboard_handles_db_error(aiohttp_client, make_admin_app):
    db = AsyncMock()
    db.get_system_metrics = AsyncMock(side_effect=RuntimeError("boom"))
    client = await aiohttp_client(
        make_admin_app(password="letmein", db=db)
    )
    await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    resp = await client.get("/admin/")
    # Even with a DB failure, dashboard renders rather than 500-ing.
    assert resp.status == 200
    body = await resp.text()
    assert "Database query failed" in body


async def test_logout_clears_cookie(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app(password="letmein"))
    await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    # Verify auth works
    pre = await client.get("/admin/", allow_redirects=False)
    assert pre.status == 200

    resp = await client.get("/admin/logout", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/login"

    # After logout, the dashboard should bounce back to login.
    post = await client.get("/admin/", allow_redirects=False)
    assert post.status == 302
    assert post.headers["Location"] == "/admin/login"


async def test_login_redirects_when_already_authed(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="letmein"))
    await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    # Hitting /admin/login while authed should redirect to dashboard.
    resp = await client.get("/admin/login", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/"


async def test_unconfigured_password_refuses_login(
    aiohttp_client, make_admin_app
):
    """Empty ADMIN_PASSWORD must reject all login attempts with 500."""
    client = await aiohttp_client(make_admin_app(password=""))
    resp = await client.post(
        "/admin/login",
        data={"password": "anything"},
        allow_redirects=False,
    )
    assert resp.status == 500
    body = await resp.text()
    assert "not configured" in body


async def test_unconfigured_session_secret_refuses_login(
    aiohttp_client, make_admin_app
):
    """Empty ADMIN_SESSION_SECRET must reject all login attempts even
    if ADMIN_PASSWORD is set.

    Devin Review caught a bug on PR #54 where ``setup_admin_routes``
    auto-generated a random secret when the env var was missing,
    which silently bypassed the ``not expected or not secret`` guard
    in ``login_post`` and let a half-configured deploy log in. Pin
    the new behaviour: empty secret = guard fires = 500 with
    "not configured" body.
    """
    client = await aiohttp_client(
        make_admin_app(password="letmein", session_secret="")
    )
    resp = await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    assert resp.status == 500
    body = await resp.text()
    assert "not configured" in body


# ---------------------------------------------------------------------
# Stage-9-Step-1: /admin/login rate-limit
# ---------------------------------------------------------------------


async def test_login_rate_limited_after_burst(aiohttp_client, make_admin_app):
    """N wrong-password attempts in quick succession trip the per-IP
    bucket and flip the response from 401 to 429.

    Tighten the cache so the test doesn't need a sleep — one token
    capacity, refill ~0/sec means the very first wrong password exhausts
    the bucket and every subsequent attempt is rejected before we even
    compare passwords.
    """
    from rate_limit import (
        LOGIN_RATE_LIMIT_CACHE_KEY,
        _LRUBucketCache,
    )

    app = make_admin_app(password="letmein")
    # Replace the cache with a tighter one so the test is deterministic.
    app[LOGIN_RATE_LIMIT_CACHE_KEY] = _LRUBucketCache(
        capacity=1, refill_rate=0.001
    )
    client = await aiohttp_client(app)

    first = await client.post(
        "/admin/login", data={"password": "wrong"}, allow_redirects=False
    )
    # Bucket had 1 token, first attempt consumes it, we still reach the
    # password compare → 401.
    assert first.status == 401

    second = await client.post(
        "/admin/login", data={"password": "wrong"}, allow_redirects=False
    )
    # Bucket is empty now → 429 BEFORE we compare passwords.
    assert second.status == 429
    body = await second.text()
    assert "Too many login attempts" in body


async def test_login_rate_limit_runs_before_password_compare(
    aiohttp_client, make_admin_app
):
    """Even a correct password is rate-limited. Stops an attacker with
    the right password from re-submitting faster than the bucket refills
    to brute-force a hypothetical 2FA code later.
    """
    from rate_limit import (
        LOGIN_RATE_LIMIT_CACHE_KEY,
        _LRUBucketCache,
    )

    app = make_admin_app(password="letmein")
    app[LOGIN_RATE_LIMIT_CACHE_KEY] = _LRUBucketCache(
        capacity=1, refill_rate=0.001
    )
    client = await aiohttp_client(app)

    # Burn the bucket with a wrong password.
    await client.post(
        "/admin/login", data={"password": "wrong"}, allow_redirects=False
    )
    # Now attempt with the correct one — still 429, no cookie set.
    resp = await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False
    )
    assert resp.status == 429
    assert COOKIE_NAME not in resp.cookies


async def test_login_rate_limit_installed_by_setup_admin_routes(
    make_admin_app,
):
    """``setup_admin_routes`` must install the login rate-limit cache
    on the app — if the bucket isn't there, ``consume_login_token``
    fails open and the throttle is silently disabled.
    """
    from rate_limit import LOGIN_RATE_LIMIT_CACHE_KEY

    app = make_admin_app()
    assert LOGIN_RATE_LIMIT_CACHE_KEY in app


async def test_login_rate_limit_uses_xff_with_trust_proxy(
    aiohttp_client, make_admin_app, monkeypatch
):
    """Bundled fix regression pin: with ``TRUST_PROXY_HEADERS=1`` set,
    the login throttle keys on ``X-Forwarded-For`` — two clients behind
    the same proxy each get their own bucket.

    Without this fix, a Cloudflare-tunnel deploy would bucket every
    admin onto the tunnel IP: the first password sprayer drains the
    shared bucket and locks out every legitimate admin.
    """
    from rate_limit import (
        LOGIN_RATE_LIMIT_CACHE_KEY,
        TRUST_PROXY_HEADERS_ENV,
        _LRUBucketCache,
    )

    monkeypatch.setenv(TRUST_PROXY_HEADERS_ENV, "1")
    app = make_admin_app(password="letmein")
    app[LOGIN_RATE_LIMIT_CACHE_KEY] = _LRUBucketCache(
        capacity=1, refill_rate=0.001
    )
    client = await aiohttp_client(app)

    # Client A uses up its bucket (2nd attempt 429).
    a1 = await client.post(
        "/admin/login",
        data={"password": "wrong"},
        headers={"X-Forwarded-For": "203.0.113.1"},
        allow_redirects=False,
    )
    a2 = await client.post(
        "/admin/login",
        data={"password": "wrong"},
        headers={"X-Forwarded-For": "203.0.113.1"},
        allow_redirects=False,
    )
    assert a1.status == 401
    assert a2.status == 429

    # Client B from a different public IP still has its full bucket.
    b = await client.post(
        "/admin/login",
        data={"password": "wrong"},
        headers={"X-Forwarded-For": "198.51.100.9"},
        allow_redirects=False,
    )
    assert b.status == 401


async def test_admin_bare_path_redirects_to_slash(
    aiohttp_client, make_admin_app
):
    """/admin → /admin/ — common typo, should not 404."""
    client = await aiohttp_client(make_admin_app())
    resp = await client.get("/admin", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/"


async def test_double_setup_is_idempotent(make_admin_app, caplog):
    """A second setup_admin_routes call on the same app is a no-op."""
    import logging

    app = make_admin_app()
    with caplog.at_level(logging.WARNING, logger="bot.web_admin"):
        setup_admin_routes(
            app,
            db=_stub_db(),
            password="another",
            session_secret="y" * 32,
        )
    assert any(
        "called twice" in rec.message for rec in caplog.records
    )


async def test_constant_time_password_compare_handles_length_mismatch(
    aiohttp_client, make_admin_app
):
    """Submitting a password with a different length than expected
    must NOT crash — hmac.compare_digest tolerates mismatched lengths."""
    client = await aiohttp_client(make_admin_app(password="short"))
    resp = await client.post(
        "/admin/login",
        data={"password": "this-one-is-much-longer"},
        allow_redirects=False,
    )
    assert resp.status == 401  # rejected, not 500


async def test_invalid_cookie_treated_as_unauthed(
    aiohttp_client, make_admin_app
):
    """A cookie with the right name but a tampered signature should
    cause /admin/ to redirect, not 500."""
    client = await aiohttp_client(make_admin_app())
    client.session.cookie_jar.update_cookies(
        {COOKIE_NAME: "totally-bogus-value"}
    )
    resp = await client.get("/admin/", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/login"


async def test_dashboard_renders_against_real_db_schema(
    aiohttp_client, make_admin_app
):
    """Pin the contract: dashboard.html must render cleanly against the
    exact key shape ``Database.get_system_metrics`` returns (see
    ``database.py:1088-1101`` and ``admin.format_metrics`` which
    already consumes this shape successfully).

    Devin Review caught a key-name mismatch in the original Stage-8-Part-1
    where the template was reading ``user_count`` / ``total_revenue_usd``
    while the DB returned ``users_total`` / ``revenue_usd`` — a 500 on
    every dashboard load in production. This test fails loudly if the
    template or fallback dicts drift from the real schema again.
    """
    db = AsyncMock()
    # Exact shape returned by Database.get_system_metrics (verified
    # from database.py and admin.format_metrics).
    db.get_system_metrics = AsyncMock(
        return_value={
            "users_total": 9999,
            "users_active_7d": 250,
            "revenue_usd": 4321.0,
            "spend_usd": 1234.5678,
            "top_models": [
                {
                    "model": "openai/gpt-4o-mini",
                    "count": 5000,
                    "cost_usd": 12.3456,
                },
                {
                    "model": "anthropic/claude-3.5-sonnet",
                    "count": 1234,
                    "cost_usd": 7.8901,
                },
            ],
        }
    )
    client = await aiohttp_client(
        make_admin_app(password="letmein", db=db)
    )
    await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    resp = await client.get("/admin/")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    # All four stat tiles render with the right values.
    assert "9,999" in body
    assert "250" in body
    assert "$4,321.00" in body
    assert "$1,234.5678" in body
    # Top-models table renders both rows, with model name + count + cost.
    assert "openai/gpt-4o-mini" in body
    assert "anthropic/claude-3.5-sonnet" in body
    assert "5,000" in body
    assert "$12.3456" in body
    assert "$7.8901" in body


async def test_dashboard_fallback_dicts_match_template_keys(
    aiohttp_client, make_admin_app
):
    """When the DB call fails, the fallback dict must use the same
    keys the template reads — otherwise the error path 500s instead
    of rendering a graceful 'Database query failed' banner.
    """
    db = AsyncMock()
    db.get_system_metrics = AsyncMock(side_effect=RuntimeError("kaboom"))
    client = await aiohttp_client(
        make_admin_app(password="letmein", db=db)
    )
    await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    resp = await client.get("/admin/")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "Database query failed" in body
    # Tile labels render with zero values (proves the fallback's keys
    # match the template's reads, otherwise jinja would 500).
    assert "Total users" in body
    assert "Active (7d)" in body


# ---------------------------------------------------------------------
# Stage-8-Part-2: promo codes UI
# ---------------------------------------------------------------------


from web_admin import (
    DISCOUNT_AMOUNT_MAX,
    csrf_token_for,
    parse_promo_form,
    pop_flash,
    set_flash,
    verify_csrf_token,
)


# parse_promo_form (pure function)
# ---------------------------------------------------------------------


def _form(d: dict) -> dict:
    """Form data is multidict-ish; plain dict is fine here since
    parse_promo_form only does .get(name)."""
    return d


def test_parse_promo_form_percent_happy():
    out = parse_promo_form(_form({
        "code": "welcome20",
        "discount_kind": "percent",
        "discount_value": "20",
    }))
    assert out == {
        "code": "WELCOME20",
        "discount_percent": 20,
        "discount_amount": None,
        "max_uses": None,
        "expires_in_days": None,
    }


def test_parse_promo_form_percent_with_sign():
    out = parse_promo_form(_form({
        "code": "BLACKFRI",
        "discount_kind": "percent",
        "discount_value": "30%",
    }))
    assert out["discount_percent"] == 30


def test_parse_promo_form_amount_happy():
    out = parse_promo_form(_form({
        "code": "GIFT5",
        "discount_kind": "amount",
        "discount_value": "$5",
    }))
    assert out["discount_amount"] == 5.0
    assert out["discount_percent"] is None


def test_parse_promo_form_amount_with_max_uses_and_expiry():
    out = parse_promo_form(_form({
        "code": "BIRTHDAY",
        "discount_kind": "amount",
        "discount_value": "10.5",
        "max_uses": "100",
        "expires_in_days": "30",
    }))
    assert out["discount_amount"] == 10.5
    assert out["max_uses"] == 100
    assert out["expires_in_days"] == 30


def test_parse_promo_form_missing_code():
    assert parse_promo_form(_form({"discount_kind": "percent",
                                    "discount_value": "10"})) == "missing_code"


def test_parse_promo_form_bad_code_chars():
    assert parse_promo_form(_form({
        "code": "has spaces",
        "discount_kind": "percent",
        "discount_value": "10",
    })) == "bad_code"


def test_parse_promo_form_bad_discount_kind():
    assert parse_promo_form(_form({
        "code": "X",
        "discount_kind": "free",
        "discount_value": "1",
    })) == "bad_discount_kind"


def test_parse_promo_form_bad_percent_zero():
    assert parse_promo_form(_form({
        "code": "X",
        "discount_kind": "percent",
        "discount_value": "0",
    })) == "bad_percent"


def test_parse_promo_form_bad_percent_over_100():
    assert parse_promo_form(_form({
        "code": "X",
        "discount_kind": "percent",
        "discount_value": "101",
    })) == "bad_percent"


def test_parse_promo_form_bad_percent_non_int():
    assert parse_promo_form(_form({
        "code": "X",
        "discount_kind": "percent",
        "discount_value": "10.5",
    })) == "bad_percent"


def test_parse_promo_form_bad_amount_negative():
    assert parse_promo_form(_form({
        "code": "X",
        "discount_kind": "amount",
        "discount_value": "-5",
    })) == "bad_amount"


def test_parse_promo_form_bad_amount_nan():
    assert parse_promo_form(_form({
        "code": "X",
        "discount_kind": "amount",
        "discount_value": "nan",
    })) == "bad_amount"


def test_parse_promo_form_bad_amount_inf():
    assert parse_promo_form(_form({
        "code": "X",
        "discount_kind": "amount",
        "discount_value": "inf",
    })) == "bad_amount"


def test_parse_promo_form_amount_too_large():
    """The bundled bug fix: discount_amount has no upper bound, so
    creating a promo with $9_999_999 used to crash the INSERT with
    PG ``numeric field overflow`` because the column is DECIMAL(10,4)
    and the parser/db happily passed the giant value through. The
    parser now rejects it client-side with a friendly error."""
    out = parse_promo_form(_form({
        "code": "MILLION",
        "discount_kind": "amount",
        "discount_value": str(DISCOUNT_AMOUNT_MAX + 1),
    }))
    assert out == "discount_too_large"


def test_parse_promo_form_amount_at_cap_passes():
    out = parse_promo_form(_form({
        "code": "BIG",
        "discount_kind": "amount",
        "discount_value": str(DISCOUNT_AMOUNT_MAX),
    }))
    assert isinstance(out, dict)
    assert out["discount_amount"] == DISCOUNT_AMOUNT_MAX


def test_parse_promo_form_bad_max_uses_non_int():
    assert parse_promo_form(_form({
        "code": "X",
        "discount_kind": "percent",
        "discount_value": "10",
        "max_uses": "many",
    })) == "bad_max_uses"


def test_parse_promo_form_bad_max_uses_zero():
    assert parse_promo_form(_form({
        "code": "X",
        "discount_kind": "percent",
        "discount_value": "10",
        "max_uses": "0",
    })) == "bad_max_uses"


def test_parse_promo_form_bad_days():
    assert parse_promo_form(_form({
        "code": "X",
        "discount_kind": "percent",
        "discount_value": "10",
        "expires_in_days": "-1",
    })) == "bad_days"


# create_promo_code DB upper-bound guard (pure unit test on the helper)
# ---------------------------------------------------------------------


def test_create_promo_code_rejects_amount_over_decimal_cap():
    """Bundled bug fix mirrored on the DB layer.

    Even if a future admin path bypasses parse_promo_form (e.g.
    direct API integration, or a different web framework), the DB
    method itself must reject discount_amount > DECIMAL(10,4)
    capacity so we never get PG ``numeric field overflow``.
    """
    from database import Database
    db = Database.__new__(Database)  # don't need a real connection
    db.pool = None
    import asyncio
    with pytest.raises(ValueError, match="DECIMAL"):
        asyncio.get_event_loop().run_until_complete(
            db.create_promo_code(
                code="X",
                discount_amount=1_000_000.0,
            )
        )


# CSRF helpers
# ---------------------------------------------------------------------


async def test_csrf_token_empty_when_not_logged_in(
    aiohttp_client, make_admin_app
):
    """No session cookie = no CSRF token. Forms aren't reachable
    without auth anyway, but the helper should fail closed."""
    app = make_admin_app()
    client = await aiohttp_client(app)
    resp = await client.get("/admin/login", allow_redirects=False)
    # We can't call csrf_token_for from outside a request context
    # easily, so we instead verify the login page (unauthed) doesn't
    # contain a csrf_token at all.
    body = await resp.text()
    assert 'name="csrf_token"' not in body


async def test_csrf_token_consistent_within_a_session(
    aiohttp_client, make_admin_app
):
    """Two GETs from the same session should embed the same token."""
    client = await aiohttp_client(make_admin_app(password="letmein"))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False
    )
    r1 = await client.get("/admin/promos")
    r2 = await client.get("/admin/promos")
    body1, body2 = await r1.text(), await r2.text()

    import re
    m1 = re.search(r'name="csrf_token" value="([^"]+)"', body1)
    m2 = re.search(r'name="csrf_token" value="([^"]+)"', body2)
    assert m1 and m2
    assert m1.group(1) == m2.group(1)


async def test_csrf_token_changes_after_relogin(
    aiohttp_client, make_admin_app
):
    """A new login = new session cookie = new derived CSRF token,
    so old form tokens stop working."""
    client = await aiohttp_client(make_admin_app(password="letmein"))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False
    )
    r1 = await client.get("/admin/promos")
    body1 = await r1.text()

    await client.get("/admin/logout", allow_redirects=False)
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False
    )
    r2 = await client.get("/admin/promos")
    body2 = await r2.text()

    import re
    m1 = re.search(r'name="csrf_token" value="([^"]+)"', body1)
    m2 = re.search(r'name="csrf_token" value="([^"]+)"', body2)
    assert m1 and m2
    assert m1.group(1) != m2.group(1)


# /admin/promos GET
# ---------------------------------------------------------------------


async def test_promos_get_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app())
    resp = await client.get("/admin/promos", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")


async def test_promos_get_lists_codes(aiohttp_client, make_admin_app):
    rows = [
        {
            "code": "WELCOME20",
            "discount_percent": 20,
            "discount_amount": None,
            "max_uses": 100,
            "used_count": 5,
            "expires_at": "2030-12-31T23:59:59+00:00",
            "is_active": True,
            "created_at": "2026-01-01T00:00:00+00:00",
        },
        {
            "code": "REVOKED",
            "discount_percent": None,
            "discount_amount": 5.0,
            "max_uses": None,
            "used_count": 0,
            "expires_at": None,
            "is_active": False,
            "created_at": "2025-01-01T00:00:00+00:00",
        },
    ]
    client = await aiohttp_client(
        make_admin_app(password="pw", db=_stub_db(promo_rows=rows))
    )
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/promos")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "WELCOME20" in body
    assert "20%" in body
    assert "REVOKED" in body
    assert "$5.00" in body
    assert "active" in body
    assert "revoked" in body
    # CSRF token embedded in revoke form (only for active rows).
    assert 'action="/admin/promos/WELCOME20/revoke"' in body


async def test_promos_get_db_error_renders_banner(
    aiohttp_client, make_admin_app
):
    db = AsyncMock()
    db.list_promo_codes = AsyncMock(side_effect=RuntimeError("boom"))
    client = await aiohttp_client(
        make_admin_app(password="pw", db=db)
    )
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/promos")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "Database query failed" in body


# /admin/promos POST (create)
# ---------------------------------------------------------------------


async def _login_and_get_csrf(client, password: str = "pw") -> str:
    """Log in, fetch the promos page, scrape its CSRF token."""
    await client.post(
        "/admin/login", data={"password": password}, allow_redirects=False
    )
    resp = await client.get("/admin/promos")
    body = await resp.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token in /admin/promos form"
    return m.group(1)


async def test_promos_create_happy_path(aiohttp_client, make_admin_app):
    db = _stub_db(create_promo_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_csrf(client, "pw")

    resp = await client.post(
        "/admin/promos",
        data={
            "csrf_token": csrf,
            "code": "WELCOME20",
            "discount_kind": "percent",
            "discount_value": "20",
            "max_uses": "10",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/promos"
    db.create_promo_code.assert_awaited_once()
    kwargs = db.create_promo_code.await_args.kwargs
    assert kwargs["code"] == "WELCOME20"
    assert kwargs["discount_percent"] == 20
    assert kwargs["discount_amount"] is None
    assert kwargs["max_uses"] == 10

    # Follow the redirect, expect success flash.
    resp2 = await client.get("/admin/promos")
    body = await resp2.text()
    assert "alert-success" in body
    assert "Created" in body and "WELCOME20" in body


async def test_promos_create_rejects_missing_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.post(
        "/admin/promos",
        data={
            "code": "X",
            "discount_kind": "percent",
            "discount_value": "20",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.create_promo_code.assert_not_awaited()
    resp2 = await client.get("/admin/promos")
    body = await resp2.text()
    assert "CSRF" in body


async def test_promos_create_rejects_wrong_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login_and_get_csrf(client, "pw")  # establish session
    resp = await client.post(
        "/admin/promos",
        data={
            "csrf_token": "obviously-wrong",
            "code": "X",
            "discount_kind": "percent",
            "discount_value": "20",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.create_promo_code.assert_not_awaited()


async def test_promos_create_validation_error_shows_flash(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_csrf(client, "pw")
    resp = await client.post(
        "/admin/promos",
        data={
            "csrf_token": csrf,
            "code": "BAD",
            "discount_kind": "amount",
            "discount_value": "9999999",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.create_promo_code.assert_not_awaited()
    resp2 = await client.get("/admin/promos")
    body = await resp2.text()
    assert "alert-error" in body
    # Friendly message text.
    assert "DB limit" in body or "999,999.00" in body


async def test_promos_create_conflict_shows_flash(
    aiohttp_client, make_admin_app
):
    db = _stub_db(create_promo_result=False)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_csrf(client, "pw")
    resp = await client.post(
        "/admin/promos",
        data={
            "csrf_token": csrf,
            "code": "EXISTING",
            "discount_kind": "percent",
            "discount_value": "10",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/promos")
    body = await resp2.text()
    assert "already exists" in body


async def test_promos_create_db_value_error_shows_flash(
    aiohttp_client, make_admin_app
):
    """If create_promo_code raises ValueError (e.g., the DB-side
    DECIMAL guard fired because the parser was bypassed), show the
    raw message rather than 500."""
    db = _stub_db(create_promo_result=ValueError("DECIMAL(10,4) limit"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_csrf(client, "pw")
    resp = await client.post(
        "/admin/promos",
        data={
            "csrf_token": csrf,
            "code": "X",
            "discount_kind": "percent",
            "discount_value": "10",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/promos")
    body = await resp2.text()
    assert "DECIMAL(10,4) limit" in body


async def test_promos_create_with_expires_in_days_passes_datetime(
    aiohttp_client, make_admin_app
):
    db = _stub_db(create_promo_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_csrf(client, "pw")
    resp = await client.post(
        "/admin/promos",
        data={
            "csrf_token": csrf,
            "code": "FRIDAY",
            "discount_kind": "percent",
            "discount_value": "15",
            "expires_in_days": "7",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    kwargs = db.create_promo_code.await_args.kwargs
    assert kwargs["expires_at"] is not None
    # 7 days +/- 1 minute (test-runtime slack).
    delta = kwargs["expires_at"] - datetime.now(timezone.utc)
    assert timedelta(days=6, hours=23, minutes=58) < delta < timedelta(days=7, minutes=2)


# /admin/promos/{code}/revoke POST
# ---------------------------------------------------------------------


async def test_promos_revoke_happy_path(aiohttp_client, make_admin_app):
    db = _stub_db(revoke_promo_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_csrf(client, "pw")
    resp = await client.post(
        "/admin/promos/WELCOME20/revoke",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.revoke_promo_code.assert_awaited_once_with("WELCOME20")
    resp2 = await client.get("/admin/promos")
    body = await resp2.text()
    assert "alert-success" in body
    assert "Revoked" in body and "WELCOME20" in body


async def test_promos_revoke_already_inactive_shows_info(
    aiohttp_client, make_admin_app
):
    db = _stub_db(revoke_promo_result=False)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_csrf(client, "pw")
    resp = await client.post(
        "/admin/promos/GHOST/revoke",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/promos")
    body = await resp2.text()
    assert "alert-info" in body
    # Apostrophe is HTML-escaped by Jinja's autoescape; tolerate
    # either form so we don't couple to escape-style.
    assert "already revoked" in body
    assert ("doesn't exist" in body) or ("doesn&#39;t exist" in body) \
        or ("doesn&#x27;t exist" in body)


async def test_promos_revoke_rejects_missing_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_db(revoke_promo_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.post(
        "/admin/promos/X/revoke",
        data={},  # no csrf_token
        allow_redirects=False,
    )
    assert resp.status == 302
    db.revoke_promo_code.assert_not_awaited()


async def test_promos_revoke_requires_auth(aiohttp_client, make_admin_app):
    db = _stub_db(revoke_promo_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    resp = await client.post(
        "/admin/promos/X/revoke",
        data={"csrf_token": "anything"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")
    db.revoke_promo_code.assert_not_awaited()


async def test_promos_revoke_invalid_url_code(aiohttp_client, make_admin_app):
    """Even with a valid CSRF token, junk in the URL must be rejected
    rather than passed through to the DB."""
    db = _stub_db(revoke_promo_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_csrf(client, "pw")
    # 80-char code (above 64-char cap)
    long_code = "A" * 80
    resp = await client.post(
        f"/admin/promos/{long_code}/revoke",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.revoke_promo_code.assert_not_awaited()


# Flash cookie round-trip
# ---------------------------------------------------------------------


def test_flash_cookie_signed_round_trip():
    """A flash cookie set with a secret must round-trip through the
    sign + parse path. Pop should also clear the cookie."""
    from aiohttp import web as _web
    secret = "test-secret-1234567890"
    response = _web.Response()
    set_flash(response, kind="success", message="Hi", secret=secret)
    cookie_header = response.cookies.get("meow_flash")
    assert cookie_header is not None
    raw_value = cookie_header.value

    # Build a fake request whose cookies carry the flash cookie.
    class _FakeApp(dict):
        pass
    app = _FakeApp()
    from web_admin import APP_KEY_SESSION_SECRET
    app[APP_KEY_SESSION_SECRET] = secret
    req = _web.Request.__new__(_web.Request)
    req.__dict__["_cookies"] = {"meow_flash": raw_value}
    # Patch the _cookies property via a lambda — simpler: build a
    # minimal stand-in object with the attributes we use.

    class _R:
        def __init__(self, cookies, app):
            self.cookies = cookies
            self.app = app

    r = _R({"meow_flash": raw_value}, app)
    response2 = _web.Response()
    flash = pop_flash(r, response2)
    assert flash == {"kind": "success", "message": "Hi"}
    # Cookie cleared on response.
    assert response2.cookies.get("meow_flash").value == ""


def test_flash_cookie_rejects_tampered_signature():
    secret = "test-secret-1234567890"
    response = web.Response()
    set_flash(response, kind="success", message="Hi", secret=secret)
    raw = response.cookies.get("meow_flash").value
    payload_b64, sig_b64 = raw.split(".", 1)
    # Flip a byte in the signature.
    tampered = payload_b64 + "." + ("A" + sig_b64[1:] if sig_b64[0] != "A" else "B" + sig_b64[1:])

    class _FakeApp(dict):
        pass
    from web_admin import APP_KEY_SESSION_SECRET
    app = _FakeApp()
    app[APP_KEY_SESSION_SECRET] = secret

    class _R:
        def __init__(self):
            self.cookies = {"meow_flash": tampered}
            self.app = app

    response2 = web.Response()
    assert pop_flash(_R(), response2) is None


# ---------------------------------------------------------------------
# Stage-8-Part-3: gift codes UI
# ---------------------------------------------------------------------


from web_admin import (
    EXPIRES_IN_DAYS_MAX,
    GIFT_AMOUNT_MAX,
    parse_gift_form,
)


# parse_gift_form (pure function)
# ---------------------------------------------------------------------


def test_parse_gift_form_happy_path():
    out = parse_gift_form(_form({
        "code": "birthday5",
        "amount_usd": "5",
    }))
    assert out == {
        "code": "BIRTHDAY5",
        "amount_usd": 5.0,
        "max_uses": None,
        "expires_in_days": None,
    }


def test_parse_gift_form_with_dollar_sign_and_caps():
    out = parse_gift_form(_form({
        "code": "Welcome_Gift",
        "amount_usd": "$10.50",
        "max_uses": "10",
        "expires_in_days": "30",
    }))
    assert out == {
        "code": "WELCOME_GIFT",
        "amount_usd": 10.5,
        "max_uses": 10,
        "expires_in_days": 30,
    }


def test_parse_gift_form_missing_code():
    assert parse_gift_form(_form({
        "amount_usd": "5",
    })) == "missing_code"


def test_parse_gift_form_bad_code():
    assert parse_gift_form(_form({
        "code": "has spaces",
        "amount_usd": "5",
    })) == "bad_code"


def test_parse_gift_form_code_too_long():
    assert parse_gift_form(_form({
        "code": "A" * 65,
        "amount_usd": "5",
    })) == "bad_code"


def test_parse_gift_form_missing_amount():
    assert parse_gift_form(_form({"code": "X"})) == "missing_amount"


def test_parse_gift_form_bad_amount_text():
    assert parse_gift_form(_form({
        "code": "X",
        "amount_usd": "abc",
    })) == "bad_amount"


def test_parse_gift_form_negative_amount():
    assert parse_gift_form(_form({
        "code": "X",
        "amount_usd": "-1",
    })) == "bad_amount"


def test_parse_gift_form_zero_amount():
    assert parse_gift_form(_form({
        "code": "X",
        "amount_usd": "0",
    })) == "bad_amount"


def test_parse_gift_form_nan_amount():
    assert parse_gift_form(_form({
        "code": "X",
        "amount_usd": "nan",
    })) == "bad_amount"


def test_parse_gift_form_inf_amount():
    assert parse_gift_form(_form({
        "code": "X",
        "amount_usd": "inf",
    })) == "bad_amount"


def test_parse_gift_form_amount_at_cap_ok():
    """Values exactly at GIFT_AMOUNT_MAX are accepted."""
    out = parse_gift_form(_form({
        "code": "X",
        "amount_usd": str(GIFT_AMOUNT_MAX),
    }))
    assert isinstance(out, dict)
    assert out["amount_usd"] == round(GIFT_AMOUNT_MAX, 4)


def test_parse_gift_form_amount_over_cap():
    assert parse_gift_form(_form({
        "code": "X",
        "amount_usd": "9999999",
    })) == "amount_too_large"


def test_parse_gift_form_bad_max_uses_text():
    assert parse_gift_form(_form({
        "code": "X",
        "amount_usd": "5",
        "max_uses": "abc",
    })) == "bad_max_uses"


def test_parse_gift_form_bad_max_uses_zero():
    assert parse_gift_form(_form({
        "code": "X",
        "amount_usd": "5",
        "max_uses": "0",
    })) == "bad_max_uses"


def test_parse_gift_form_bad_days_text():
    assert parse_gift_form(_form({
        "code": "X",
        "amount_usd": "5",
        "expires_in_days": "abc",
    })) == "bad_days"


def test_parse_gift_form_bad_days_negative():
    assert parse_gift_form(_form({
        "code": "X",
        "amount_usd": "5",
        "expires_in_days": "-3",
    })) == "bad_days"


# Bundled bug fix: oversize expires_in_days no longer crashes the
# create handler with OverflowError; the parser rejects it cleanly.
def test_parse_gift_form_days_too_large():
    assert parse_gift_form(_form({
        "code": "X",
        "amount_usd": "5",
        "expires_in_days": str(EXPIRES_IN_DAYS_MAX + 1),
    })) == "days_too_large"


def test_parse_gift_form_days_at_cap_ok():
    out = parse_gift_form(_form({
        "code": "X",
        "amount_usd": "5",
        "expires_in_days": str(EXPIRES_IN_DAYS_MAX),
    }))
    assert isinstance(out, dict)
    assert out["expires_in_days"] == EXPIRES_IN_DAYS_MAX


def test_parse_promo_form_days_too_large():
    """Same OverflowError fix mirrored on the promo form parser."""
    assert parse_promo_form(_form({
        "code": "X",
        "discount_kind": "percent",
        "discount_value": "10",
        "expires_in_days": str(EXPIRES_IN_DAYS_MAX + 1),
    })) == "days_too_large"


def test_parse_promo_form_days_at_cap_ok():
    out = parse_promo_form(_form({
        "code": "X",
        "discount_kind": "percent",
        "discount_value": "10",
        "expires_in_days": str(EXPIRES_IN_DAYS_MAX),
    }))
    assert isinstance(out, dict)
    assert out["expires_in_days"] == EXPIRES_IN_DAYS_MAX


# create_gift_code DB upper-bound guard (pure unit test)
# ---------------------------------------------------------------------


def test_create_gift_code_rejects_amount_over_decimal_cap():
    """DB layer must also reject amount_usd > DECIMAL(10,4) capacity
    so a hypothetical caller bypassing parse_gift_form (e.g. a future
    JSON API) can't trigger PG ``numeric field overflow``."""
    from database import Database
    db = Database.__new__(Database)
    db.pool = None
    import asyncio
    with pytest.raises(ValueError, match="DECIMAL"):
        asyncio.get_event_loop().run_until_complete(
            db.create_gift_code(
                code="X",
                amount_usd=1_000_000.0,
            )
        )


def test_create_gift_code_rejects_zero_amount():
    from database import Database
    db = Database.__new__(Database)
    db.pool = None
    import asyncio
    with pytest.raises(ValueError, match="positive"):
        asyncio.get_event_loop().run_until_complete(
            db.create_gift_code(code="X", amount_usd=0.0)
        )


def test_create_gift_code_rejects_negative_max_uses():
    from database import Database
    db = Database.__new__(Database)
    db.pool = None
    import asyncio
    with pytest.raises(ValueError, match="max_uses"):
        asyncio.get_event_loop().run_until_complete(
            db.create_gift_code(
                code="X", amount_usd=5.0, max_uses=-1,
            )
        )


# /admin/gifts GET
# ---------------------------------------------------------------------


async def test_gifts_get_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app())
    resp = await client.get("/admin/gifts", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")


async def test_gifts_get_lists_codes(aiohttp_client, make_admin_app):
    rows = [
        {
            "code": "BIRTHDAY5",
            "amount_usd": 5.0,
            "max_uses": 10,
            "used_count": 3,
            "expires_at": "2030-12-31T23:59:59+00:00",
            "is_active": True,
            "created_at": "2026-01-01T00:00:00+00:00",
        },
        {
            "code": "REVOKED_GIFT",
            "amount_usd": 1.5,
            "max_uses": None,
            "used_count": 0,
            "expires_at": None,
            "is_active": False,
            "created_at": "2025-01-01T00:00:00+00:00",
        },
    ]
    client = await aiohttp_client(
        make_admin_app(password="pw", db=_stub_db(gift_rows=rows))
    )
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/gifts")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "BIRTHDAY5" in body
    assert "$5.00" in body
    assert "3 /" in body  # used_count / max_uses
    assert "10" in body
    assert "REVOKED_GIFT" in body
    assert "$1.50" in body
    assert "active" in body
    assert "revoked" in body
    assert 'action="/admin/gifts/BIRTHDAY5/revoke"' in body


async def test_gifts_get_db_error_renders_banner(
    aiohttp_client, make_admin_app
):
    db = AsyncMock()
    db.list_gift_codes = AsyncMock(side_effect=RuntimeError("boom"))
    client = await aiohttp_client(
        make_admin_app(password="pw", db=db)
    )
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/gifts")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "Database query failed" in body


async def test_layout_has_gifts_nav_link(aiohttp_client, make_admin_app):
    """The sidebar should now show Gift codes as a real link, not
    the disabled coming-soon placeholder Stage-8-Part-1 left."""
    client = await aiohttp_client(make_admin_app(password="pw"))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/")
    body = await resp.text()
    assert 'href="/admin/gifts"' in body
    assert "Coming in Stage-8-Part-3" not in body


# /admin/gifts POST (create)
# ---------------------------------------------------------------------


async def _login_and_get_gift_csrf(client, password: str = "pw") -> str:
    """Log in, fetch the gifts page, scrape its CSRF token."""
    await client.post(
        "/admin/login", data={"password": password}, allow_redirects=False
    )
    resp = await client.get("/admin/gifts")
    body = await resp.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token in /admin/gifts form"
    return m.group(1)


async def test_gifts_create_happy_path(aiohttp_client, make_admin_app):
    db = _stub_db(create_gift_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_gift_csrf(client, "pw")

    resp = await client.post(
        "/admin/gifts",
        data={
            "csrf_token": csrf,
            "code": "BIRTHDAY5",
            "amount_usd": "5",
            "max_uses": "10",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/gifts"
    db.create_gift_code.assert_awaited_once()
    kwargs = db.create_gift_code.await_args.kwargs
    assert kwargs["code"] == "BIRTHDAY5"
    assert kwargs["amount_usd"] == 5.0
    assert kwargs["max_uses"] == 10
    assert kwargs["expires_at"] is None

    resp2 = await client.get("/admin/gifts")
    body = await resp2.text()
    assert "alert-success" in body
    assert "Created" in body and "BIRTHDAY5" in body


async def test_gifts_create_rejects_missing_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.post(
        "/admin/gifts",
        data={"code": "X", "amount_usd": "5"},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.create_gift_code.assert_not_awaited()
    resp2 = await client.get("/admin/gifts")
    body = await resp2.text()
    assert "CSRF" in body


async def test_gifts_create_rejects_wrong_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login_and_get_gift_csrf(client, "pw")
    resp = await client.post(
        "/admin/gifts",
        data={
            "csrf_token": "obviously-wrong",
            "code": "X",
            "amount_usd": "5",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.create_gift_code.assert_not_awaited()


async def test_gifts_create_validation_error_shows_flash(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_gift_csrf(client, "pw")
    resp = await client.post(
        "/admin/gifts",
        data={
            "csrf_token": csrf,
            "code": "BAD",
            "amount_usd": "-1",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.create_gift_code.assert_not_awaited()
    resp2 = await client.get("/admin/gifts")
    body = await resp2.text()
    assert "alert-error" in body
    assert "Amount must be" in body


async def test_gifts_create_oversized_days_does_not_crash(
    aiohttp_client, make_admin_app
):
    """The bundled bug fix: a huge expires_in_days no longer crashes
    the handler with OverflowError → 500. It now flashes a friendly
    error and re-renders the page."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_gift_csrf(client, "pw")
    resp = await client.post(
        "/admin/gifts",
        data={
            "csrf_token": csrf,
            "code": "BIG",
            "amount_usd": "5",
            "expires_in_days": str(10**18),  # would OverflowError timedelta
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.create_gift_code.assert_not_awaited()
    resp2 = await client.get("/admin/gifts")
    body = await resp2.text()
    assert "alert-error" in body
    assert "100 years" in body or "at most" in body


async def test_gifts_create_duplicate_shows_flash(
    aiohttp_client, make_admin_app
):
    db = _stub_db(create_gift_result=False)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_gift_csrf(client, "pw")
    resp = await client.post(
        "/admin/gifts",
        data={
            "csrf_token": csrf,
            "code": "BIRTHDAY5",
            "amount_usd": "5",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/gifts")
    body = await resp2.text()
    assert "alert-error" in body
    assert "already exists" in body


async def test_gifts_create_db_value_error_shows_flash(
    aiohttp_client, make_admin_app
):
    db = _stub_db(create_gift_result=ValueError("DECIMAL(10,4) limit"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_gift_csrf(client, "pw")
    resp = await client.post(
        "/admin/gifts",
        data={
            "csrf_token": csrf,
            "code": "BIG",
            "amount_usd": "100",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/gifts")
    body = await resp2.text()
    assert "alert-error" in body
    assert "DECIMAL" in body


async def test_gifts_create_expires_in_days_passes_through(
    aiohttp_client, make_admin_app
):
    db = _stub_db(create_gift_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_gift_csrf(client, "pw")
    resp = await client.post(
        "/admin/gifts",
        data={
            "csrf_token": csrf,
            "code": "EXPIRING",
            "amount_usd": "5",
            "expires_in_days": "7",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    kwargs = db.create_gift_code.await_args.kwargs
    assert kwargs["expires_at"] is not None
    delta = kwargs["expires_at"] - datetime.now(timezone.utc)
    assert timedelta(days=6, hours=23, minutes=58) < delta < timedelta(days=7, minutes=2)


# /admin/gifts/{code}/revoke POST
# ---------------------------------------------------------------------


async def test_gifts_revoke_happy_path(aiohttp_client, make_admin_app):
    db = _stub_db(revoke_gift_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_gift_csrf(client, "pw")
    resp = await client.post(
        "/admin/gifts/BIRTHDAY5/revoke",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.revoke_gift_code.assert_awaited_once_with("BIRTHDAY5")
    resp2 = await client.get("/admin/gifts")
    body = await resp2.text()
    assert "alert-success" in body
    assert "Revoked" in body and "BIRTHDAY5" in body


async def test_gifts_revoke_already_inactive_shows_info(
    aiohttp_client, make_admin_app
):
    db = _stub_db(revoke_gift_result=False)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_gift_csrf(client, "pw")
    resp = await client.post(
        "/admin/gifts/GHOST/revoke",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/gifts")
    body = await resp2.text()
    assert "alert-info" in body
    assert "already revoked" in body
    assert ("doesn't exist" in body) or ("doesn&#39;t exist" in body) \
        or ("doesn&#x27;t exist" in body)


async def test_gifts_revoke_rejects_missing_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_db(revoke_gift_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.post(
        "/admin/gifts/X/revoke",
        data={},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.revoke_gift_code.assert_not_awaited()


async def test_gifts_revoke_requires_auth(aiohttp_client, make_admin_app):
    db = _stub_db(revoke_gift_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    resp = await client.post(
        "/admin/gifts/X/revoke",
        data={"csrf_token": "anything"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")
    db.revoke_gift_code.assert_not_awaited()


async def test_gifts_revoke_invalid_url_code(aiohttp_client, make_admin_app):
    db = _stub_db(revoke_gift_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_gift_csrf(client, "pw")
    long_code = "A" * 80
    resp = await client.post(
        f"/admin/gifts/{long_code}/revoke",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.revoke_gift_code.assert_not_awaited()


# ---------------------------------------------------------------------
# Users page — Stage-8-Part-4
# ---------------------------------------------------------------------


from web_admin import (  # noqa: E402  (keep Part-4 imports grouped)
    ADJUST_MAX_USD,
    ADMIN_WEB_SENTINEL_ID,
    parse_adjust_form,
)


# parse_adjust_form unit tests -----------------------------------------


def test_parse_adjust_form_credit_happy():
    parsed = parse_adjust_form(
        {"action": "credit", "amount_usd": "5.25", "reason": "refund"}
    )
    assert parsed == {
        "action": "credit",
        "amount_usd": 5.25,
        "reason": "refund",
    }


def test_parse_adjust_form_debit_happy():
    parsed = parse_adjust_form(
        {"action": "debit", "amount_usd": "$12", "reason": "chargeback"}
    )
    assert parsed == {
        "action": "debit",
        "amount_usd": 12.0,
        "reason": "chargeback",
    }


def test_parse_adjust_form_bad_action_missing():
    assert parse_adjust_form(
        {"amount_usd": "1", "reason": "x"}
    ) == "bad_action"


def test_parse_adjust_form_bad_action_unknown():
    assert parse_adjust_form(
        {"action": "nuke", "amount_usd": "1", "reason": "x"}
    ) == "bad_action"


def test_parse_adjust_form_missing_amount():
    assert parse_adjust_form(
        {"action": "credit", "amount_usd": "", "reason": "x"}
    ) == "missing_amount"


def test_parse_adjust_form_bad_amount_non_number():
    assert parse_adjust_form(
        {"action": "credit", "amount_usd": "abc", "reason": "x"}
    ) == "bad_amount"


def test_parse_adjust_form_bad_amount_negative():
    assert parse_adjust_form(
        {"action": "credit", "amount_usd": "-5", "reason": "x"}
    ) == "bad_amount"


def test_parse_adjust_form_bad_amount_zero():
    assert parse_adjust_form(
        {"action": "credit", "amount_usd": "0", "reason": "x"}
    ) == "bad_amount"


def test_parse_adjust_form_bad_amount_nan():
    assert parse_adjust_form(
        {"action": "credit", "amount_usd": "nan", "reason": "x"}
    ) == "bad_amount"


def test_parse_adjust_form_bad_amount_inf():
    assert parse_adjust_form(
        {"action": "credit", "amount_usd": "inf", "reason": "x"}
    ) == "bad_amount"


def test_parse_adjust_form_amount_too_large():
    assert parse_adjust_form(
        {
            "action": "credit",
            "amount_usd": str(ADJUST_MAX_USD + 1),
            "reason": "x",
        }
    ) == "amount_too_large"


def test_parse_adjust_form_amount_at_cap_passes():
    parsed = parse_adjust_form(
        {
            "action": "credit",
            "amount_usd": str(ADJUST_MAX_USD),
            "reason": "x",
        }
    )
    assert isinstance(parsed, dict)
    assert parsed["amount_usd"] == ADJUST_MAX_USD


def test_parse_adjust_form_missing_reason():
    assert parse_adjust_form(
        {"action": "credit", "amount_usd": "5", "reason": ""}
    ) == "missing_reason"


def test_parse_adjust_form_reason_whitespace_only():
    assert parse_adjust_form(
        {"action": "credit", "amount_usd": "5", "reason": "   "}
    ) == "missing_reason"


def test_parse_adjust_form_reason_too_long():
    assert parse_adjust_form(
        {
            "action": "credit",
            "amount_usd": "5",
            "reason": "x" * 501,
        }
    ) == "bad_reason"


def test_parse_adjust_form_action_case_insensitive():
    parsed = parse_adjust_form(
        {"action": "CREDIT", "amount_usd": "1", "reason": "x"}
    )
    assert isinstance(parsed, dict)
    assert parsed["action"] == "credit"


# Integration tests -----------------------------------------------------


async def _login(client, password: str) -> None:
    resp = await client.post(
        "/admin/login", data={"password": password}, allow_redirects=False
    )
    assert resp.status == 302


async def _login_and_get_user_csrf(client, password: str, user_id: int) -> str:
    await _login(client, password)
    resp = await client.get(f"/admin/users/{user_id}")
    body = await resp.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token on user detail page"
    return m.group(1)


async def test_users_page_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app())
    resp = await client.get("/admin/users", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/login"


async def test_users_page_empty_query_shows_prompt(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users")
    assert resp.status == 200
    body = await resp.text()
    assert "Enter a Telegram id or username to search" in body
    db.search_users.assert_not_awaited()


async def test_users_page_search_by_username_renders_rows(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        search_users_result=[
            {
                "telegram_id": 11111111,
                "username": "kashlev",
                "balance_usd": 2.3456,
                "free_messages_left": 4,
                "language_code": "fa",
            },
            {
                "telegram_id": 22222222,
                "username": "kash2",
                "balance_usd": 0.0,
                "free_messages_left": 10,
                "language_code": "en",
            },
        ]
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users?q=kash")
    assert resp.status == 200
    body = await resp.text()
    assert "@kashlev" in body
    assert "@kash2" in body
    # Currency is formatted with 4dp (matches OpenRouter precision).
    assert "$2.3456" in body
    db.search_users.assert_awaited_once_with("kash", limit=50)


async def test_users_page_search_by_id(aiohttp_client, make_admin_app):
    db = _stub_db(
        search_users_result=[
            {
                "telegram_id": 12345,
                "username": None,
                "balance_usd": 0.0,
                "free_messages_left": 10,
                "language_code": "fa",
            }
        ]
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users?q=12345")
    assert resp.status == 200
    body = await resp.text()
    assert "12345" in body
    db.search_users.assert_awaited_once_with("12345", limit=50)


async def test_users_page_search_no_results(
    aiohttp_client, make_admin_app
):
    db = _stub_db(search_users_result=[])
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users?q=ghost")
    assert resp.status == 200
    body = await resp.text()
    assert "No users match" in body
    assert "ghost" in body


async def test_users_page_db_error_renders_banner(
    aiohttp_client, make_admin_app
):
    db = _stub_db(search_users_result=RuntimeError("boom"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users?q=anything")
    assert resp.status == 200
    body = await resp.text()
    assert "Database query failed" in body


async def test_user_detail_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app())
    resp = await client.get("/admin/users/123", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/login"


async def test_user_detail_bad_id_redirects_to_list(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users/not-an-int", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/users"
    db.get_user_admin_summary.assert_not_awaited()


async def test_user_detail_unknown_user_shows_empty_state(
    aiohttp_client, make_admin_app
):
    db = _stub_db(user_summary_result=None)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users/999")
    assert resp.status == 200
    body = await resp.text()
    assert "No user with id" in body


async def test_user_detail_renders_summary(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        user_summary_result={
            "telegram_id": 777,
            "username": "alice",
            "balance_usd": 42.5000,
            "free_messages_left": 3,
            "active_model": "openai/gpt-4o",
            "language_code": "en",
            "total_credited_usd": 100.0,
            "total_spent_usd": 57.5,
            "recent_transactions": [
                {
                    "id": 501,
                    "gateway": "nowpayments",
                    "currency": "USD",
                    "amount_usd": 10.0,
                    "status": "SUCCESS",
                    "created_at": "2026-04-28T09:00:00+00:00",
                    "notes": None,
                },
                {
                    "id": 502,
                    "gateway": "admin",
                    "currency": "USD",
                    "amount_usd": -5.0,
                    "status": "SUCCESS",
                    "created_at": "2026-04-28T10:00:00+00:00",
                    "notes": "[web] overcharge fix",
                },
            ],
        }
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users/777")
    assert resp.status == 200
    body = await resp.text()
    assert "@alice" in body
    assert "$42.5000" in body
    assert "openai/gpt-4o" in body
    # Positive and negative sign rendering with abs magnitude.
    assert "+$10.0000" in body
    assert "−$5.0000" in body
    assert "[web] overcharge fix" in body
    # CSRF token is injected into the adjust form.
    assert 'name="csrf_token"' in body
    db.get_user_admin_summary.assert_awaited_once_with(
        777, recent_tx_limit=20
    )


async def test_user_detail_db_error_shows_banner(
    aiohttp_client, make_admin_app
):
    db = _stub_db(user_summary_result=RuntimeError("boom"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users/888")
    assert resp.status == 200
    body = await resp.text()
    assert "Database query failed" in body


async def test_user_adjust_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app())
    resp = await client.post(
        "/admin/users/1/adjust",
        data={"csrf_token": "x", "action": "credit", "amount_usd": "1", "reason": "x"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")


async def test_user_adjust_bad_id_in_url(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.post(
        "/admin/users/abc/adjust",
        data={"csrf_token": "x", "action": "credit", "amount_usd": "1", "reason": "x"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/users"
    db.admin_adjust_balance.assert_not_awaited()


async def test_user_adjust_rejects_missing_csrf(
    aiohttp_client, make_admin_app
):
    # Need the get_user_admin_summary stub because the login bootstrap
    # navigates via /admin/users/{id} to grab the CSRF token first.
    db = _stub_db(
        user_summary_result={
            "telegram_id": 500,
            "username": "bob",
            "balance_usd": 10.0,
            "free_messages_left": 0,
            "active_model": "x",
            "language_code": "en",
            "total_credited_usd": 10.0,
            "total_spent_usd": 0.0,
            "recent_transactions": [],
        },
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.post(
        "/admin/users/500/adjust",
        data={"action": "credit", "amount_usd": "5", "reason": "x"},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.admin_adjust_balance.assert_not_awaited()
    # Redirect target is the detail page; follow to see the flash.
    resp2 = await client.get("/admin/users/500")
    body = await resp2.text()
    assert "CSRF" in body


async def test_user_adjust_validation_error_shows_flash(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        user_summary_result={
            "telegram_id": 500,
            "username": None,
            "balance_usd": 10.0,
            "free_messages_left": 0,
            "active_model": "x",
            "language_code": "en",
            "total_credited_usd": 10.0,
            "total_spent_usd": 0.0,
            "recent_transactions": [],
        },
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 500)
    resp = await client.post(
        "/admin/users/500/adjust",
        data={
            "csrf_token": csrf,
            "action": "credit",
            "amount_usd": "-1",
            "reason": "x",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.admin_adjust_balance.assert_not_awaited()
    resp2 = await client.get("/admin/users/500")
    body = await resp2.text()
    assert "alert-error" in body
    assert "positive number" in body


async def test_user_adjust_credit_happy_path(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        user_summary_result={
            "telegram_id": 500,
            "username": "bob",
            "balance_usd": 10.0,
            "free_messages_left": 0,
            "active_model": "x",
            "language_code": "en",
            "total_credited_usd": 10.0,
            "total_spent_usd": 0.0,
            "recent_transactions": [],
        },
        adjust_balance_result={
            "new_balance": 15.0,
            "transaction_id": 99,
            "delta": 5.0,
        },
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 500)
    resp = await client.post(
        "/admin/users/500/adjust",
        data={
            "csrf_token": csrf,
            "action": "credit",
            "amount_usd": "5",
            "reason": "refund",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/users/500"
    db.admin_adjust_balance.assert_awaited_once()
    kwargs = db.admin_adjust_balance.await_args.kwargs
    assert kwargs["telegram_id"] == 500
    assert kwargs["delta_usd"] == 5.0
    assert kwargs["reason"] == "[web] refund"
    assert kwargs["admin_telegram_id"] == ADMIN_WEB_SENTINEL_ID
    # Follow the redirect → success flash rendered.
    resp2 = await client.get("/admin/users/500")
    body = await resp2.text()
    assert "alert-success" in body
    assert "Credited" in body
    assert "Tx #99" in body


async def test_user_adjust_debit_sends_negative_delta(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        user_summary_result={
            "telegram_id": 500,
            "username": None,
            "balance_usd": 10.0,
            "free_messages_left": 0,
            "active_model": "x",
            "language_code": "en",
            "total_credited_usd": 10.0,
            "total_spent_usd": 0.0,
            "recent_transactions": [],
        },
        adjust_balance_result={
            "new_balance": 7.5,
            "transaction_id": 100,
            "delta": -2.5,
        },
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 500)
    resp = await client.post(
        "/admin/users/500/adjust",
        data={
            "csrf_token": csrf,
            "action": "debit",
            "amount_usd": "2.50",
            "reason": "chargeback",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    kwargs = db.admin_adjust_balance.await_args.kwargs
    assert kwargs["delta_usd"] == -2.5
    assert kwargs["reason"] == "[web] chargeback"


async def test_user_adjust_debit_insufficient_funds(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        user_summary_result={
            "telegram_id": 500,
            "username": None,
            "balance_usd": 1.0,
            "free_messages_left": 0,
            "active_model": "x",
            "language_code": "en",
            "total_credited_usd": 1.0,
            "total_spent_usd": 0.0,
            "recent_transactions": [],
        },
        adjust_balance_result=None,
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 500)
    resp = await client.post(
        "/admin/users/500/adjust",
        data={
            "csrf_token": csrf,
            "action": "debit",
            "amount_usd": "500",
            "reason": "oops",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/users/500")
    body = await resp2.text()
    assert "Refused" in body
    assert "below zero" in body


async def test_user_adjust_nonexistent_user(aiohttp_client, make_admin_app):
    # First GET for CSRF uses summary=<bob>, then adjust + follow-up
    # lookup both see None — we flip the mock's return_value mid-test.
    summary = {
        "telegram_id": 500,
        "username": None,
        "balance_usd": 0.0,
        "free_messages_left": 0,
        "active_model": "x",
        "language_code": "en",
        "total_credited_usd": 0.0,
        "total_spent_usd": 0.0,
        "recent_transactions": [],
    }
    db = _stub_db(
        user_summary_result=summary,
        adjust_balance_result=None,
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 500)
    # From now on pretend the user was deleted between the CSRF fetch
    # and the form submit — admin_adjust_balance returns None AND the
    # follow-up summary returns None.
    db.get_user_admin_summary.return_value = None
    resp = await client.post(
        "/admin/users/500/adjust",
        data={
            "csrf_token": csrf,
            "action": "credit",
            "amount_usd": "1",
            "reason": "x",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    # Follow redirect — now the detail page also sees None → empty state
    # with the "No user with id" banner plus the flash banner.
    resp2 = await client.get("/admin/users/500")
    body = await resp2.text()
    assert "No user with id 500" in body


async def test_user_adjust_db_exception_shows_flash(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        user_summary_result={
            "telegram_id": 500,
            "username": None,
            "balance_usd": 1.0,
            "free_messages_left": 0,
            "active_model": "x",
            "language_code": "en",
            "total_credited_usd": 1.0,
            "total_spent_usd": 0.0,
            "recent_transactions": [],
        },
        adjust_balance_result=RuntimeError("boom"),
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 500)
    resp = await client.post(
        "/admin/users/500/adjust",
        data={
            "csrf_token": csrf,
            "action": "credit",
            "amount_usd": "1",
            "reason": "x",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/users/500")
    body = await resp2.text()
    assert "Database write failed" in body


# =========================================================================
# Stage-8-Part-5: broadcast page tests
# =========================================================================

from web_admin import (  # noqa: E402  (keep Part-5 imports grouped)
    APP_KEY_BOT,
    APP_KEY_BROADCAST_JOBS,
    APP_KEY_BROADCAST_TASKS,
    BROADCAST_MAX_HISTORY,
    BROADCAST_TEXT_MAX_LEN,
    _new_broadcast_job,
    _run_broadcast_job,
    _store_broadcast_job,
    parse_broadcast_web_form,
)


# ---- parse_broadcast_web_form (unit tests) ------------------------------


def test_parse_broadcast_form_happy_path():
    out = parse_broadcast_web_form(
        {"text": "hello everyone", "only_active_days": ""}
    )
    assert out == {"text": "hello everyone", "only_active_days": None}


def test_parse_broadcast_form_strips_whitespace():
    out = parse_broadcast_web_form(
        {"text": "  hello  \n", "only_active_days": "  "}
    )
    assert out == {"text": "hello", "only_active_days": None}


def test_parse_broadcast_form_with_active_days():
    out = parse_broadcast_web_form(
        {"text": "tap tap", "only_active_days": "7"}
    )
    assert out == {"text": "tap tap", "only_active_days": 7}


def test_parse_broadcast_form_missing_text_returns_key():
    assert parse_broadcast_web_form(
        {"text": "", "only_active_days": ""}
    ) == "missing_text"
    assert parse_broadcast_web_form(
        {"text": "   ", "only_active_days": ""}
    ) == "missing_text"


def test_parse_broadcast_form_too_long_returns_key():
    big = "x" * (BROADCAST_TEXT_MAX_LEN + 1)
    assert parse_broadcast_web_form(
        {"text": big, "only_active_days": ""}
    ) == "text_too_long"


def test_parse_broadcast_form_at_max_length_passes():
    """Boundary test: exactly BROADCAST_TEXT_MAX_LEN chars must still pass."""
    msg = "x" * BROADCAST_TEXT_MAX_LEN
    out = parse_broadcast_web_form(
        {"text": msg, "only_active_days": ""}
    )
    assert out == {"text": msg, "only_active_days": None}


@pytest.mark.parametrize("bad", ["abc", "0", "-5", "1.5"])
def test_parse_broadcast_form_bad_active(bad):
    assert parse_broadcast_web_form(
        {"text": "hi", "only_active_days": bad}
    ) == "bad_active"


def test_parse_broadcast_form_limit_aligns_with_telegram_cmd():
    """The web form and the Telegram ``/admin_broadcast`` command must
    agree on the body-length ceiling so an admin can't craft a message
    that passes one validator and is rejected by the other. Caught a
    real drift risk — the constants live in different modules."""
    from admin import _BROADCAST_MAX_TEXT_LEN
    assert BROADCAST_TEXT_MAX_LEN == _BROADCAST_MAX_TEXT_LEN


def test_parse_broadcast_form_active_too_large():
    """Stage-8-Part-6 guard: mirrors the Telegram command — an
    active-days filter above ``BROADCAST_ACTIVE_DAYS_MAX`` would
    overflow PG's interval column. Reject at the form boundary.
    """
    from web_admin import BROADCAST_ACTIVE_DAYS_MAX

    # Exactly at the cap is allowed.
    out = parse_broadcast_web_form(
        {"text": "hi", "only_active_days": str(BROADCAST_ACTIVE_DAYS_MAX)}
    )
    assert isinstance(out, dict)
    assert out["only_active_days"] == BROADCAST_ACTIVE_DAYS_MAX

    # One past is rejected with the new error key.
    assert (
        parse_broadcast_web_form(
            {"text": "hi", "only_active_days": str(BROADCAST_ACTIVE_DAYS_MAX + 1)}
        )
        == "active_too_large"
    )
    # Nonsensical huge value also rejected (not silently dropped).
    assert (
        parse_broadcast_web_form(
            {"text": "hi", "only_active_days": "9999999999"}
        )
        == "active_too_large"
    )


def test_parse_broadcast_form_active_cap_aligns_with_telegram_cmd():
    """Sanity: the two parsers must cap at the same value or an admin
    could craft a filter that passes one and is refused by the other.
    """
    from admin import _BROADCAST_ACTIVE_DAYS_MAX
    from web_admin import BROADCAST_ACTIVE_DAYS_MAX

    assert BROADCAST_ACTIVE_DAYS_MAX == _BROADCAST_ACTIVE_DAYS_MAX


# ---- in-memory job lifecycle (unit tests) -------------------------------


def test_new_broadcast_job_shape():
    job = _new_broadcast_job(text="hello", only_active_days=None)
    # id is opaque but non-empty and URL-safe.
    assert isinstance(job["id"], str) and job["id"]
    assert job["state"] == "queued"
    assert job["total"] == 0
    assert job["sent"] == 0 and job["blocked"] == 0 and job["failed"] == 0
    assert job["text_preview"] == "hello"
    assert job["full_text_len"] == 5
    assert job["only_active_days"] is None
    assert job["error"] is None


def test_new_broadcast_job_truncates_preview_for_long_text():
    msg = "x" * 500
    job = _new_broadcast_job(text=msg, only_active_days=30)
    assert job["full_text_len"] == 500
    assert len(job["text_preview"]) == 118  # 117 chars + ellipsis
    assert job["text_preview"].endswith("…")
    assert job["only_active_days"] == 30


def test_new_broadcast_job_ids_are_unique():
    ids = {
        _new_broadcast_job(text="x", only_active_days=None)["id"]
        for _ in range(50)
    }
    assert len(ids) == 50  # overwhelmingly likely with 6 bytes of randomness


def test_store_broadcast_job_never_evicts_live_jobs():
    """Running / queued jobs must survive eviction even past the cap.
    Otherwise a backlog of pending broadcasts could be silently dropped
    mid-run — a correctness bug, not just a UX bug."""
    app = web.Application()
    app[APP_KEY_BROADCAST_JOBS] = {}
    # Fill with terminal jobs.
    for _ in range(BROADCAST_MAX_HISTORY):
        j = _new_broadcast_job(text="done", only_active_days=None)
        j["state"] = "completed"
        _store_broadcast_job(app, j)
    # Now add a running job past the cap.
    live = _new_broadcast_job(text="active", only_active_days=None)
    live["state"] = "running"
    _store_broadcast_job(app, live)
    assert live["id"] in app[APP_KEY_BROADCAST_JOBS]
    # Stays present even under further pressure.
    for _ in range(5):
        extra = _new_broadcast_job(text="more", only_active_days=None)
        extra["state"] = "completed"
        _store_broadcast_job(app, extra)
    assert live["id"] in app[APP_KEY_BROADCAST_JOBS]
    assert len(app[APP_KEY_BROADCAST_JOBS]) <= BROADCAST_MAX_HISTORY


def test_store_broadcast_job_evicts_oldest_terminal_first():
    app = web.Application()
    app[APP_KEY_BROADCAST_JOBS] = {}
    ordered_ids: list[str] = []
    for _ in range(BROADCAST_MAX_HISTORY):
        j = _new_broadcast_job(text="done", only_active_days=None)
        j["state"] = "completed"
        _store_broadcast_job(app, j)
        ordered_ids.append(j["id"])
    # One more terminal pushes the cap — oldest must be evicted.
    newest = _new_broadcast_job(text="next", only_active_days=None)
    newest["state"] = "completed"
    _store_broadcast_job(app, newest)
    assert ordered_ids[0] not in app[APP_KEY_BROADCAST_JOBS]
    assert newest["id"] in app[APP_KEY_BROADCAST_JOBS]
    assert len(app[APP_KEY_BROADCAST_JOBS]) == BROADCAST_MAX_HISTORY


async def test_run_broadcast_job_marks_failed_without_bot(make_admin_app):
    """Defence in depth: the background task must surface a failure
    rather than silently 'complete' when the bot is missing."""
    app = make_admin_app()  # bot=None by default
    job = _new_broadcast_job(text="hi", only_active_days=None)
    _store_broadcast_job(app, job)
    await _run_broadcast_job(app=app, job=job, text="hi")
    assert job["state"] == "failed"
    assert "Background task launched without" in job["error"]
    assert job["completed_at"] is not None


async def test_run_broadcast_job_completes_empty_recipients(make_admin_app):
    """No recipients ⇒ job transitions to 'completed' without calling
    the bot. Exercises the early-return path in _run_broadcast_job."""
    db = _stub_db(broadcast_recipients=[])
    bot = AsyncMock()
    app = make_admin_app(db=db, bot=bot)
    job = _new_broadcast_job(text="hi", only_active_days=None)
    _store_broadcast_job(app, job)
    await _run_broadcast_job(app=app, job=job, text="hi")
    assert job["state"] == "completed"
    assert job["total"] == 0
    # Bot was never touched.
    bot.send_message.assert_not_called()


async def test_run_broadcast_job_db_failure_marks_failed(make_admin_app):
    db = _stub_db(broadcast_recipients=RuntimeError("pool closed"))
    bot = AsyncMock()
    app = make_admin_app(db=db, bot=bot)
    job = _new_broadcast_job(text="hi", only_active_days=None)
    _store_broadcast_job(app, job)
    await _run_broadcast_job(app=app, job=job, text="hi")
    assert job["state"] == "failed"
    assert "pool closed" in job["error"]


async def test_run_broadcast_job_happy_path_sends_all(make_admin_app):
    """End-to-end in-process: the background task should hand each
    recipient id to bot.send_message and update the job counters as
    admin._do_broadcast progresses."""
    recipients = [100, 200, 300]
    db = _stub_db(broadcast_recipients=recipients)
    bot = AsyncMock()
    bot.send_message = AsyncMock(return_value=None)
    app = make_admin_app(db=db, bot=bot)
    job = _new_broadcast_job(text="hello", only_active_days=None)
    _store_broadcast_job(app, job)

    # Patch the delay so the test isn't stuck in 40ms sleeps.
    import admin
    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0
    try:
        await _run_broadcast_job(app=app, job=job, text="hello")
    finally:
        admin._BROADCAST_DELAY_S = orig_delay

    assert job["state"] == "completed"
    assert job["total"] == 3
    assert job["sent"] == 3
    assert job["blocked"] == 0
    assert job["failed"] == 0
    assert bot.send_message.await_count == 3
    # Recipients are passed positionally by chat_id kwarg — assert the
    # set, not the order, since _do_broadcast preserves the DB order.
    called_ids = {
        c.kwargs["chat_id"] for c in bot.send_message.await_args_list
    }
    assert called_ids == {100, 200, 300}


# ---- /admin/broadcast page integration ----------------------------------


async def _login_and_get_broadcast_csrf(client, password: str) -> str:
    await _login(client, password)
    resp = await client.get("/admin/broadcast")
    body = await resp.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token on broadcast page"
    return m.group(1)


async def test_broadcast_page_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app())
    resp = await client.get("/admin/broadcast", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/login"


async def test_broadcast_page_renders_form_and_empty_list(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(
        make_admin_app(password="pw", bot=AsyncMock())
    )
    await _login(client, "pw")
    resp = await client.get("/admin/broadcast")
    assert resp.status == 200
    body = await resp.text()
    assert "New broadcast" in body
    assert "No broadcasts sent yet" in body
    # Form fields
    assert 'name="text"' in body
    assert 'name="only_active_days"' in body
    assert 'name="csrf_token"' in body


async def test_broadcast_post_rejects_missing_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(
        make_admin_app(password="pw", db=db, bot=AsyncMock())
    )
    await _login(client, "pw")
    resp = await client.post(
        "/admin/broadcast",
        data={"text": "hi"},
        allow_redirects=False,
    )
    assert resp.status == 302
    # Flash redirect back to /admin/broadcast (not the detail page).
    assert resp.headers["Location"] == "/admin/broadcast"
    db.iter_broadcast_recipients.assert_not_awaited()
    # Follow the redirect and confirm the banner.
    resp2 = await client.get("/admin/broadcast")
    body = await resp2.text()
    assert "CSRF" in body


async def test_broadcast_post_empty_text_flashes_error(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(
        make_admin_app(password="pw", db=db, bot=AsyncMock())
    )
    csrf = await _login_and_get_broadcast_csrf(client, "pw")
    resp = await client.post(
        "/admin/broadcast",
        data={"csrf_token": csrf, "text": "   "},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/broadcast"
    db.iter_broadcast_recipients.assert_not_awaited()
    resp2 = await client.get("/admin/broadcast")
    body = await resp2.text()
    assert "Broadcast body is required" in body


async def test_broadcast_post_too_long_flashes_error(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(
        make_admin_app(password="pw", db=db, bot=AsyncMock())
    )
    csrf = await _login_and_get_broadcast_csrf(client, "pw")
    resp = await client.post(
        "/admin/broadcast",
        data={
            "csrf_token": csrf,
            "text": "x" * (BROADCAST_TEXT_MAX_LEN + 1),
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/broadcast")
    body = await resp2.text()
    assert f"at most {BROADCAST_TEXT_MAX_LEN} characters" in body


async def test_broadcast_post_bad_active_flashes_error(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(
        make_admin_app(password="pw", db=db, bot=AsyncMock())
    )
    csrf = await _login_and_get_broadcast_csrf(client, "pw")
    resp = await client.post(
        "/admin/broadcast",
        data={
            "csrf_token": csrf,
            "text": "hi",
            "only_active_days": "abc",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/broadcast")
    body = await resp2.text()
    assert "positive integer" in body


async def test_broadcast_post_without_bot_flashes_error(
    aiohttp_client, make_admin_app
):
    """bot=None (misconfigured deploy) must refuse to start a job."""
    db = _stub_db(broadcast_recipients=[1, 2, 3])
    client = await aiohttp_client(
        make_admin_app(password="pw", db=db, bot=None)
    )
    csrf = await _login_and_get_broadcast_csrf(client, "pw")
    resp = await client.post(
        "/admin/broadcast",
        data={"csrf_token": csrf, "text": "hello"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/broadcast"
    db.iter_broadcast_recipients.assert_not_awaited()
    resp2 = await client.get("/admin/broadcast")
    body = await resp2.text()
    assert "Bot is not wired up" in body


async def test_broadcast_post_happy_path_redirects_to_detail(
    aiohttp_client, make_admin_app
):
    """A valid POST should 302 to /admin/broadcast/<job_id> and leave
    a job dict in the app registry."""
    # No recipients so the background task completes immediately and
    # doesn't stick around polluting later tests.
    db = _stub_db(broadcast_recipients=[])
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)
    client = await aiohttp_client(app)
    csrf = await _login_and_get_broadcast_csrf(client, "pw")
    resp = await client.post(
        "/admin/broadcast",
        data={
            "csrf_token": csrf,
            "text": "hello world",
            "only_active_days": "7",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    location = resp.headers["Location"]
    assert location.startswith("/admin/broadcast/")
    job_id = location.rsplit("/", 1)[-1]

    # Allow the background task to run to completion (no recipients
    # so this is effectively instant).
    task = app[APP_KEY_BROADCAST_TASKS].get(job_id)
    assert task is not None
    await task

    jobs = app[APP_KEY_BROADCAST_JOBS]
    assert job_id in jobs
    job = jobs[job_id]
    assert job["only_active_days"] == 7
    assert job["state"] == "completed"
    assert job["total"] == 0


async def test_broadcast_detail_page_renders_for_known_job(
    aiohttp_client, make_admin_app
):
    db = _stub_db(broadcast_recipients=[])
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)
    client = await aiohttp_client(app)
    csrf = await _login_and_get_broadcast_csrf(client, "pw")

    post = await client.post(
        "/admin/broadcast",
        data={"csrf_token": csrf, "text": "hi there"},
        allow_redirects=False,
    )
    job_id = post.headers["Location"].rsplit("/", 1)[-1]
    await app[APP_KEY_BROADCAST_TASKS][job_id]

    resp = await client.get(f"/admin/broadcast/{job_id}")
    assert resp.status == 200
    body = await resp.text()
    assert job_id in body
    # State pill contains the final state somewhere in the page.
    assert "completed" in body
    assert "hi there" in body  # preview is rendered


async def test_broadcast_detail_unknown_job_redirects_with_flash(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(
        make_admin_app(password="pw", bot=AsyncMock())
    )
    await _login(client, "pw")
    resp = await client.get(
        "/admin/broadcast/does-not-exist", allow_redirects=False
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/broadcast"
    resp2 = await client.get("/admin/broadcast")
    body = await resp2.text()
    assert "Unknown broadcast job" in body


async def test_broadcast_status_unknown_job_returns_404_json(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(
        make_admin_app(password="pw", bot=AsyncMock())
    )
    await _login(client, "pw")
    resp = await client.get("/admin/broadcast/nope/status")
    assert resp.status == 404
    data = await resp.json()
    assert data == {"error": "unknown_job", "job_id": "nope"}


async def test_broadcast_status_returns_job_snapshot(
    aiohttp_client, make_admin_app
):
    db = _stub_db(broadcast_recipients=[])
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)
    client = await aiohttp_client(app)
    csrf = await _login_and_get_broadcast_csrf(client, "pw")

    post = await client.post(
        "/admin/broadcast",
        data={"csrf_token": csrf, "text": "snapshot"},
        allow_redirects=False,
    )
    job_id = post.headers["Location"].rsplit("/", 1)[-1]
    await app[APP_KEY_BROADCAST_TASKS][job_id]

    resp = await client.get(f"/admin/broadcast/{job_id}/status")
    assert resp.status == 200
    data = await resp.json()
    assert data["id"] == job_id
    assert data["state"] == "completed"
    assert data["total"] == 0
    # The preview is persisted (not the full text).
    assert data["text_preview"] == "snapshot"


# ---------------------------------------------------------------------
# Stage-8-Part-6 — transactions browser
# ---------------------------------------------------------------------


from web_admin import (  # noqa: E402  (keep Part-6 imports grouped)
    TRANSACTIONS_PER_PAGE_CHOICES,
    TRANSACTIONS_PER_PAGE_DEFAULT,
    TRANSACTIONS_PER_PAGE_MAX,
    _encode_tx_query,
    parse_transactions_query,
)
from multidict import MultiDict  # noqa: E402


# ---- parse_transactions_query (unit tests) ---------------------------


def test_parse_tx_query_defaults():
    out = parse_transactions_query(MultiDict())
    assert out == {
        "gateway": None,
        "status": None,
        "telegram_id": None,
        "page": 1,
        "per_page": TRANSACTIONS_PER_PAGE_DEFAULT,
    }


def test_parse_tx_query_happy_path():
    q = MultiDict(
        [
            ("gateway", "nowpayments"),
            ("status", "SUCCESS"),
            ("telegram_id", "42"),
            ("page", "3"),
            ("per_page", "100"),
        ]
    )
    out = parse_transactions_query(q)
    assert out == {
        "gateway": "nowpayments",
        "status": "SUCCESS",
        "telegram_id": 42,
        "page": 3,
        "per_page": 100,
    }


def test_parse_tx_query_drops_unknown_gateway_and_status():
    """Unknown enum values silently become ``None`` rather than
    bubbling a 500 — the handler re-renders as unfiltered. Tested
    with an SQL-injection-ish string to pin the allow-list.
    """
    q = MultiDict(
        [
            ("gateway", "paypal"),
            ("status", "x' OR 1=1 --"),
        ]
    )
    out = parse_transactions_query(q)
    assert out["gateway"] is None
    assert out["status"] is None


def test_parse_tx_query_drops_non_integer_telegram_id():
    q = MultiDict([("telegram_id", "not-a-number")])
    out = parse_transactions_query(q)
    assert out["telegram_id"] is None


def test_parse_tx_query_clamps_page_and_per_page():
    q = MultiDict([("page", "-10"), ("per_page", "9999")])
    out = parse_transactions_query(q)
    assert out["page"] == 1
    assert out["per_page"] == TRANSACTIONS_PER_PAGE_MAX

    q = MultiDict([("page", "garbage"), ("per_page", "junk")])
    out = parse_transactions_query(q)
    assert out["page"] == 1
    assert out["per_page"] == TRANSACTIONS_PER_PAGE_DEFAULT


def test_parse_tx_query_strips_whitespace_on_enum_values():
    q = MultiDict([("gateway", "  nowpayments  "), ("status", "  SUCCESS  ")])
    out = parse_transactions_query(q)
    assert out["gateway"] == "nowpayments"
    assert out["status"] == "SUCCESS"


def test_encode_tx_query_omits_defaults():
    """Default filters shouldn't pollute the URL — makes the
    "Reset" link compare equal to the unfiltered landing page.
    """
    filters = {
        "gateway": None,
        "status": None,
        "telegram_id": None,
        "page": 1,
        "per_page": TRANSACTIONS_PER_PAGE_DEFAULT,
    }
    assert _encode_tx_query(filters) == ""


def test_encode_tx_query_round_trips_filters():
    filters = {
        "gateway": "nowpayments",
        "status": "SUCCESS",
        "telegram_id": 42,
        "page": 2,
        "per_page": 100,
    }
    encoded = _encode_tx_query(filters)
    # The re-parse must produce an equivalent filter dict.
    from urllib.parse import parse_qsl

    re_parsed = parse_transactions_query(MultiDict(parse_qsl(encoded)))
    assert re_parsed["gateway"] == "nowpayments"
    assert re_parsed["status"] == "SUCCESS"
    assert re_parsed["telegram_id"] == 42
    assert re_parsed["page"] == 2
    assert re_parsed["per_page"] == 100


def test_encode_tx_query_page_override():
    filters = {"gateway": None, "status": None, "telegram_id": None,
               "page": 5, "per_page": 50}
    assert "page=3" in _encode_tx_query(filters, page=3)
    # page=1 is the default — should be dropped by the encoder.
    assert "page=" not in _encode_tx_query(filters, page=1)


# ---- transactions handler (integration) ------------------------------


async def test_transactions_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app(password="pw"))
    resp = await client.get("/admin/transactions", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")


async def test_transactions_renders_empty_state(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        list_transactions_result={
            "rows": [],
            "total": 0,
            "page": 1,
            "per_page": 50,
            "total_pages": 0,
        }
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/transactions")
    assert resp.status == 200
    body = await resp.text()
    assert "No transactions match" in body
    # Sidebar link is now enabled + active.
    assert 'href="/admin/transactions"' in body
    # Filter dropdowns are populated.
    for gw in ("nowpayments", "admin", "gift"):
        assert f'value="{gw}"' in body
    for st in ("PENDING", "SUCCESS", "PARTIAL", "FAILED", "EXPIRED", "REFUNDED"):
        assert f'value="{st}"' in body


async def test_transactions_renders_rows_and_pagination(
    aiohttp_client, make_admin_app
):
    rows = [
        {
            "id": 101,
            "telegram_id": 7,
            "gateway": "nowpayments",
            "currency": "USDT",
            "amount_crypto_or_rial": 1.0,
            "amount_usd": 9.99,
            "status": "SUCCESS",
            "gateway_invoice_id": "inv-1",
            "created_at": "2026-04-28T12:00:00+00:00",
            "completed_at": "2026-04-28T12:05:00+00:00",
            "notes": None,
        },
        {
            "id": 102,
            "telegram_id": 8,
            "gateway": "admin",
            "currency": "USD",
            "amount_crypto_or_rial": None,
            "amount_usd": -2.5,
            "status": "SUCCESS",
            "gateway_invoice_id": None,
            "created_at": "2026-04-28T11:00:00+00:00",
            "completed_at": "2026-04-28T11:00:00+00:00",
            "notes": "[web] refund for stuck invoice",
        },
    ]
    db = _stub_db(
        list_transactions_result={
            "rows": rows,
            "total": 120,
            "page": 2,
            "per_page": 50,
            "total_pages": 3,  # 120 / 50 → 3 pages
        }
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/transactions?page=2")
    assert resp.status == 200
    body = await resp.text()
    # Both rows rendered.
    assert ">101<" in body and ">102<" in body
    # Debit styling: negative amount.
    assert "-$2.5000" in body
    # Positive amount formatted to 4 decimals.
    assert "$9.9900" in body
    # Pager shows current position + total.
    assert "Page 2 of 3" in body
    assert "120 row(s)" in body
    # Prev link points to page 1 (param dropped → bare URL), next to page 3.
    assert 'href="/admin/transactions"' in body
    assert "page=3" in body


async def test_transactions_forwards_filters_to_db(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get(
        "/admin/transactions"
        "?gateway=nowpayments&status=SUCCESS&telegram_id=42&page=2&per_page=25"
    )
    assert resp.status == 200
    db.list_transactions.assert_awaited_once_with(
        gateway="nowpayments",
        status="SUCCESS",
        telegram_id=42,
        page=2,
        per_page=25,
    )


async def test_transactions_handles_list_transactions_value_error(
    aiohttp_client, make_admin_app
):
    """Belt-and-suspenders: if list_transactions raises ValueError
    (e.g. a future DB-layer validation catches what the parser
    missed), the page renders as empty rather than 500ing.
    """
    db = _stub_db(list_transactions_result=ValueError("boom"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/transactions")
    assert resp.status == 200
    body = await resp.text()
    assert "No transactions match" in body


async def test_transactions_bad_filter_values_ignored_not_500(
    aiohttp_client, make_admin_app
):
    """Unknown enum values in the query string must not crash —
    the handler re-renders as unfiltered (matching the pattern
    documented on ``parse_transactions_query``).
    """
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get(
        "/admin/transactions?gateway=bogus&status=nope&telegram_id=abc"
    )
    assert resp.status == 200
    # Filters were dropped before reaching the DB layer.
    db.list_transactions.assert_awaited_once_with(
        gateway=None,
        status=None,
        telegram_id=None,
        page=1,
        per_page=TRANSACTIONS_PER_PAGE_DEFAULT,
    )


async def test_transactions_per_page_choices_are_dropdown_values():
    """Every value shipped in ``TRANSACTIONS_PER_PAGE_CHOICES`` must
    satisfy the per-page clamp — otherwise the dropdown would
    silently render an option that down-clamps when submitted.
    """
    assert all(
        1 <= c <= TRANSACTIONS_PER_PAGE_MAX
        for c in TRANSACTIONS_PER_PAGE_CHOICES
    )
    # The default is one of the options (otherwise selecting it
    # doesn't round-trip through the form).
    assert TRANSACTIONS_PER_PAGE_DEFAULT in TRANSACTIONS_PER_PAGE_CHOICES


async def test_transactions_links_user_column_to_user_detail(
    aiohttp_client, make_admin_app
):
    """The telegram_id column must link through to /admin/users/{id}
    so the admin can jump from a tx row to the full wallet view and
    credit/debit form.
    """
    db = _stub_db(
        list_transactions_result={
            "rows": [
                {
                    "id": 1, "telegram_id": 5555, "gateway": "nowpayments",
                    "currency": "USDT", "amount_crypto_or_rial": 1.0,
                    "amount_usd": 5.0, "status": "SUCCESS",
                    "gateway_invoice_id": "inv",
                    "created_at": "2026-04-28T00:00:00+00:00",
                    "completed_at": None, "notes": None,
                }
            ],
            "total": 1, "page": 1, "per_page": 50, "total_pages": 1,
        }
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/transactions")
    body = await resp.text()
    assert 'href="/admin/users/5555"' in body


async def test_transactions_null_telegram_id_renders_em_dash(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        list_transactions_result={
            "rows": [
                {
                    "id": 1, "telegram_id": None, "gateway": "nowpayments",
                    "currency": "USDT", "amount_crypto_or_rial": None,
                    "amount_usd": 0.0, "status": "PENDING",
                    "gateway_invoice_id": None, "created_at": None,
                    "completed_at": None, "notes": None,
                }
            ],
            "total": 1, "page": 1, "per_page": 50, "total_pages": 1,
        }
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/transactions")
    body = await resp.text()
    # No crash + orphaned row renders with dash rather than a
    # broken link.
    assert "/admin/users/None" not in body

