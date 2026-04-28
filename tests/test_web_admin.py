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


def _stub_db(metrics: dict | None = None):
    """A minimal Database stub that exposes the methods web_admin uses."""
    db = AsyncMock()
    db.get_system_metrics = AsyncMock(
        return_value=metrics
        or {
            "user_count": 42,
            "active_users_7d": 7,
            "total_revenue_usd": 123.45,
            "total_spend_usd": 67.89,
            "top_models": [
                {
                    "model": "openrouter/auto",
                    "request_count": 100,
                    "total_cost_usd": 3.21,
                }
            ],
        }
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
    ):
        app = web.Application()
        setup_admin_routes(
            app,
            db=db if db is not None else _stub_db(),
            password=password,
            session_secret=session_secret,
            ttl_hours=24,
            cookie_secure=cookie_secure,
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
            "user_count": 1234,
            "active_users_7d": 5,
            "total_revenue_usd": 99.50,
            "total_spend_usd": 12.34,
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
    assert "$12.34" in body
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
