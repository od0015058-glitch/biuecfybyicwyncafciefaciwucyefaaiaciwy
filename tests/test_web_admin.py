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
