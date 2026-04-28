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

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

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
    string_overrides_result: dict | Exception | None = None,
    upsert_string_result: object | Exception = None,
    delete_string_result: bool | Exception = True,
    audit_log_result: list | Exception | None = None,
    record_audit_result: object | Exception = 1,
    update_user_fields_result: dict | None | Exception = None,
    user_usage_result: dict | Exception | None = None,
    user_usage_aggregates_result: dict | Exception | None = None,
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
    # Stage-9-Step-1.6: bot_strings.
    if isinstance(string_overrides_result, Exception):
        db.load_all_string_overrides = AsyncMock(
            side_effect=string_overrides_result
        )
    else:
        db.load_all_string_overrides = AsyncMock(
            return_value=string_overrides_result
            if string_overrides_result is not None
            else {}
        )
    if isinstance(upsert_string_result, Exception):
        db.upsert_string_override = AsyncMock(
            side_effect=upsert_string_result
        )
    else:
        db.upsert_string_override = AsyncMock(
            return_value=upsert_string_result
        )
    if isinstance(delete_string_result, Exception):
        db.delete_string_override = AsyncMock(
            side_effect=delete_string_result
        )
    else:
        db.delete_string_override = AsyncMock(
            return_value=delete_string_result
        )
    # Stage-9-Step-2: admin_audit_log + user-field editor.
    if isinstance(audit_log_result, Exception):
        db.list_admin_audit_log = AsyncMock(side_effect=audit_log_result)
    else:
        db.list_admin_audit_log = AsyncMock(
            return_value=audit_log_result
            if audit_log_result is not None else []
        )
    if isinstance(record_audit_result, Exception):
        db.record_admin_audit = AsyncMock(side_effect=record_audit_result)
    else:
        db.record_admin_audit = AsyncMock(return_value=record_audit_result)
    if isinstance(update_user_fields_result, Exception):
        db.update_user_admin_fields = AsyncMock(
            side_effect=update_user_fields_result
        )
    else:
        db.update_user_admin_fields = AsyncMock(
            return_value=update_user_fields_result
        )
    # Stage-9-Step-8: per-user usage browser.
    default_usage = {
        "rows": [],
        "total": 0,
        "page": 1,
        "per_page": 50,
        "total_pages": 0,
    }
    if isinstance(user_usage_result, Exception):
        db.list_user_usage_logs = AsyncMock(side_effect=user_usage_result)
    else:
        db.list_user_usage_logs = AsyncMock(
            return_value=user_usage_result
            if user_usage_result is not None
            else default_usage
        )
    default_aggregates = {
        "total_calls": 0, "total_tokens": 0, "total_cost_usd": 0.0,
    }
    if isinstance(user_usage_aggregates_result, Exception):
        db.get_user_usage_aggregates = AsyncMock(
            side_effect=user_usage_aggregates_result
        )
    else:
        db.get_user_usage_aggregates = AsyncMock(
            return_value=user_usage_aggregates_result
            if user_usage_aggregates_result is not None
            else default_aggregates
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
        totp_secret: str = "",
        totp_issuer: str = "Meowassist Admin",
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
            totp_secret=totp_secret,
            totp_issuer=totp_issuer,
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
# Stage-9-Step-3: bundled bug fix — whitespace-only credentials
# ---------------------------------------------------------------------


def test_setup_admin_routes_rejects_whitespace_password(make_admin_app):
    """Whitespace-only ADMIN_PASSWORD is the documented deploy typo.

    Pre-fix the value would be stored verbatim and every login attempt
    silently rejected as "Wrong password" — operators could spend
    hours debugging a stray space in their .env. Now it raises at
    boot with a clear message.
    """
    with pytest.raises(ValueError, match="ADMIN_PASSWORD"):
        make_admin_app(password="   ")


def test_setup_admin_routes_rejects_whitespace_session_secret(
    make_admin_app,
):
    """Same fail-fast rule for ADMIN_SESSION_SECRET."""
    with pytest.raises(ValueError, match="ADMIN_SESSION_SECRET"):
        make_admin_app(password="letmein", session_secret="\t\n ")


def test_setup_admin_routes_accepts_truly_empty_credentials(
    make_admin_app,
):
    """Empty (not whitespace) creds keep the documented dev path:
    panel installs, login refuses. This pins the back-compat boundary.
    """
    # Should not raise.
    app = make_admin_app(password="", session_secret="")
    from web_admin import APP_KEY_PASSWORD, APP_KEY_SESSION_SECRET
    assert app[APP_KEY_PASSWORD] == ""
    assert app[APP_KEY_SESSION_SECRET] == ""


# ---------------------------------------------------------------------
# Stage-9-Step-3: TOTP / 2FA pure-helper tests
# ---------------------------------------------------------------------


def test_validate_totp_secret_empty_returns_empty():
    from web_admin import validate_totp_secret

    assert validate_totp_secret("") == ""
    assert validate_totp_secret("   ") == ""
    assert validate_totp_secret("\t\n") == ""


def test_validate_totp_secret_normalizes_whitespace_and_case():
    """A copy-pasted authenticator-app secret like
    ``"abcd efgh ijkl mnop"`` should validate and uppercase cleanly.
    """
    from web_admin import validate_totp_secret

    norm = validate_totp_secret("abcd efgh ijkl mnop")
    assert norm == "ABCDEFGHIJKLMNOP"


def test_validate_totp_secret_rejects_short_input():
    """< 16 base32 chars = < 80 bits of entropy = brute-forceable."""
    from web_admin import validate_totp_secret

    with pytest.raises(ValueError, match="at least 16"):
        validate_totp_secret("ABCDEFG")


def test_validate_totp_secret_rejects_invalid_base32():
    """Non-base32 chars (1, 8, 9, 0, lowercase l, etc) must fail."""
    from web_admin import validate_totp_secret

    with pytest.raises(ValueError, match="not a valid base32"):
        # 16 chars but '!' is not in the base32 alphabet.
        validate_totp_secret("ABCDEFGHIJKLMNO!")


def test_verify_totp_code_accepts_current():
    import pyotp
    from web_admin import verify_totp_code

    secret = pyotp.random_base32()
    code = pyotp.TOTP(secret).now()
    assert verify_totp_code(secret, code) is True


def test_verify_totp_code_strips_whitespace():
    """Authenticators show codes as ``"123 456"`` for readability;
    we should accept that.
    """
    import pyotp
    from web_admin import verify_totp_code

    secret = pyotp.random_base32()
    code = pyotp.TOTP(secret).now()
    assert verify_totp_code(secret, f"{code[:3]} {code[3:]}") is True


def test_verify_totp_code_rejects_empty():
    from web_admin import verify_totp_code

    assert verify_totp_code("ABCDEFGHIJKLMNOP", "") is False
    assert verify_totp_code("ABCDEFGHIJKLMNOP", "   ") is False


def test_verify_totp_code_rejects_non_digits():
    from web_admin import verify_totp_code

    assert verify_totp_code("ABCDEFGHIJKLMNOP", "abc123") is False
    assert verify_totp_code("ABCDEFGHIJKLMNOP", "12345") is False  # 5 digits
    assert verify_totp_code("ABCDEFGHIJKLMNOP", "1234567") is False  # 7 digits


def test_verify_totp_code_rejects_wrong_code():
    import pyotp
    from web_admin import verify_totp_code

    secret = pyotp.random_base32()
    # 000000 is statistically unlikely to be the current code; if it is,
    # 999999 won't be — try both.
    now = pyotp.TOTP(secret).now()
    bad = "000000" if now != "000000" else "999999"
    assert verify_totp_code(secret, bad) is False


def test_verify_totp_code_swallows_pyotp_errors_returns_false(monkeypatch):
    """If pyotp raises, we refuse the code (don't 500 the request)."""
    import pyotp
    from web_admin import verify_totp_code

    class _Boom:
        def verify(self, *_a, **_k):
            raise RuntimeError("simulated pyotp failure")

    monkeypatch.setattr(pyotp, "TOTP", lambda secret: _Boom())
    assert verify_totp_code("ABCDEFGHIJKLMNOP", "123456") is False


def test_setup_admin_routes_rejects_invalid_totp_secret(make_admin_app):
    """Invalid base32 in ADMIN_2FA_SECRET fails at boot — the app must
    not start with a half-broken 2FA config.
    """
    with pytest.raises(ValueError, match="ADMIN_2FA_SECRET"):
        make_admin_app(totp_secret="not-base32!!!")


def test_setup_admin_routes_normalizes_totp_secret(make_admin_app):
    """A copy-pasted spaced secret is normalized at boot, so the
    ``APP_KEY_TOTP_SECRET`` value is the canonical base32 string
    every downstream consumer expects.
    """
    from web_admin import APP_KEY_TOTP_SECRET

    app = make_admin_app(totp_secret="abcd efgh ijkl mnop")
    assert app[APP_KEY_TOTP_SECRET] == "ABCDEFGHIJKLMNOP"


# ---------------------------------------------------------------------
# Stage-9-Step-3: TOTP / 2FA login flow integration
# ---------------------------------------------------------------------


async def test_login_form_omits_2fa_field_when_disabled(
    aiohttp_client, make_admin_app,
):
    """No ADMIN_2FA_SECRET → login form is password-only."""
    client = await aiohttp_client(make_admin_app())
    resp = await client.get("/admin/login")
    body = await resp.text()
    assert 'name="password"' in body
    assert 'name="code"' not in body


async def test_login_form_includes_2fa_field_when_enabled(
    aiohttp_client, make_admin_app,
):
    """Configured ADMIN_2FA_SECRET → login form prompts for a code."""
    client = await aiohttp_client(
        make_admin_app(totp_secret="ABCDEFGHIJKLMNOP")
    )
    resp = await client.get("/admin/login")
    body = await resp.text()
    assert 'name="password"' in body
    assert 'name="code"' in body
    # Hint to mobile keyboards / password managers so the right
    # autofill kicks in.
    assert 'autocomplete="one-time-code"' in body


async def test_login_with_2fa_rejects_missing_code(
    aiohttp_client, make_admin_app,
):
    """Right password but no code → 401 with no cookie."""
    client = await aiohttp_client(
        make_admin_app(password="letmein", totp_secret="ABCDEFGHIJKLMNOP")
    )
    resp = await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    assert resp.status == 401
    body = await resp.text()
    assert "Invalid 2FA code" in body
    assert COOKIE_NAME not in resp.cookies


async def test_login_with_2fa_rejects_bad_code(
    aiohttp_client, make_admin_app,
):
    """Right password + obviously wrong 6-digit → 401."""
    client = await aiohttp_client(
        make_admin_app(password="letmein", totp_secret="ABCDEFGHIJKLMNOP")
    )
    resp = await client.post(
        "/admin/login",
        data={"password": "letmein", "code": "000000"},
        allow_redirects=False,
    )
    # 000000 is overwhelmingly unlikely to be the current TOTP for a
    # fixed secret — the test depends on that probabilistic argument.
    # In the ~1-in-a-million case it passes, the test is harmless.
    assert resp.status == 401
    assert COOKIE_NAME not in resp.cookies


async def test_login_with_2fa_accepts_valid_code(
    aiohttp_client, make_admin_app,
):
    """Right password + the live TOTP code → 302 + cookie set."""
    import pyotp

    secret = "ABCDEFGHIJKLMNOP"
    client = await aiohttp_client(
        make_admin_app(password="letmein", totp_secret=secret)
    )
    code = pyotp.TOTP(secret).now()
    resp = await client.post(
        "/admin/login",
        data={"password": "letmein", "code": code},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/"
    assert COOKIE_NAME in resp.cookies


async def test_login_with_2fa_runs_after_password_compare(
    aiohttp_client, make_admin_app,
):
    """Wrong password + valid code → 401 "Wrong password" (NOT
    "Invalid 2FA code"). This pins the deliberate ordering: an
    attacker without the password can't probe the 2FA code in
    isolation.
    """
    import pyotp

    secret = "ABCDEFGHIJKLMNOP"
    client = await aiohttp_client(
        make_admin_app(password="letmein", totp_secret=secret)
    )
    code = pyotp.TOTP(secret).now()
    resp = await client.post(
        "/admin/login",
        data={"password": "WRONG", "code": code},
        allow_redirects=False,
    )
    assert resp.status == 401
    body = await resp.text()
    assert "Wrong password" in body
    # The form still includes the "2FA code" label (the field is
    # always rendered when 2FA is enabled), but the error banner
    # itself must NOT mention 2FA — that would tell an attacker the
    # password was right.
    assert "Invalid 2FA code" not in body


async def test_login_without_2fa_ignores_submitted_code(
    aiohttp_client, make_admin_app,
):
    """When ADMIN_2FA_SECRET is unset, an extra ``code`` form field
    is silently ignored — back-compat for legacy form posts.
    """
    client = await aiohttp_client(make_admin_app(password="letmein"))
    resp = await client.post(
        "/admin/login",
        data={"password": "letmein", "code": "anything"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert COOKIE_NAME in resp.cookies


# ---------------------------------------------------------------------
# Stage-9-Step-3: /admin/enroll_2fa
# ---------------------------------------------------------------------


async def test_enroll_2fa_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app())
    resp = await client.get("/admin/enroll_2fa", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/login"


async def test_enroll_2fa_renders_qr_when_disabled(
    aiohttp_client, make_admin_app,
):
    """No configured secret → page suggests a fresh one + renders QR."""
    client = await aiohttp_client(make_admin_app(password="letmein"))
    await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    resp = await client.get("/admin/enroll_2fa")
    assert resp.status == 200
    body = await resp.text()
    # Suggestion banner is shown.
    assert "currently disabled" in body
    # SVG QR is inlined.
    assert "<svg" in body
    # otpauth URI is rendered for manual import.
    assert "otpauth://totp/" in body


async def test_enroll_2fa_shows_configured_secret(
    aiohttp_client, make_admin_app,
):
    """Configured secret → page shows it (operator re-pairing a device)
    rather than generating a new suggestion.
    """
    secret = "ABCDEFGHIJKLMNOP"
    client = await aiohttp_client(
        make_admin_app(password="letmein", totp_secret=secret)
    )
    await client.post(
        "/admin/login",
        data={
            "password": "letmein",
            "code": __import__("pyotp").TOTP(secret).now(),
        },
        allow_redirects=False,
    )
    resp = await client.get("/admin/enroll_2fa")
    assert resp.status == 200
    body = await resp.text()
    assert "currently enabled" in body
    # The configured secret is rendered (chunked for readability) so
    # check for any 4-char chunk of it.
    assert "ABCD" in body


async def test_enroll_2fa_suggestion_changes_each_load(
    aiohttp_client, make_admin_app,
):
    """Each load with no secret gets a fresh suggestion — pins the
    "don't cache the suggestion server-side" property.
    """
    import re

    client = await aiohttp_client(make_admin_app(password="letmein"))
    await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    resp1 = await client.get("/admin/enroll_2fa")
    resp2 = await client.get("/admin/enroll_2fa")
    body1 = await resp1.text()
    body2 = await resp2.text()
    # Pull the otpauth secret= query param out of each URI.
    sec1 = re.search(r"otpauth://totp/[^?]+\?secret=([A-Z0-9]+)", body1)
    sec2 = re.search(r"otpauth://totp/[^?]+\?secret=([A-Z0-9]+)", body2)
    assert sec1 and sec2
    assert sec1.group(1) != sec2.group(1)


async def test_login_2fa_audit_trail_records_deny_reason(
    aiohttp_client, make_admin_app,
):
    """A bad/missing 2FA code records ``login_deny`` with the reason
    in ``meta`` so an operator reading /admin/audit can tell apart
    rate-limited / bad-password / missing-2fa / bad-2fa.
    """
    db = _stub_db()
    client = await aiohttp_client(
        make_admin_app(
            password="letmein",
            totp_secret="ABCDEFGHIJKLMNOP",
            db=db,
        )
    )
    await client.post(
        "/admin/login",
        data={"password": "letmein"},  # no code
        allow_redirects=False,
    )
    db.record_admin_audit.assert_awaited()
    # Find the call that recorded the deny — most recent kwargs.
    last = db.record_admin_audit.await_args
    assert last.kwargs["action"] == "login_deny"
    assert last.kwargs["meta"] == {"reason": "missing_2fa"}


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
    # Stage-9-Step-7 unified format_usd: minus is the ASCII ``-``
    # placed BEFORE the dollar sign; positive numbers get a leading
    # ``+`` sign-marker.
    assert "+$10.0000" in body
    assert "-$5.0000" in body
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



# ---------------------------------------------------------------------
# Stage-9-Step-1.6: /admin/strings (editable bot text)
# ---------------------------------------------------------------------


async def test_strings_get_renders_compiled_table(
    aiohttp_client, make_admin_app
):
    """The list page renders one row per (lang, key) in the compiled
    table. Slugs we know exist must appear; default badge is shown
    when no override exists."""
    db = _stub_db(string_overrides_result={})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/strings")
    assert resp.status == 200
    body = await resp.text()
    # Slug from the compiled table that ships with the bot.
    assert "hub_btn_wallet" in body
    # Status badge for non-overridden rows.
    assert "default" in body
    # Sidebar link is active on this page.
    assert 'class="active"' in body and "Bot text" in body
    # Sanity: lang badge appears (per row).
    assert "lang-badge" in body


async def test_strings_get_filters_by_lang_and_search(
    aiohttp_client, make_admin_app
):
    db = _stub_db(string_overrides_result={})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/strings?lang=en&q=memory")
    assert resp.status == 200
    body = await resp.text()
    # Lang filter clamps to en — so a known fa-only-display row's
    # row-table render should not include the same slug under fa.
    # Easier check: the filter form should round-trip the parameters.
    assert 'value="memory"' in body
    assert '<option value="en" selected>en</option>' in body


async def test_strings_get_marks_overridden_rows(
    aiohttp_client, make_admin_app
):
    """When an override exists, the row shows the override badge and
    serves the override value rather than the compiled default."""
    overrides = {("en", "hub_btn_wallet"): "💰 Custom Wallet Label"}
    db = _stub_db(string_overrides_result=overrides)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/strings?lang=en&q=hub_btn_wallet")
    body = await resp.text()
    assert "Custom Wallet Label" in body
    assert "badge-override" in body


async def test_strings_get_handles_db_error(
    aiohttp_client, make_admin_app
):
    """A DB failure on the list page must render a banner — not 500."""
    db = _stub_db(string_overrides_result=RuntimeError("pool down"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/strings")
    assert resp.status == 200
    body = await resp.text()
    assert "Database query failed" in body


async def test_strings_detail_get_renders_form(
    aiohttp_client, make_admin_app
):
    db = _stub_db(string_overrides_result={})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/strings/en/hub_btn_wallet")
    assert resp.status == 200
    body = await resp.text()
    # Form points at the right action URL.
    assert 'action="/admin/strings/en/hub_btn_wallet"' in body
    # Compiled default block + textarea both rendered.
    assert "Compiled default" in body
    assert "<textarea" in body
    # Revert button is disabled when no override exists.
    assert "Revert to default" in body
    assert "disabled" in body


async def test_strings_detail_get_unknown_lang_404(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/strings/zh/anything")
    assert resp.status == 404


async def test_strings_detail_get_unknown_key_404(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/strings/en/this_slug_does_not_exist_anywhere")
    assert resp.status == 404


async def _login_and_get_strings_csrf(
    client, password: str, lang: str = "en", key: str = "hub_btn_wallet"
) -> str:
    """Log in, fetch the per-string editor, scrape the CSRF token."""
    await _login(client, password)
    resp = await client.get(f"/admin/strings/{lang}/{key}")
    body = await resp.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token in /admin/strings/<lang>/<key> form"
    return m.group(1)


async def test_strings_save_post_happy_path(
    aiohttp_client, make_admin_app
):
    """A save POST upserts the (lang, key, value) row, refreshes the
    in-memory cache, and redirects back to the editor with a success
    flash."""
    import strings as bot_strings_module

    # Reset module-level override cache so this test's assertion is
    # independent of any prior state.
    bot_strings_module.set_overrides({})

    db = _stub_db()
    # On the post-write refresh the DB returns the new override.
    db.load_all_string_overrides = AsyncMock(
        side_effect=[
            {},  # initial detail GET
            {("en", "hub_btn_wallet"): "💰 Custom"},  # post-save refresh
        ]
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")

    resp = await client.post(
        "/admin/strings/en/hub_btn_wallet",
        data={"csrf_token": csrf, "value": "💰 Custom"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/strings/en/hub_btn_wallet"
    db.upsert_string_override.assert_awaited_once_with(
        "en", "hub_btn_wallet", "💰 Custom", updated_by="web"
    )
    # Cache was refreshed and the in-memory override is now visible.
    assert bot_strings_module.get_override("en", "hub_btn_wallet") == "💰 Custom"

    # Reset to keep the test isolated from later tests.
    bot_strings_module.set_overrides({})


async def test_strings_save_post_strips_whitespace(
    aiohttp_client, make_admin_app
):
    """The override is trimmed before persistence — Telegram strips
    leading/trailing whitespace on inline-button text anyway."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    await client.post(
        "/admin/strings/en/hub_btn_wallet",
        data={"csrf_token": csrf, "value": "   padded   "},
        allow_redirects=False,
    )
    db.upsert_string_override.assert_awaited_once_with(
        "en", "hub_btn_wallet", "padded", updated_by="web"
    )


async def test_strings_save_post_rejects_empty_value(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    resp = await client.post(
        "/admin/strings/en/hub_btn_wallet",
        data={"csrf_token": csrf, "value": "   "},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_string_override.assert_not_awaited()


async def test_strings_save_post_rejects_oversize(
    aiohttp_client, make_admin_app
):
    """Submitting a value longer than the cap is rejected with a flash —
    not 500'd by some downstream length check in the DB layer."""
    from web_admin import STRING_OVERRIDE_MAX_CHARS

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    too_big = "x" * (STRING_OVERRIDE_MAX_CHARS + 1)
    resp = await client.post(
        "/admin/strings/en/hub_btn_wallet",
        data={"csrf_token": csrf, "value": too_big},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_string_override.assert_not_awaited()


async def test_strings_save_post_rejects_missing_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.post(
        "/admin/strings/en/hub_btn_wallet",
        data={"value": "hello"},  # no csrf
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_string_override.assert_not_awaited()


async def test_strings_save_post_rejects_unknown_lang(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    resp = await client.post(
        "/admin/strings/zh/hub_btn_wallet",
        data={"csrf_token": csrf, "value": "x"},
        allow_redirects=False,
    )
    assert resp.status == 404
    db.upsert_string_override.assert_not_awaited()


async def test_strings_save_post_rejects_unknown_key(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    resp = await client.post(
        "/admin/strings/en/this_slug_does_not_exist",
        data={"csrf_token": csrf, "value": "x"},
        allow_redirects=False,
    )
    assert resp.status == 404
    db.upsert_string_override.assert_not_awaited()


async def test_strings_save_post_db_error_shows_flash(
    aiohttp_client, make_admin_app
):
    db = _stub_db(upsert_string_result=RuntimeError("disk full"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    resp = await client.post(
        "/admin/strings/en/hub_btn_wallet",
        data={"csrf_token": csrf, "value": "x"},
        allow_redirects=False,
    )
    assert resp.status == 302
    # Follow the redirect; the editor renders the error flash.
    resp2 = await client.get("/admin/strings/en/hub_btn_wallet")
    body = await resp2.text()
    assert "Database write failed" in body


async def test_strings_revert_post_happy_path(
    aiohttp_client, make_admin_app
):
    import strings as bot_strings_module

    # Pre-state: the override is currently set.
    bot_strings_module.set_overrides(
        {("en", "hub_btn_wallet"): "💰 Custom"}
    )

    db = _stub_db(delete_string_result=True)
    # Detail GET sees the override; post-revert refresh sees nothing.
    db.load_all_string_overrides = AsyncMock(
        side_effect=[
            {("en", "hub_btn_wallet"): "💰 Custom"},
            {},
        ]
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    resp = await client.post(
        "/admin/strings/en/hub_btn_wallet/revert",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_string_override.assert_awaited_once_with("en", "hub_btn_wallet")
    # Cache was refreshed — override is gone.
    assert bot_strings_module.get_override("en", "hub_btn_wallet") is None

    bot_strings_module.set_overrides({})


async def test_strings_revert_post_rejects_missing_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.post(
        "/admin/strings/en/hub_btn_wallet/revert",
        data={},  # no csrf
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_string_override.assert_not_awaited()


async def test_strings_revert_post_no_override_flashes_info(
    aiohttp_client, make_admin_app
):
    """Reverting when there's no override is not an error — the flash
    just informs the operator that there was nothing to do."""
    db = _stub_db(delete_string_result=False)
    db.load_all_string_overrides = AsyncMock(return_value={})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    resp = await client.post(
        "/admin/strings/en/hub_btn_wallet/revert",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/strings/en/hub_btn_wallet")
    body = await resp2.text()
    assert "nothing to revert" in body


async def test_strings_routes_require_auth(
    aiohttp_client, make_admin_app
):
    """All four endpoints redirect to login when unauthed."""
    client = await aiohttp_client(make_admin_app(password="pw"))
    for path, method in [
        ("/admin/strings", "get"),
        ("/admin/strings/en/hub_btn_wallet", "get"),
        ("/admin/strings/en/hub_btn_wallet", "post"),
        ("/admin/strings/en/hub_btn_wallet/revert", "post"),
    ]:
        if method == "get":
            resp = await client.get(path, allow_redirects=False)
        else:
            resp = await client.post(path, data={}, allow_redirects=False)
        assert resp.status == 302
        assert resp.headers["Location"] == "/admin/login"


# ---------------------------------------------------------------------
# Stage-9-Step-2: user-field editor + audit log
# ---------------------------------------------------------------------

# A canonical full user_summary fixture for the user-edit tests. Always
# spread + override per-test rather than mutating in place.
_BASE_USER_SUMMARY = {
    "telegram_id": 777,
    "username": "alice",
    "balance_usd": 42.5,
    "free_messages_left": 3,
    "active_model": "openai/gpt-4o",
    "language_code": "en",
    "memory_enabled": False,
    "total_credited_usd": 100.0,
    "total_spent_usd": 57.5,
    "recent_transactions": [],
}


async def test_user_detail_renders_edit_form(
    aiohttp_client, make_admin_app
):
    db = _stub_db(user_summary_result={**_BASE_USER_SUMMARY})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp = await client.get("/admin/users/777")
    assert resp.status == 200
    body = await resp.text()
    assert "Edit user fields" in body
    # Language dropdown gets pre-selected.
    assert 'value="en"\n              selected' in body or 'value="en" selected' in body
    # Active model input rendered.
    assert 'value="openai/gpt-4o"' in body
    # Memory checkbox NOT pre-checked when False.
    assert 'name="memory_enabled" value="on"\n' in body
    assert 'memory_enabled" value="on"\n              checked' not in body
    # Free messages numeric pre-filled.
    assert 'value="3"' in body
    # Username pre-filled.
    assert 'value="alice"' in body
    # Sentinel field present.
    assert 'name="memory_enabled_present"' in body


async def test_user_edit_post_happy_path_changes_language(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        user_summary_result={**_BASE_USER_SUMMARY},
        update_user_fields_result={"changed": {"language_code": "fa"}},
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)

    resp = await client.post(
        "/admin/users/777/edit",
        data={
            "csrf_token": csrf,
            "language_code": "fa",
            "active_model": "openai/gpt-4o",
            "memory_enabled_present": "1",
            # memory_enabled omitted = unchecked = False (matches current)
            "free_messages_left": "3",
            "username": "alice",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/users/777"
    db.update_user_admin_fields.assert_awaited_once()
    call = db.update_user_admin_fields.await_args
    assert call.args == (777,)
    assert call.kwargs == {"fields": {"language_code": "fa"}}
    db.record_admin_audit.assert_awaited()
    audit_call = db.record_admin_audit.await_args
    assert audit_call.kwargs["action"] == "user_edit"
    assert audit_call.kwargs["target"] == "user:777"
    assert audit_call.kwargs["meta"] == {"changed": {"language_code": "fa"}}


async def test_user_edit_post_no_changes_flashes_info(
    aiohttp_client, make_admin_app
):
    """Resubmitting the form with every field unchanged is a no-op
    flash, NOT a DB write — keeps the audit log and txn history clean."""
    db = _stub_db(user_summary_result={**_BASE_USER_SUMMARY})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)

    resp = await client.post(
        "/admin/users/777/edit",
        data={
            "csrf_token": csrf,
            "language_code": "en",
            "active_model": "openai/gpt-4o",
            "memory_enabled_present": "1",
            "free_messages_left": "3",
            "username": "alice",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_not_awaited()
    user_edit_calls = [
        c for c in db.record_admin_audit.await_args_list
        if c.kwargs.get("action") == "user_edit"
    ]
    assert user_edit_calls == []


async def test_user_edit_post_toggles_memory_on(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        user_summary_result={**_BASE_USER_SUMMARY, "memory_enabled": False},
        update_user_fields_result={"changed": {"memory_enabled": True}},
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)

    resp = await client.post(
        "/admin/users/777/edit",
        data={
            "csrf_token": csrf,
            "memory_enabled_present": "1",
            "memory_enabled": "on",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_awaited_once_with(
        777, fields={"memory_enabled": True},
    )


async def test_user_edit_post_clears_username(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        user_summary_result={**_BASE_USER_SUMMARY},
        update_user_fields_result={"changed": {"username": None}},
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)

    resp = await client.post(
        "/admin/users/777/edit",
        data={
            "csrf_token": csrf,
            "memory_enabled_present": "1",
            "username": "",  # explicitly cleared
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_awaited_once_with(
        777, fields={"username": None},
    )


async def test_user_edit_post_strips_at_prefix_from_username(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        user_summary_result={**_BASE_USER_SUMMARY},
        update_user_fields_result={"changed": {"username": "bob"}},
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)

    resp = await client.post(
        "/admin/users/777/edit",
        data={
            "csrf_token": csrf,
            "memory_enabled_present": "1",
            "username": "@bob",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_awaited_once_with(
        777, fields={"username": "bob"},
    )


async def test_user_edit_post_rejects_unknown_lang(
    aiohttp_client, make_admin_app
):
    db = _stub_db(user_summary_result={**_BASE_USER_SUMMARY})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)

    resp = await client.post(
        "/admin/users/777/edit",
        data={
            "csrf_token": csrf,
            "memory_enabled_present": "1",
            "language_code": "zh",  # not in SUPPORTED_LANGUAGES
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_not_awaited()


async def test_user_edit_post_rejects_bad_model_id(
    aiohttp_client, make_admin_app
):
    db = _stub_db(user_summary_result={**_BASE_USER_SUMMARY})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)

    resp = await client.post(
        "/admin/users/777/edit",
        data={
            "csrf_token": csrf,
            "memory_enabled_present": "1",
            "active_model": "no-slash-here",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_not_awaited()


async def test_user_edit_post_rejects_negative_free_messages(
    aiohttp_client, make_admin_app
):
    db = _stub_db(user_summary_result={**_BASE_USER_SUMMARY})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)

    resp = await client.post(
        "/admin/users/777/edit",
        data={
            "csrf_token": csrf,
            "memory_enabled_present": "1",
            "free_messages_left": "-5",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_not_awaited()


async def test_user_edit_post_rejects_oversize_free_messages(
    aiohttp_client, make_admin_app
):
    db = _stub_db(user_summary_result={**_BASE_USER_SUMMARY})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)

    resp = await client.post(
        "/admin/users/777/edit",
        data={
            "csrf_token": csrf,
            "memory_enabled_present": "1",
            "free_messages_left": "9999999999",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_not_awaited()


async def test_user_edit_post_rejects_username_with_spaces(
    aiohttp_client, make_admin_app
):
    db = _stub_db(user_summary_result={**_BASE_USER_SUMMARY})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)

    resp = await client.post(
        "/admin/users/777/edit",
        data={
            "csrf_token": csrf,
            "memory_enabled_present": "1",
            "username": "bad name",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_not_awaited()


async def test_user_edit_post_requires_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_db(user_summary_result={**_BASE_USER_SUMMARY})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.post(
        "/admin/users/777/edit",
        data={"language_code": "fa", "memory_enabled_present": "1"},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_not_awaited()


async def test_user_edit_post_user_not_found(
    aiohttp_client, make_admin_app
):
    db = _stub_db(user_summary_result=None)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    # We can't fetch CSRF from the detail page when summary is None,
    # so log in via promos like the other patterns and reuse that.
    csrf = await _login_and_get_csrf(client, "pw")

    resp = await client.post(
        "/admin/users/12345/edit",
        data={
            "csrf_token": csrf,
            "memory_enabled_present": "1",
            "language_code": "fa",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_not_awaited()


async def test_user_edit_post_invalid_user_id_redirects_to_users(
    aiohttp_client, make_admin_app
):
    """Bad URL segment shouldn't 500 — silently bounce to the list.
    Mirrors ``user_adjust_post`` and ``user_detail_get`` behaviour."""
    db = _stub_db(user_summary_result={**_BASE_USER_SUMMARY})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.post(
        "/admin/users/notanumber/edit",
        data={"csrf_token": "x"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/users"
    db.update_user_admin_fields.assert_not_awaited()


async def test_user_edit_post_audit_failure_does_not_block_save(
    aiohttp_client, make_admin_app
):
    """An audit-write failure must not cause the underlying user-edit
    to fail or roll back. The flash should still be ``success``."""
    db = _stub_db(
        user_summary_result={**_BASE_USER_SUMMARY},
        update_user_fields_result={"changed": {"language_code": "fa"}},
        record_audit_result=RuntimeError("audit pool down"),
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)

    resp = await client.post(
        "/admin/users/777/edit",
        data={
            "csrf_token": csrf,
            "memory_enabled_present": "1",
            "language_code": "fa",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.update_user_admin_fields.assert_awaited_once()


# ---------------------------------------------------------------------
# /admin/audit
# ---------------------------------------------------------------------


async def test_audit_get_renders_rows(aiohttp_client, make_admin_app):
    db = _stub_db(audit_log_result=[
        {
            "id": 1,
            "ts": "2026-04-28T09:00:00+00:00",
            "actor": "web",
            "action": "user_adjust",
            "target": "user:777",
            "ip": "203.0.113.10",
            "outcome": "ok",
            "meta": {"delta_usd": 5.0},
        },
        {
            "id": 2,
            "ts": "2026-04-28T08:59:00+00:00",
            "actor": "web",
            "action": "login_deny",
            "target": None,
            "ip": "203.0.113.10",
            "outcome": "deny",
            "meta": {"reason": "bad_password"},
        },
    ])
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/audit")
    assert resp.status == 200
    body = await resp.text()
    assert "user:777" in body
    assert "Wallet credit / debit" in body  # action label
    assert "Login (denied)" in body
    db.list_admin_audit_log.assert_awaited_once_with(
        limit=200, action=None, actor=None,
    )


async def test_audit_get_passes_filters(aiohttp_client, make_admin_app):
    db = _stub_db(audit_log_result=[])
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/audit?action=user_adjust&actor=web")
    assert resp.status == 200
    db.list_admin_audit_log.assert_awaited_once_with(
        limit=200, action="user_adjust", actor="web",
    )


async def test_audit_get_handles_db_error(
    aiohttp_client, make_admin_app
):
    db = _stub_db(audit_log_result=RuntimeError("pool exhausted"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/audit")
    assert resp.status == 200
    body = await resp.text()
    assert "Database query failed" in body


async def test_audit_get_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app(password="pw"))
    resp = await client.get("/admin/audit", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/login"


async def test_user_edit_route_requires_auth(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="pw"))
    resp = await client.post(
        "/admin/users/777/edit", data={}, allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/login"


# ---------------------------------------------------------------------
# Audit hook coverage on existing handlers
# ---------------------------------------------------------------------


async def test_login_post_records_audit_on_success(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    resp = await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    assert resp.status == 302
    db.record_admin_audit.assert_awaited()
    actions = [c.kwargs["action"] for c in db.record_admin_audit.await_args_list]
    assert "login_ok" in actions


async def test_login_post_records_audit_on_bad_password(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    resp = await client.post(
        "/admin/login", data={"password": "wrong"}, allow_redirects=False,
    )
    assert resp.status == 401
    actions = [c.kwargs["action"] for c in db.record_admin_audit.await_args_list]
    assert "login_deny" in actions


async def test_user_adjust_post_records_audit_on_success(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        user_summary_result={**_BASE_USER_SUMMARY},
        adjust_balance_result={
            "new_balance": 47.5, "transaction_id": 999, "delta": 5.0,
        },
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_user_csrf(client, "pw", 777)
    resp = await client.post(
        "/admin/users/777/adjust",
        data={
            "csrf_token": csrf,
            "action": "credit",
            "amount_usd": "5",
            "reason": "manual top-up",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    actions = [c.kwargs["action"] for c in db.record_admin_audit.await_args_list]
    assert "user_adjust" in actions


async def test_promos_create_records_audit(
    aiohttp_client, make_admin_app
):
    db = _stub_db(create_promo_result=True)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_csrf(client, "pw")
    resp = await client.post(
        "/admin/promos",
        data={
            "csrf_token": csrf,
            "code": "SAVE10",
            "discount_kind": "percent",
            "discount_value": "10",
            "max_uses": "5",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    actions = [c.kwargs["action"] for c in db.record_admin_audit.await_args_list]
    assert "promo_create" in actions


# =========================================================================
# Stage-9-Step-8: per-user AI usage log browser
# =========================================================================


def _usage_row(**overrides) -> dict:
    base = {
        "id": 1,
        "model": "openrouter/auto",
        "prompt_tokens": 100,
        "completion_tokens": 50,
        "total_tokens": 150,
        "cost_usd": 0.0042,
        "created_at": "2026-04-28T12:00:00+00:00",
    }
    base.update(overrides)
    return base


async def test_user_usage_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app(password="pw"))
    resp = await client.get(
        "/admin/users/100/usage", allow_redirects=False
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")


async def test_user_usage_invalid_id_redirects_to_users(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="pw"))
    await _login(client, "pw")
    resp = await client.get(
        "/admin/users/not-an-int/usage", allow_redirects=False
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/users"


async def test_user_usage_renders_empty_state(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users/777/usage")
    assert resp.status == 200
    body = await resp.text()
    assert "No AI calls recorded" in body
    # Breadcrumb back to user detail.
    assert 'href="/admin/users/777"' in body
    db.list_user_usage_logs.assert_awaited_once_with(
        telegram_id=777, page=1, per_page=50,
    )
    db.get_user_usage_aggregates.assert_awaited_once_with(777)


async def test_user_usage_renders_rows_and_aggregates(
    aiohttp_client, make_admin_app
):
    rows = [
        _usage_row(id=10, model="openai/gpt-4o", prompt_tokens=1234,
                   completion_tokens=567, total_tokens=1801,
                   cost_usd=0.0234),
        _usage_row(id=11, model="anthropic/claude-3-opus",
                   prompt_tokens=42, completion_tokens=1024,
                   total_tokens=1066, cost_usd=0.1500),
    ]
    db = _stub_db(
        user_usage_result={
            "rows": rows, "total": 2, "page": 1,
            "per_page": 50, "total_pages": 1,
        },
        user_usage_aggregates_result={
            "total_calls": 2, "total_tokens": 2867, "total_cost_usd": 0.1734,
        },
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users/100/usage")
    body = await resp.text()
    assert "openai/gpt-4o" in body
    assert "anthropic/claude-3-opus" in body
    # Comma-grouped tokens for legibility.
    assert "1,234" in body
    # Aggregates rendered.
    assert "2,867" in body  # lifetime tokens
    assert "$0.1734" in body  # lifetime cost (4dp)
    # Per-row cost rendering.
    assert "$0.1500" in body
    assert "Page 1 of 1" in body
    assert "2 call(s)" in body


async def test_user_usage_pagination_forwards_params(
    aiohttp_client, make_admin_app
):
    """``page`` and ``per_page`` query params must be forwarded as
    kwargs to ``list_user_usage_logs``."""
    db = _stub_db(
        user_usage_result={
            "rows": [], "total": 0, "page": 2,
            "per_page": 25, "total_pages": 0,
        }
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users/100/usage?page=2&per_page=25")
    assert resp.status == 200
    db.list_user_usage_logs.assert_awaited_once_with(
        telegram_id=100, page=2, per_page=25,
    )


async def test_user_usage_per_page_clamped(
    aiohttp_client, make_admin_app
):
    """Stray ``per_page`` values are clamped to the documented max."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users/100/usage?per_page=99999")
    assert resp.status == 200
    db.list_user_usage_logs.assert_awaited_once_with(
        telegram_id=100, page=1, per_page=200,  # USAGE_LOGS_PER_PAGE_MAX
    )


async def test_user_usage_db_error_renders_friendly_banner(
    aiohttp_client, make_admin_app
):
    db = _stub_db(user_usage_result=Exception("boom"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users/100/usage")
    assert resp.status == 200
    body = await resp.text()
    assert "Database query failed" in body


async def test_user_detail_links_to_usage_page(
    aiohttp_client, make_admin_app
):
    """The user detail page must include a link to the usage log."""
    summary = {
        "telegram_id": 100,
        "username": "alice",
        "language_code": "en",
        "active_model": "openrouter/auto",
        "balance_usd": 5.0,
        "free_messages_left": 0,
        "total_credited_usd": 10.0,
        "total_spent_usd": 5.0,
        "is_admin": False,
        "is_banned": False,
        "ban_reason": None,
        "recent_transactions": [],
    }
    db = _stub_db(user_summary_result=summary)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/users/100")
    body = await resp.text()
    assert 'href="/admin/users/100/usage"' in body
    assert "View AI usage log" in body
# Stage-9-Step-6: soft-cancel running broadcasts + retry_after cap
# =========================================================================


async def test_do_broadcast_returns_cancelled_false_by_default():
    """Backwards compat — pre-Step-6 callers without ``should_cancel``
    must still see a stats dict; the new ``cancelled`` key defaults to
    False."""
    import admin
    bot = AsyncMock()
    bot.send_message = AsyncMock(return_value=None)
    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0
    try:
        stats = await admin._do_broadcast(
            bot, recipients=[1, 2, 3], text="hi", admin_id=0
        )
    finally:
        admin._BROADCAST_DELAY_S = orig_delay
    assert stats == {
        "sent": 3, "blocked": 0, "failed": 0,
        "total": 3, "cancelled": False,
    }


async def test_do_broadcast_honours_should_cancel_after_first_send():
    """A flag flip after the first successful send must short-circuit
    the loop; remaining recipients must NOT receive the message."""
    import admin
    bot = AsyncMock()
    bot.send_message = AsyncMock(return_value=None)
    flag = {"cancel": False}

    def cancel_after_first():
        # The cancel-check fires at the *top* of every iteration; flip
        # the flag after the first send so iteration 2 sees it.
        if bot.send_message.await_count >= 1:
            flag["cancel"] = True
        return flag["cancel"]

    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0
    try:
        stats = await admin._do_broadcast(
            bot,
            recipients=[1, 2, 3, 4, 5],
            text="hi",
            admin_id=0,
            should_cancel=cancel_after_first,
        )
    finally:
        admin._BROADCAST_DELAY_S = orig_delay
    assert stats["cancelled"] is True
    assert stats["sent"] == 1
    assert bot.send_message.await_count == 1


async def test_do_broadcast_cancel_predicate_exception_is_swallowed():
    """A buggy ``should_cancel`` predicate must not abort the
    broadcast — a raised exception is treated as 'not cancelled'."""
    import admin
    bot = AsyncMock()
    bot.send_message = AsyncMock(return_value=None)

    def explode():
        raise RuntimeError("predicate broken")

    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0
    try:
        stats = await admin._do_broadcast(
            bot, recipients=[1, 2, 3], text="hi", admin_id=0,
            should_cancel=explode,
        )
    finally:
        admin._BROADCAST_DELAY_S = orig_delay
    assert stats["cancelled"] is False
    assert stats["sent"] == 3


async def test_do_broadcast_cancel_check_at_top_of_loop():
    """Cancel set BEFORE the first send must result in zero sends."""
    import admin
    bot = AsyncMock()
    bot.send_message = AsyncMock(return_value=None)

    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0
    try:
        stats = await admin._do_broadcast(
            bot, recipients=[1, 2, 3], text="hi", admin_id=0,
            should_cancel=lambda: True,
        )
    finally:
        admin._BROADCAST_DELAY_S = orig_delay
    assert stats["cancelled"] is True
    assert stats["sent"] == 0
    bot.send_message.assert_not_awaited()


async def test_do_broadcast_caps_retry_after():
    """Pre-fix, ``await asyncio.sleep(exc.retry_after)`` was uncapped.
    A misbehaving Telegram returning ``retry_after=3600`` would pin
    the worker for an hour. Now capped at
    ``_BROADCAST_RETRY_AFTER_MAX_S``."""
    import admin
    from aiogram.exceptions import TelegramRetryAfter
    bot = AsyncMock()
    # First call raises 429 with retry_after=600 (10 min); retry succeeds.
    bot.send_message = AsyncMock(
        side_effect=[
            TelegramRetryAfter(method=MagicMock(), message="flood", retry_after=600),
            None,
        ]
    )
    sleeps: list[float] = []
    orig_sleep = asyncio.sleep
    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        # Keep coroutine semantics but don't actually wait.
        await orig_sleep(0)

    try:
        with patch.object(admin.asyncio, "sleep", fake_sleep):
            stats = await admin._do_broadcast(
                bot, recipients=[1], text="hi", admin_id=0
            )
    finally:
        admin._BROADCAST_DELAY_S = orig_delay
    # The retry sleep must have been capped at the configured maximum.
    assert any(
        s == admin._BROADCAST_RETRY_AFTER_MAX_S for s in sleeps
    ), f"expected one sleep at the cap, got {sleeps}"
    # Ensure no sleep call ever exceeded the cap.
    assert all(
        s <= admin._BROADCAST_RETRY_AFTER_MAX_S for s in sleeps
    ), f"unexpected uncapped sleep in {sleeps}"
    assert stats["sent"] == 1


async def test_do_broadcast_retry_after_within_cap_unchanged():
    """A normal retry_after (well under the cap) must be honoured
    verbatim — the cap only kicks in for outliers."""
    import admin
    from aiogram.exceptions import TelegramRetryAfter
    bot = AsyncMock()
    bot.send_message = AsyncMock(
        side_effect=[
            TelegramRetryAfter(method=MagicMock(), message="flood", retry_after=5),
            None,
        ]
    )
    sleeps: list[float] = []
    orig_sleep = asyncio.sleep
    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        await orig_sleep(0)

    try:
        with patch.object(admin.asyncio, "sleep", fake_sleep):
            stats = await admin._do_broadcast(
                bot, recipients=[1], text="hi", admin_id=0
            )
    finally:
        admin._BROADCAST_DELAY_S = orig_delay
    assert 5 in sleeps or 5.0 in sleeps
    assert stats["sent"] == 1


async def test_run_broadcast_job_marks_cancelled_state(make_admin_app):
    """``_run_broadcast_job`` must read ``job['cancel_requested']``
    and surface the resulting state as ``cancelled`` (not
    ``completed``)."""
    import admin
    recipients = [10, 20, 30, 40, 50]
    db = _stub_db(broadcast_recipients=recipients)
    bot = AsyncMock()
    bot.send_message = AsyncMock(return_value=None)
    app = make_admin_app(db=db, bot=bot)
    job = _new_broadcast_job(text="hi", only_active_days=None)
    _store_broadcast_job(app, job)

    # Flip the cancel flag after the first send.
    async def cancel_after_first(chat_id, text):
        if bot.send_message.await_count == 1:
            job["cancel_requested"] = True

    bot.send_message.side_effect = cancel_after_first

    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0
    try:
        await _run_broadcast_job(app=app, job=job, text="hi")
    finally:
        admin._BROADCAST_DELAY_S = orig_delay

    assert job["state"] == "cancelled"
    assert job["sent"] == 1
    # ``i`` reflects only the recipients we actually attempted, NOT
    # the full recipient list size.
    assert job["i"] == 1
    # ``total`` still records the recipient list length (so the UI
    # can render "stopped at 1/5").
    assert job["total"] == 5


def test_store_broadcast_job_evicts_cancelled_entries():
    """Cancelled jobs must be eligible for eviction once the
    registry hits its cap, not pile up forever."""
    app = web.Application()
    app[APP_KEY_BROADCAST_JOBS] = {}
    ordered_ids: list[str] = []
    for _ in range(BROADCAST_MAX_HISTORY):
        j = _new_broadcast_job(text="cancelled", only_active_days=None)
        j["state"] = "cancelled"
        _store_broadcast_job(app, j)
        ordered_ids.append(j["id"])
    newest = _new_broadcast_job(text="next", only_active_days=None)
    newest["state"] = "completed"
    _store_broadcast_job(app, newest)
    assert ordered_ids[0] not in app[APP_KEY_BROADCAST_JOBS]
    assert newest["id"] in app[APP_KEY_BROADCAST_JOBS]
    assert len(app[APP_KEY_BROADCAST_JOBS]) == BROADCAST_MAX_HISTORY


# ---- /admin/broadcast/{id}/cancel endpoint integration ------------------


async def test_broadcast_cancel_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app())
    resp = await client.post(
        "/admin/broadcast/abc/cancel", allow_redirects=False
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/login"


async def test_broadcast_cancel_csrf_required(aiohttp_client, make_admin_app):
    db = _stub_db()
    app = make_admin_app(password="pw", db=db, bot=AsyncMock())
    job = _new_broadcast_job(text="hi", only_active_days=None)
    job["state"] = "running"
    _store_broadcast_job(app, job)
    client = await aiohttp_client(app)
    await _login(client, "pw")
    resp = await client.post(
        f"/admin/broadcast/{job['id']}/cancel",
        data={},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert job.get("cancel_requested", False) is False


async def test_broadcast_cancel_unknown_job_redirects_to_index(
    aiohttp_client, make_admin_app
):
    app = make_admin_app(password="pw", bot=AsyncMock())
    client = await aiohttp_client(app)
    await _login(client, "pw")
    resp = await client.get("/admin/broadcast")
    body = await resp.text()
    import re
    csrf = re.search(r'name="csrf_token" value="([^"]+)"', body).group(1)
    resp = await client.post(
        "/admin/broadcast/does-not-exist/cancel",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/broadcast"


async def test_broadcast_cancel_on_running_job_sets_flag_and_audits(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    app = make_admin_app(password="pw", db=db, bot=AsyncMock())
    job = _new_broadcast_job(text="hi", only_active_days=None)
    job["state"] = "running"
    _store_broadcast_job(app, job)
    client = await aiohttp_client(app)
    csrf = await _login_and_get_broadcast_csrf(client, "pw")
    resp = await client.post(
        f"/admin/broadcast/{job['id']}/cancel",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == f"/admin/broadcast/{job['id']}"
    assert job["cancel_requested"] is True
    actions = [c.kwargs["action"] for c in db.record_admin_audit.await_args_list]
    assert "broadcast_cancel" in actions


async def test_broadcast_cancel_on_terminal_job_rejected(
    aiohttp_client, make_admin_app
):
    """Cancelling a job that already completed is a flash-error,
    NOT a state mutation."""
    db = _stub_db()
    app = make_admin_app(password="pw", db=db, bot=AsyncMock())
    job = _new_broadcast_job(text="hi", only_active_days=None)
    job["state"] = "completed"
    _store_broadcast_job(app, job)
    client = await aiohttp_client(app)
    csrf = await _login_and_get_broadcast_csrf(client, "pw")
    resp = await client.post(
        f"/admin/broadcast/{job['id']}/cancel",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert job.get("cancel_requested", False) is False
    # No audit row written for a rejected cancel.
    audit_actions = [
        c.kwargs.get("action") for c in db.record_admin_audit.await_args_list
    ]
    assert "broadcast_cancel" not in audit_actions


async def test_broadcast_cancel_idempotent(aiohttp_client, make_admin_app):
    """A second cancel on an already-cancelling job must NOT write a
    second audit row."""
    db = _stub_db()
    app = make_admin_app(password="pw", db=db, bot=AsyncMock())
    job = _new_broadcast_job(text="hi", only_active_days=None)
    job["state"] = "running"
    _store_broadcast_job(app, job)
    client = await aiohttp_client(app)
    csrf = await _login_and_get_broadcast_csrf(client, "pw")
    for _ in range(2):
        resp = await client.post(
            f"/admin/broadcast/{job['id']}/cancel",
            data={"csrf_token": csrf},
            allow_redirects=False,
        )
        assert resp.status == 302
    cancel_audits = [
        c for c in db.record_admin_audit.await_args_list
        if c.kwargs.get("action") == "broadcast_cancel"
    ]
    assert len(cancel_audits) == 1


async def test_broadcast_detail_renders_cancel_button_for_running(
    aiohttp_client, make_admin_app
):
    """Verify the live-progress page renders the cancel form when the
    job is ``running`` and hides it when terminal."""
    db = _stub_db()
    app = make_admin_app(password="pw", db=db, bot=AsyncMock())
    job_running = _new_broadcast_job(text="hi", only_active_days=None)
    job_running["state"] = "running"
    _store_broadcast_job(app, job_running)
    job_done = _new_broadcast_job(text="hi", only_active_days=None)
    job_done["state"] = "completed"
    _store_broadcast_job(app, job_done)
    client = await aiohttp_client(app)
    await _login(client, "pw")

    resp = await client.get(f"/admin/broadcast/{job_running['id']}")
    body = await resp.text()
    assert f"/admin/broadcast/{job_running['id']}/cancel" in body
    assert "Cancel" in body

    resp = await client.get(f"/admin/broadcast/{job_done['id']}")
    body = await resp.text()
    assert f"/admin/broadcast/{job_done['id']}/cancel" not in body
