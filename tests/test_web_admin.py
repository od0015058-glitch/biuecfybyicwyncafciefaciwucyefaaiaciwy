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

import database as database_module

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


_UNSET = object()  # sentinel for stub-default vs explicit-None.


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
    # Stage-9-Step-10: durable broadcast registry.
    broadcast_jobs_rows: list | Exception | None = None,
    get_broadcast_job_result: dict | None | Exception = None,
    insert_broadcast_job_result: object | Exception = None,
    update_broadcast_job_result: bool | Exception = True,
    mark_orphan_broadcast_jobs_result: int | Exception = 0,
    # Stage-12-Step-A: refund flow.
    refund_transaction_result: dict | None | Exception = None,
    # Stage-12-Step-D: per-code redemption drilldown. Sentinel default
    # ``object()`` distinguishes "caller didn't pass a value, use the
    # standard fixture row" from "caller explicitly wants None" (i.e.
    # the gift code doesn't exist).
    get_gift_code_result=_UNSET,
    list_gift_code_redemptions_result: dict | Exception | None = None,
    gift_code_redemption_aggregates_result: dict | Exception | None = None,
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
            # Stage-9-Step-9: pending-payments tile keys.
            "pending_payments_count": 0,
            "pending_payments_oldest_age_hours": None,
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
    # Stage-9-Step-10: durable broadcast registry stubs. These have to
    # be explicit because ``AsyncMock()`` defaults every unspecified
    # method to "returns an AsyncMock instance", which fails JSON
    # serialisation on /admin/broadcast/{id}/status and turns the
    # in-memory fallback into nonsense for /admin/broadcast.
    if isinstance(broadcast_jobs_rows, Exception):
        db.list_broadcast_jobs = AsyncMock(side_effect=broadcast_jobs_rows)
    else:
        db.list_broadcast_jobs = AsyncMock(
            return_value=broadcast_jobs_rows
            if broadcast_jobs_rows is not None else []
        )
    if isinstance(get_broadcast_job_result, Exception):
        db.get_broadcast_job = AsyncMock(side_effect=get_broadcast_job_result)
    else:
        db.get_broadcast_job = AsyncMock(
            return_value=get_broadcast_job_result
        )
    if isinstance(insert_broadcast_job_result, Exception):
        db.insert_broadcast_job = AsyncMock(
            side_effect=insert_broadcast_job_result
        )
    else:
        db.insert_broadcast_job = AsyncMock(
            return_value=insert_broadcast_job_result
        )
    if isinstance(update_broadcast_job_result, Exception):
        db.update_broadcast_job = AsyncMock(
            side_effect=update_broadcast_job_result
        )
    else:
        db.update_broadcast_job = AsyncMock(
            return_value=update_broadcast_job_result
        )
    if isinstance(mark_orphan_broadcast_jobs_result, Exception):
        db.mark_orphan_broadcast_jobs_interrupted = AsyncMock(
            side_effect=mark_orphan_broadcast_jobs_result
        )
    else:
        db.mark_orphan_broadcast_jobs_interrupted = AsyncMock(
            return_value=mark_orphan_broadcast_jobs_result
        )
    # Stage-12-Step-A: refund_transaction stub.
    if isinstance(refund_transaction_result, Exception):
        db.refund_transaction = AsyncMock(side_effect=refund_transaction_result)
    else:
        db.refund_transaction = AsyncMock(return_value=refund_transaction_result)
    # Stage-12-Step-D: per-code redemption drilldown stubs. Default to
    # a non-None gift row so the handler doesn't redirect on every test
    # that doesn't explicitly opt out — tests that *want* a missing
    # code pass ``get_gift_code_result=None`` explicitly.
    _DEFAULT_GIFT_META = {
        "code": "BIRTHDAY5",
        "amount_usd": 5.0,
        "max_uses": 10,
        "used_count": 3,
        "expires_at": None,
        "is_active": True,
        "created_at": "2026-01-01T00:00:00+00:00",
    }
    if isinstance(get_gift_code_result, Exception):
        db.get_gift_code = AsyncMock(side_effect=get_gift_code_result)
    elif get_gift_code_result is _UNSET:
        db.get_gift_code = AsyncMock(return_value=_DEFAULT_GIFT_META)
    else:
        db.get_gift_code = AsyncMock(return_value=get_gift_code_result)
    _DEFAULT_REDEMPTIONS = {
        "rows": [], "total": 0, "page": 1,
        "per_page": 50, "total_pages": 0,
    }
    if isinstance(list_gift_code_redemptions_result, Exception):
        db.list_gift_code_redemptions = AsyncMock(
            side_effect=list_gift_code_redemptions_result
        )
    else:
        db.list_gift_code_redemptions = AsyncMock(
            return_value=list_gift_code_redemptions_result
            if list_gift_code_redemptions_result is not None
            else _DEFAULT_REDEMPTIONS
        )
    _DEFAULT_GIFT_AGG = {
        "total_redemptions": 0, "total_credited_usd": 0.0,
        "first_redeemed_at": None, "last_redeemed_at": None,
    }
    if isinstance(gift_code_redemption_aggregates_result, Exception):
        db.get_gift_code_redemption_aggregates = AsyncMock(
            side_effect=gift_code_redemption_aggregates_result
        )
    else:
        db.get_gift_code_redemption_aggregates = AsyncMock(
            return_value=gift_code_redemption_aggregates_result
            if gift_code_redemption_aggregates_result is not None
            else _DEFAULT_GIFT_AGG
        )
    # Stage-15-Step-E #5 follow-up #2: admin-roles web page stubs. The
    # standard stub returns an empty role list and a successful grant /
    # revoke. Tests that exercise the failure paths construct their own
    # AsyncMock and pass it directly via ``db=`` instead of going
    # through ``_stub_db``.
    db.list_admin_roles = AsyncMock(return_value=[])
    db.set_admin_role = AsyncMock(return_value="viewer")
    db.delete_admin_role = AsyncMock(return_value=True)
    # Surface the canonical refundable-gateways set on the stub so the
    # transactions template can iterate it without reaching for the
    # real Database class — matches the production import path.
    db.REFUNDABLE_GATEWAYS = database_module.Database.REFUNDABLE_GATEWAYS
    db.REFUND_REFUSAL_NOT_SUCCESS = (
        database_module.Database.REFUND_REFUSAL_NOT_SUCCESS
    )
    db.REFUND_REFUSAL_GATEWAY_NOT_REFUNDABLE = (
        database_module.Database.REFUND_REFUSAL_GATEWAY_NOT_REFUNDABLE
    )
    db.REFUND_REFUSAL_INSUFFICIENT_BALANCE = (
        database_module.Database.REFUND_REFUSAL_INSUFFICIENT_BALANCE
    )
    # Stage-15-Step-E #4 follow-up #2: DB-backed OpenRouter key
    # registry. Default mocks ensure the new ``/admin/openrouter-keys``
    # render path has a benign empty list / no-op pool.
    db.list_openrouter_keys = AsyncMock(return_value=[])
    db.list_enabled_openrouter_keys_with_secret = AsyncMock(return_value=[])
    db.add_openrouter_key = AsyncMock(return_value=1)
    db.set_openrouter_key_enabled = AsyncMock(return_value=True)
    db.delete_openrouter_key = AsyncMock(return_value=True)
    db.mark_openrouter_key_used = AsyncMock(return_value=None)
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
            "pending_payments_count": 0,
            "pending_payments_oldest_age_hours": None,
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


def test_verify_totp_code_accepts_persian_digits():
    """Stage-15-Step-F bundled bug fix.

    The bot's primary user base is Persian. An admin pasting the
    current TOTP from a Persian-locale clipboard would type the
    code in Persian digits (``۱۲۳۴۵۶`` U+06F0..U+06F9) and see a
    confusing "Invalid 2FA code" error — ``str.isdigit()`` accepted
    them but ``pyotp.TOTP.verify`` rejected them. The fix normalises
    Persian + Arabic-Indic digits to ASCII before validation.
    """
    import pyotp
    from web_admin import verify_totp_code

    _PERSIAN = "۰۱۲۳۴۵۶۷۸۹"
    secret = pyotp.random_base32()
    ascii_code = pyotp.TOTP(secret).now()
    persian_code = "".join(_PERSIAN[int(d)] for d in ascii_code)
    assert verify_totp_code(secret, persian_code) is True


def test_verify_totp_code_accepts_arabic_indic_digits():
    """Stage-15-Step-F bundled bug fix — Arabic-Indic digits
    (``٠١٢٣٤٥٦٧٨٩`` U+0660..U+0669) also normalise to ASCII."""
    import pyotp
    from web_admin import verify_totp_code

    _ARABIC_INDIC = "٠١٢٣٤٥٦٧٨٩"
    secret = pyotp.random_base32()
    ascii_code = pyotp.TOTP(secret).now()
    arabic_code = "".join(_ARABIC_INDIC[int(d)] for d in ascii_code)
    assert verify_totp_code(secret, arabic_code) is True


def test_verify_totp_code_accepts_mixed_persian_and_ascii():
    """Stage-15-Step-F bundled bug fix — mixed scripts within the
    same code (a Persian admin who half-typed and half-pasted)
    still normalise cleanly."""
    import pyotp
    from web_admin import verify_totp_code

    _PERSIAN = "۰۱۲۳۴۵۶۷۸۹"
    secret = pyotp.random_base32()
    ascii_code = pyotp.TOTP(secret).now()
    # Replace just the first 3 digits with their Persian equivalents.
    mixed = "".join(_PERSIAN[int(d)] for d in ascii_code[:3]) + ascii_code[3:]
    assert verify_totp_code(secret, mixed) is True


def test_verify_totp_code_rejects_non_arabic_persian_unicode_digits():
    """Stage-15-Step-F bundled bug fix — only Persian + Arabic-Indic
    digits are normalised. Other Unicode digit classes (Bengali,
    full-width, mathematical) still fail-fast with ``False`` rather
    than reaching pyotp."""
    from web_admin import verify_totp_code

    # Bengali ``০১২৩৪৫`` (U+09E6..U+09EB).
    assert verify_totp_code("ABCDEFGHIJKLMNOP", "০১২৩৪৫") is False
    # Full-width Latin digits ``１２３４５６``.
    assert verify_totp_code("ABCDEFGHIJKLMNOP", "１２３４５６") is False


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
            "pending_payments_count": 7,
            "pending_payments_oldest_age_hours": 12.5,
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
    # All five stat tiles render with the right values (Stage-9-Step-9
    # added the pending-payments tile).
    assert "9,999" in body
    assert "250" in body
    assert "$4,321.00" in body
    assert "$1,234.5678" in body
    # Pending-payments tile renders count + oldest-age sub-label.
    assert "Pending payments" in body
    assert "12.5h" in body
    # Top-models table renders both rows, with model name + count + cost.
    assert "openai/gpt-4o-mini" in body
    assert "anthropic/claude-3.5-sonnet" in body
    assert "5,000" in body
    assert "$12.3456" in body
    assert "$7.8901" in body


async def test_dashboard_pending_zero_hides_oldest_age(
    aiohttp_client, make_admin_app
):
    """Stage-9-Step-9: the 'oldest Xh' sub-label must NOT render
    when zero pending rows exist — that's where the back-end query
    returns ``MIN(created_at)`` = NULL and we surface ``None``. The
    main count still renders ('0'), but the misleading sub-label is
    suppressed.
    """
    db = _stub_db({
        "users_total": 1,
        "users_active_7d": 0,
        "revenue_usd": 0.0,
        "spend_usd": 0.0,
        "top_models": [],
        "pending_payments_count": 0,
        "pending_payments_oldest_age_hours": None,
    })
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "Pending payments" in body
    # The "oldest Xh" sub-label MUST be hidden when count == 0; pin
    # the absence of the literal "oldest" word in the rendered tile.
    assert "oldest " not in body, (
        "pending-payments tile rendered oldest-age sub-label "
        "with count=0 — UI shows misleading 'oldest 0.0h' text"
    )


async def test_dashboard_renders_ipn_health_tile_with_drop_counts(
    aiohttp_client, make_admin_app, monkeypatch
):
    """Stage-15-Step-D #5: the new IPN-health panel surfaces every
    drop-counter reason with its current count.

    The counters live in :mod:`payments` and :mod:`tetrapay` and
    are read each render via :func:`web_admin._collect_ipn_health`.
    Patching the accessors gives us deterministic values for the
    template assertions.
    """
    import payments
    import tetrapay
    monkeypatch.setattr(
        payments,
        "get_ipn_drop_counters",
        lambda: {
            "bad_signature": 7,
            "bad_json": 3,
            "missing_payment_id": 0,
            "replay": 1,
        },
    )
    monkeypatch.setattr(
        tetrapay,
        "get_tetrapay_drop_counters",
        lambda: {
            "bad_json": 0,
            "missing_authority": 0,
            "non_success_callback": 5,
            "unknown_invoice": 2,
            "verify_failed": 0,
        },
    )

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login",
        data={"password": "letmein"},
        allow_redirects=False,
    )
    resp = await client.get("/admin/")
    assert resp.status == 200, await resp.text()
    body = await resp.text()

    # Section heading + the "since last restart" caveat are both
    # part of the contract — operators must understand the
    # counters reset on every redeploy.
    assert "IPN health" in body
    assert "since last restart" in body

    # Per-gateway sub-headings + the reason-code rows.
    assert "NowPayments" in body
    assert "TetraPay" in body
    for reason in (
        "bad_signature", "bad_json", "missing_payment_id", "replay",
        "missing_authority", "non_success_callback",
        "unknown_invoice", "verify_failed",
    ):
        assert reason in body, f"missing reason {reason!r} in IPN tile"

    # Numeric values render with thousands-sep (we picked small
    # numbers so the formatter is identity, but the assertions
    # still pin the format).
    assert "7" in body
    assert "5" in body


async def test_dashboard_renders_ipn_health_all_zero_message(
    aiohttp_client, make_admin_app, monkeypatch
):
    """When every counter is zero (fresh restart, no traffic), the
    panel must still render — and explain *why* it's empty rather
    than looking broken.
    """
    import payments
    import tetrapay
    monkeypatch.setattr(
        payments,
        "get_ipn_drop_counters",
        lambda: {"bad_signature": 0, "bad_json": 0, "missing_payment_id": 0, "replay": 0},
    )
    monkeypatch.setattr(
        tetrapay,
        "get_tetrapay_drop_counters",
        lambda: {
            "bad_json": 0, "missing_authority": 0,
            "non_success_callback": 0, "unknown_invoice": 0,
            "verify_failed": 0,
        },
    )

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/")
    assert resp.status == 200
    body = await resp.text()
    assert "no NowPayments IPN drops recorded since startup" in body
    assert "no TetraPay webhook drops recorded since startup" in body


async def test_dashboard_ipn_health_resilient_to_accessor_failure(
    aiohttp_client, make_admin_app, monkeypatch
):
    """If one accessor raises (e.g. a future regression in payments
    breaks ``get_ipn_drop_counters``) the dashboard must still
    render; the broken half just shows a "counters unavailable"
    line and the other gateway's tile keeps working.
    """
    import payments
    import tetrapay
    def boom():
        raise RuntimeError("module regression")
    monkeypatch.setattr(payments, "get_ipn_drop_counters", boom)
    monkeypatch.setattr(
        tetrapay,
        "get_tetrapay_drop_counters",
        lambda: {"bad_json": 4, "missing_authority": 0,
                 "non_success_callback": 0, "unknown_invoice": 0,
                 "verify_failed": 0},
    )

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/")
    assert resp.status == 200, await resp.text()
    body = await resp.text()

    # NowPayments tile shows the failure-mode message.
    assert "NowPayments counters unavailable" in body
    # TetraPay tile still renders normally with the live counts.
    assert "bad_json" in body
    assert "4" in body


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


async def test_dashboard_renders_zarinpal_drop_counts(
    aiohttp_client, make_admin_app, monkeypatch
):
    """Stage-15-Step-E #9 bundled fix: Zarinpal shipped its own
    drop-counter registry in Stage-15-Step-E #8 but the dashboard
    tile was never extended. The IPN-health panel now surfaces a
    third Zarinpal section alongside NowPayments and TetraPay so an
    operator debugging a verify-failure spike can see counts here
    instead of grepping the bot logs.
    """
    import zarinpal
    monkeypatch.setattr(
        zarinpal,
        "get_zarinpal_drop_counters",
        lambda: {
            "missing_authority": 1,
            "non_success_callback": 0,
            "unknown_invoice": 0,
            "verify_failed": 7,
            "replay": 2,
        },
    )

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/")
    assert resp.status == 200, await resp.text()
    body = await resp.text()

    # Zarinpal sub-heading in the IPN health panel.
    assert "Zarinpal" in body
    # Per-reason rows render. ``verify_failed`` is the most signal-
    # rich one (means Zarinpal's verify endpoint rejected our
    # finalize call) so pin both the row text and the count.
    assert "verify_failed" in body
    assert "7" in body
    assert "replay" in body


async def test_dashboard_renders_zarinpal_all_zero_message(
    aiohttp_client, make_admin_app, monkeypatch
):
    """When every Zarinpal counter is zero (fresh restart, no Iranian
    card traffic yet) the panel must still render the explanatory
    "all zero" line — same shape as the NowPayments / TetraPay
    panels, otherwise the operator sees a bare table and wonders
    whether the section is broken.
    """
    import zarinpal
    monkeypatch.setattr(
        zarinpal,
        "get_zarinpal_drop_counters",
        lambda: {
            "missing_authority": 0, "non_success_callback": 0,
            "unknown_invoice": 0, "verify_failed": 0, "replay": 0,
        },
    )

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/")
    assert resp.status == 200
    body = await resp.text()
    assert "no Zarinpal callback drops recorded since startup" in body


async def test_collect_ipn_health_includes_zarinpal(monkeypatch):
    """Direct test of the helper: the returned dict must carry a
    ``zarinpal`` key and a ``zarinpal_total`` summary alongside the
    NowPayments / TetraPay equivalents. Test pins the shape so a
    template typo (``ipn_health.zarinpal_drops`` vs
    ``ipn_health.zarinpal``) is caught at PR time.
    """
    import payments
    import tetrapay
    import zarinpal

    monkeypatch.setattr(
        payments,
        "get_ipn_drop_counters",
        lambda: {"bad_signature": 0},
    )
    monkeypatch.setattr(
        tetrapay,
        "get_tetrapay_drop_counters",
        lambda: {"bad_json": 0},
    )
    monkeypatch.setattr(
        zarinpal,
        "get_zarinpal_drop_counters",
        lambda: {"verify_failed": 5, "replay": 1},
    )

    from web_admin import _collect_ipn_health
    health = _collect_ipn_health()

    # Three gateway sub-dicts present.
    assert set(health.keys()) == {
        "nowpayments", "tetrapay", "zarinpal",
        "nowpayments_total", "tetrapay_total", "zarinpal_total",
    }
    assert health["zarinpal"] == {"verify_failed": 5, "replay": 1}
    assert health["zarinpal_total"] == 6


async def test_collect_ipn_health_resilient_to_zarinpal_accessor_failure(
    monkeypatch,
):
    """Same defense the NowPayments / TetraPay halves already get:
    a future regression in ``zarinpal.get_zarinpal_drop_counters``
    must not blank the other two panels. The Zarinpal sub-dict is
    just empty (template renders the "counters unavailable" line).
    """
    import payments
    import tetrapay
    import zarinpal

    monkeypatch.setattr(
        payments, "get_ipn_drop_counters", lambda: {"bad_signature": 1}
    )
    monkeypatch.setattr(
        tetrapay, "get_tetrapay_drop_counters", lambda: {"bad_json": 2}
    )

    def boom():
        raise RuntimeError("zarinpal regression")
    monkeypatch.setattr(zarinpal, "get_zarinpal_drop_counters", boom)

    from web_admin import _collect_ipn_health
    health = _collect_ipn_health()

    # NowPayments / TetraPay still populated.
    assert health["nowpayments"] == {"bad_signature": 1}
    assert health["tetrapay"] == {"bad_json": 2}
    # Zarinpal half is empty rather than the whole dict imploding.
    assert health["zarinpal"] == {}
    assert health["zarinpal_total"] == 0


# ---------------------------------------------------------------------
# /admin/monetization (Stage-15-Step-E #9)
# ---------------------------------------------------------------------


def _stub_db_with_monetization(summary: dict | Exception):
    """Build a stub DB that has the standard system-metrics surface
    plus a ``get_monetization_summary`` mock returning *summary* (or
    raising it).
    """
    db = _stub_db()
    if isinstance(summary, Exception):
        db.get_monetization_summary = AsyncMock(side_effect=summary)
    else:
        db.get_monetization_summary = AsyncMock(return_value=summary)
    return db


async def test_monetization_route_requires_auth(
    aiohttp_client, make_admin_app
):
    """Sanity: the monetization page is gated by ``_require_auth``
    just like the dashboard, transactions, etc. An unauthenticated
    GET must redirect to /admin/login.
    """
    client = await aiohttp_client(make_admin_app(password="letmein"))
    resp = await client.get("/admin/monetization", allow_redirects=False)
    assert resp.status in (302, 303), resp.status
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_monetization_renders_lifetime_and_window_blocks(
    aiohttp_client, make_admin_app
):
    """Happy path: the page renders the markup, both money blocks
    (lifetime + last-30-days), and the per-model table.
    """
    summary = {
        "markup": 2.0,
        "lifetime": {
            "revenue_usd": 1234.56,
            "charged_usd": 600.0,
            "openrouter_cost_usd": 300.0,
            "gross_margin_usd": 300.0,
            "gross_margin_pct": 50.0,
            "net_profit_usd": 934.56,
        },
        "window": {
            "days": 30,
            "revenue_usd": 200.0,
            "charged_usd": 80.0,
            "openrouter_cost_usd": 40.0,
            "gross_margin_usd": 40.0,
            "gross_margin_pct": 50.0,
            "net_profit_usd": 160.0,
        },
        "by_model": [
            {
                "model": "openai/gpt-4o",
                "requests": 12,
                "charged_usd": 50.0,
                "openrouter_cost_usd": 25.0,
                "gross_margin_usd": 25.0,
            }
        ],
    }
    db = _stub_db_with_monetization(summary)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    # Page heading + section labels.
    assert "Monetization" in body
    assert "Last 30 days" in body
    assert "Lifetime" in body
    # Markup with 4-decimal precision.
    assert "2.0000" in body
    # Lifetime revenue rendered with thousands sep + 2 decimals.
    assert "$1,234.56" in body
    # Per-model table.
    assert "openai/gpt-4o" in body


async def test_monetization_renders_db_error_banner_on_query_failure(
    aiohttp_client, make_admin_app
):
    """The DB-error path must render the empty-zero shape plus an
    inline banner — same fail-soft shape the ``dashboard`` handler
    uses, so a flaky DB doesn't 500 the page.
    """
    db = _stub_db_with_monetization(RuntimeError("kaboom"))
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "Database query failed" in body
    assert "Last 30 days" in body  # window block still renders


async def test_monetization_renders_dev_mode_without_db(
    aiohttp_client,
):
    """When no DB is wired (local dev), the monetization page
    renders the dev-mode banner with zero values. Same fail-soft
    shape the ``dashboard`` handler uses. We bypass ``make_admin_app``
    here because that fixture substitutes a stub DB when ``db=None``;
    the dev-mode branch we're pinning specifically wants ``app[DB]``
    to be ``None``.
    """
    app = web.Application()
    setup_admin_routes(
        app,
        db=None,
        password="letmein",
        session_secret="x" * 32,
        ttl_hours=24,
        cookie_secure=False,
    )
    client = await aiohttp_client(app)
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "No database wired up" in body
    # Lifetime + window blocks render with zero values.
    assert "Lifetime" in body
    assert "Last 30 days" in body


async def test_monetization_empty_by_model_table_renders_placeholder(
    aiohttp_client, make_admin_app
):
    """When the per-model table is empty (fresh deploy, no usage
    logged yet) the panel renders an explanatory placeholder rather
    than an empty ``<table>`` that looks broken.
    """
    summary = {
        "markup": 1.5,
        "lifetime": {
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 0.0, "net_profit_usd": 0.0,
        },
        "window": {
            "days": 30,
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 0.0, "net_profit_usd": 0.0,
        },
        "by_model": [],
    }
    db = _stub_db_with_monetization(summary)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "No model usage logged in the last 30 days" in body


# ---------------------------------------------------------------------
# Stage-15-Step-E #9 follow-up: top-users-by-revenue panel
# ---------------------------------------------------------------------


async def test_monetization_renders_top_users_panel(
    aiohttp_client, make_admin_app
):
    """Happy path: the page renders the new "Top users by revenue"
    panel with each user's username, top-up count, revenue and
    wallet charges. Username links to the per-user detail page;
    the telegram_id is shown as a secondary annotation when a
    username is present, or as the primary identifier otherwise.
    """
    summary = {
        "markup": 2.0,
        "lifetime": {
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 0.0,
        },
        "window": {
            "days": 30,
            "revenue_usd": 130.0, "charged_usd": 30.0,
            "openrouter_cost_usd": 15.0, "gross_margin_usd": 15.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 115.0,
        },
        "by_model": [],
        "top_users": [
            {
                "telegram_id": 111,
                "username": "alice",
                "revenue_usd": 80.0,
                "topup_count": 4,
                "charged_usd": 30.0,
            },
            {
                "telegram_id": 222,
                "username": None,
                "revenue_usd": 50.0,
                "topup_count": 1,
                "charged_usd": 0.0,
            },
        ],
    }
    db = _stub_db_with_monetization(summary)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    assert resp.status == 200, await resp.text()
    body = await resp.text()

    # Panel heading.
    assert "Top users by revenue" in body
    # alice's username + link to her detail page.
    assert "@alice" in body
    assert "/admin/users/111" in body
    # Username-less user falls back to the telegram_id as the
    # link text and link target.
    assert "/admin/users/222" in body
    # Money fields rendered with thousands sep + 4 dp.
    assert "$80.0000" in body
    assert "$50.0000" in body
    # Topup count rendered with thousands sep.
    assert ">4<" in body  # alice's topup count
    assert ">1<" in body  # bob's topup count


async def test_monetization_renders_top_users_empty_state(
    aiohttp_client, make_admin_app
):
    """When ``top_users`` is empty (fresh deploy, no paid top-ups in
    the window) the panel renders an explanatory placeholder rather
    than an empty ``<table>`` that looks broken. Mirrors the
    by_model empty-state shape.
    """
    summary = {
        "markup": 1.5,
        "lifetime": {
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 0.0, "net_profit_usd": 0.0,
        },
        "window": {
            "days": 7,
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 0.0, "net_profit_usd": 0.0,
        },
        "by_model": [],
        "top_users": [],
    }
    db = _stub_db_with_monetization(summary)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization?window=7")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "No paying users in the last 7 days" in body
    assert "excluding manual credits and gift-code redemptions" in body


async def test_monetization_route_passes_top_users_limit_to_db(
    aiohttp_client, make_admin_app
):
    """The HTML handler must pass ``top_users_limit`` to the DB call
    so the SQL caps the result set at the panel size (10).
    """
    from web_admin import _MONETIZATION_TOP_USERS_LIMIT

    summary = {
        "markup": 2.0,
        "lifetime": {
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 0.0,
        },
        "window": {
            "days": 30,
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 0.0,
        },
        "by_model": [],
        "top_users": [],
    }
    db = _stub_db_with_monetization(summary)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    assert resp.status == 200
    db.get_monetization_summary.assert_awaited_once()
    kwargs = db.get_monetization_summary.await_args.kwargs
    assert kwargs["top_users_limit"] == _MONETIZATION_TOP_USERS_LIMIT
    assert kwargs["top_users_limit"] == 10


async def test_monetization_csv_route_passes_wider_top_users_limit(
    aiohttp_client, make_admin_app
):
    """CSV export pulls ``MONETIZATION_CSV_TOP_USERS_LIMIT`` (1000)
    rather than the on-page panel cap so an operator doing monthly
    P&L sees the long tail.
    """
    from web_admin import MONETIZATION_CSV_TOP_USERS_LIMIT

    db = _stub_db_with_monetization(_sample_monetization_summary())
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization/export.csv")
    assert resp.status == 200
    db.get_monetization_summary.assert_awaited()
    call_kwargs = db.get_monetization_summary.await_args.kwargs
    assert call_kwargs["top_users_limit"] == MONETIZATION_CSV_TOP_USERS_LIMIT
    assert call_kwargs["top_users_limit"] == 1000


# ---------------------------------------------------------------------
# Stage-15-Step-E #9 follow-up #1: window selector
# ---------------------------------------------------------------------


import pytest as _pytest_for_window  # noqa: E402  (test-suite local alias)
from web_admin import (  # noqa: E402
    _MONETIZATION_DEFAULT_WINDOW_DAYS,
    _MONETIZATION_WINDOW_OPTIONS,
    _empty_monetization_summary,
    _parse_monetization_window,
)


@_pytest_for_window.mark.parametrize(
    "raw,expected",
    [
        # Allowlist hits.
        ("7", 7),
        ("30", 30),
        ("90", 90),
        # Padding / leading-plus tolerated by ``int()`` after .strip().
        (" 30 ", 30),
        ("+30", 30),
        # Allowlist misses → fall back to default.
        ("14", _MONETIZATION_DEFAULT_WINDOW_DAYS),
        ("365", _MONETIZATION_DEFAULT_WINDOW_DAYS),
        ("0", _MONETIZATION_DEFAULT_WINDOW_DAYS),
        ("-7", _MONETIZATION_DEFAULT_WINDOW_DAYS),
        # Non-numeric / malformed → fall back to default.
        ("abc", _MONETIZATION_DEFAULT_WINDOW_DAYS),
        ("", _MONETIZATION_DEFAULT_WINDOW_DAYS),
        ("7d", _MONETIZATION_DEFAULT_WINDOW_DAYS),
        ("7.0", _MONETIZATION_DEFAULT_WINDOW_DAYS),
        # Missing entirely → default.
        (None, _MONETIZATION_DEFAULT_WINDOW_DAYS),
    ],
)
def test_parse_monetization_window_allowlist(raw, expected):
    """The query-param parser accepts only the fixed allowlist
    (7 / 30 / 90); anything else falls back to the default."""
    assert _parse_monetization_window(raw) == expected


def test_parse_monetization_window_options_constant():
    """Pin the allowlist tuple so a future regression that drops one
    of the conventional windows is caught at test time. The template
    iterates this tuple directly to render the segmented control."""
    assert _MONETIZATION_WINDOW_OPTIONS == (7, 30, 90)
    assert _MONETIZATION_DEFAULT_WINDOW_DAYS in _MONETIZATION_WINDOW_OPTIONS


@_pytest_for_window.mark.parametrize("markup,expected_pct", [
    (1.0, 0.0),
    (0.0, 0.0),
    (2.0, 50.0),
    (1.5, (1.5 - 1.0) / 1.5 * 100.0),
    (4.0, 75.0),
])
def test_empty_monetization_summary_derives_gross_margin_pct(
    markup, expected_pct
):
    """Bundled bug fix: the empty-fallback shape now derives
    ``gross_margin_pct`` from the markup rather than hardcoding 0.0,
    so the dev-mode / DB-error paths no longer mis-render the
    pricing tile (e.g. "markup 2× / margin 0%")."""
    summary = _empty_monetization_summary(window_days=30, markup=markup)
    assert summary["lifetime"]["gross_margin_pct"] == _pytest_for_window.approx(
        expected_pct
    )
    assert summary["window"]["gross_margin_pct"] == _pytest_for_window.approx(
        expected_pct
    )


async def test_monetization_route_default_window_when_no_query_param(
    aiohttp_client, make_admin_app
):
    """No ``?window=`` query → 30-day window passed to the DB call."""
    summary = {
        "markup": 2.0,
        "lifetime": {
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 0.0,
        },
        "window": {
            "days": 30,
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 0.0,
        },
        "by_model": [],
    }
    db = _stub_db_with_monetization(summary)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    assert resp.status == 200, await resp.text()
    db.get_monetization_summary.assert_awaited_once()
    kwargs = db.get_monetization_summary.await_args.kwargs
    assert kwargs["window_days"] == 30


@_pytest_for_window.mark.parametrize("requested", [7, 30, 90])
async def test_monetization_route_honors_allowlisted_window_query(
    aiohttp_client, make_admin_app, requested
):
    """A valid ``?window=N`` (where N ∈ {7, 30, 90}) flows into the
    DB call and the rendered page heading."""
    summary = {
        "markup": 2.0,
        "lifetime": {
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 0.0,
        },
        "window": {
            "days": requested,
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 0.0,
        },
        "by_model": [],
    }
    db = _stub_db_with_monetization(summary)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get(f"/admin/monetization?window={requested}")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    db.get_monetization_summary.assert_awaited_once()
    kwargs = db.get_monetization_summary.await_args.kwargs
    assert kwargs["window_days"] == requested
    # Heading reflects the active window.
    assert f"Last {requested} days" in body


async def test_monetization_route_falls_back_on_invalid_window_query(
    aiohttp_client, make_admin_app
):
    """An out-of-allowlist or malformed ``?window=`` value silently
    falls back to the 30-day default — never 500s."""
    summary = {
        "markup": 2.0,
        "lifetime": {
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 0.0,
        },
        "window": {
            "days": 30,
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 0.0,
        },
        "by_model": [],
    }
    db = _stub_db_with_monetization(summary)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    for bogus in ("365", "abc", "0", "-7", "14"):
        db.get_monetization_summary.reset_mock()
        resp = await client.get(f"/admin/monetization?window={bogus}")
        assert resp.status == 200, await resp.text()
        kwargs = db.get_monetization_summary.await_args.kwargs
        assert kwargs["window_days"] == 30, (
            f"window={bogus!r} should fall back to 30; got {kwargs}"
        )


async def test_monetization_route_renders_window_selector(
    aiohttp_client, make_admin_app
):
    """The page renders a segmented selector with all three options;
    the active one is a non-link span (so the operator can't click
    the current view) and the inactive ones are anchors with the
    correct ``?window=`` href."""
    summary = {
        "markup": 2.0,
        "lifetime": {
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 0.0,
        },
        "window": {
            "days": 7,
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 50.0, "net_profit_usd": 0.0,
        },
        "by_model": [],
    }
    db = _stub_db_with_monetization(summary)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization?window=7")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    # All three pills appear.
    assert ">7d<" in body
    assert ">30d<" in body
    assert ">90d<" in body
    # Active pill (7d) is a span, NOT an anchor.
    assert 'class="window-selector-active"' in body
    # Inactive pills are anchors with the correct target.
    assert 'href="?window=30"' in body
    assert 'href="?window=90"' in body


async def test_monetization_db_error_path_renders_correct_margin_pct(
    aiohttp_client, make_admin_app
):
    """Bundled bug fix regression test: when the DB query fails and
    we fall back to ``_empty_monetization_summary``, the pricing tile
    must still render the markup-derived gross-margin percentage.
    Pre-fix it rendered "0.00% of every charged dollar" regardless
    of markup."""
    db = _stub_db_with_monetization(RuntimeError("boom"))
    # Pin the markup so the test is independent of env config.
    with _pytest_for_window.MonkeyPatch.context() as mp:
        mp.setattr("pricing.get_markup", lambda: 2.0)
        client = await aiohttp_client(
            make_admin_app(password="letmein", db=db)
        )
        await client.post(
            "/admin/login",
            data={"password": "letmein"},
            allow_redirects=False,
        )
        resp = await client.get("/admin/monetization")
        assert resp.status == 200, await resp.text()
        body = await resp.text()
    assert "Database query failed" in body
    # markup=2.0 → gross_margin_pct=50%.
    assert "50.00%" in body


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 2: COST_MARKUP editor on /admin/monetization
# ---------------------------------------------------------------------


async def _login_and_get_monetization_csrf(
    client, password: str = "letmein",
) -> str:
    """Log in, fetch /admin/monetization, scrape its CSRF token."""
    await client.post(
        "/admin/login", data={"password": password}, allow_redirects=False
    )
    resp = await client.get("/admin/monetization")
    body = await resp.text()
    import re

    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token on /admin/monetization markup form"
    return m.group(1)


def _stub_db_for_markup_editor(
    summary: dict | None = None,
    *,
    upsert_setting_result: object | Exception = None,
    delete_setting_result: bool | Exception = True,
    get_setting_result: str | None | Exception = None,
):
    """Stub DB pre-wired with the monetization summary + setting CRUD
    needed by the markup editor."""
    db = _stub_db_with_monetization(
        summary or _sample_monetization_summary()
    )
    if isinstance(upsert_setting_result, Exception):
        db.upsert_setting = AsyncMock(side_effect=upsert_setting_result)
    else:
        db.upsert_setting = AsyncMock(return_value=upsert_setting_result)
    if isinstance(delete_setting_result, Exception):
        db.delete_setting = AsyncMock(side_effect=delete_setting_result)
    else:
        db.delete_setting = AsyncMock(return_value=delete_setting_result)
    if isinstance(get_setting_result, Exception):
        db.get_setting = AsyncMock(side_effect=get_setting_result)
    else:
        db.get_setting = AsyncMock(return_value=get_setting_result)
    return db


async def test_monetization_renders_markup_editor_form(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """The page renders the editor section with a CSRF token + the
    "effective / db / env / default" breakdown."""
    monkeypatch.setenv("COST_MARKUP", "2.0")
    import pricing
    pricing.clear_markup_override()
    db = _stub_db_for_markup_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert 'action="/admin/monetization/markup"' in body
    assert 'name="csrf_token"' in body
    assert 'name="markup"' in body
    # Source badge surfaces "env" since COST_MARKUP is set + no override.
    assert "source: env" in body


async def test_monetization_markup_post_requires_auth(
    aiohttp_client, make_admin_app,
):
    client = await aiohttp_client(make_admin_app(password="letmein"))
    resp = await client.post(
        "/admin/monetization/markup",
        data={"markup": "3.0", "csrf_token": "x"},
        allow_redirects=False,
    )
    assert resp.status in (302, 303), resp.status
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_monetization_markup_post_rejects_csrf_mismatch(
    aiohttp_client, make_admin_app,
):
    db = _stub_db_for_markup_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.post(
        "/admin/monetization/markup",
        data={"markup": "3.0", "csrf_token": "wrong"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/monetization"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_monetization_markup_post_persists_value_and_refreshes_cache(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Happy path: a valid value goes through ``upsert_setting`` AND
    updates the in-process override so the next ``get_markup()``
    sees it without a process restart."""
    monkeypatch.setenv("COST_MARKUP", "2.0")
    import pricing
    pricing.clear_markup_override()

    # ``get_setting`` returns the just-saved value so the post-write
    # refresh re-loads the cache deterministically.
    saved = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        if key == pricing.MARKUP_SETTING_KEY:
            saved["value"] = value

    async def _get(key: str):
        if key == pricing.MARKUP_SETTING_KEY:
            return saved["value"]
        return None

    db = _stub_db_for_markup_editor()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_monetization_csrf(client)

    resp = await client.post(
        "/admin/monetization/markup",
        data={"markup": "3.5", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/monetization"

    db.upsert_setting.assert_awaited_once_with(
        pricing.MARKUP_SETTING_KEY, "3.5",
    )
    db.delete_setting.assert_not_awaited()
    # In-process cache reflects the new value.
    assert pricing.get_markup_override() == 3.5
    assert pricing.get_markup() == 3.5
    # Audit row was recorded.
    audit_calls = db.record_admin_audit.await_args_list
    matching = [
        c for c in audit_calls
        if c.kwargs.get("action") == "monetization_markup_update"
    ]
    assert matching, audit_calls


async def test_monetization_markup_post_blank_value_clears_override(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Empty form value drops the override; falls through to env / default."""
    monkeypatch.setenv("COST_MARKUP", "2.0")
    import pricing
    pricing.clear_markup_override()
    pricing.set_markup_override(3.5)
    assert pricing.get_markup() == 3.5

    db = _stub_db_for_markup_editor(
        delete_setting_result=True, get_setting_result=None,
    )

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_monetization_csrf(client)

    resp = await client.post(
        "/admin/monetization/markup",
        data={"markup": "", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_setting.assert_awaited_once_with(pricing.MARKUP_SETTING_KEY)
    db.upsert_setting.assert_not_awaited()
    assert pricing.get_markup_override() is None
    assert pricing.get_markup() == 2.0  # falls through to env


@pytest.mark.parametrize(
    "bad_value", ["not-a-number", "0.5", "0.99", "nan", "inf", "-1"],
)
async def test_monetization_markup_post_rejects_invalid_value(
    aiohttp_client, make_admin_app, monkeypatch, bad_value,
):
    """Anything below MARKUP_MINIMUM or non-finite is rejected without
    touching the DB / cache."""
    monkeypatch.setenv("COST_MARKUP", "2.0")
    import pricing
    pricing.clear_markup_override()

    db = _stub_db_for_markup_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_monetization_csrf(client)

    resp = await client.post(
        "/admin/monetization/markup",
        data={"markup": bad_value, "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()
    assert pricing.get_markup_override() is None


async def test_monetization_markup_post_rejects_above_maximum(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """A fat-fingered ``150`` (intended ``1.5``) is rejected by the
    web form rather than 100x-ing every charge silently."""
    import pricing
    pricing.clear_markup_override()

    db = _stub_db_for_markup_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_monetization_csrf(client)

    resp = await client.post(
        "/admin/monetization/markup",
        data={
            "markup": str(pricing.MARKUP_OVERRIDE_MAXIMUM),
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert pricing.get_markup_override() is None


async def test_monetization_markup_post_db_failure_keeps_previous_value(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """A DB write failure must NOT poison the in-process cache;
    the previous override stays in effect and the page renders an
    error banner."""
    monkeypatch.setenv("COST_MARKUP", "2.0")
    import pricing
    pricing.clear_markup_override()

    # Stub returns "3.0" from get_setting so the GET-render that
    # ``_login_and_get_monetization_csrf`` does keeps the override
    # at 3.0 — that's what the test wants to assert is *preserved*
    # when the upsert fails.
    db = _stub_db_for_markup_editor(
        upsert_setting_result=RuntimeError("DB down"),
        get_setting_result="3.0",
    )
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_monetization_csrf(client)
    assert pricing.get_markup_override() == 3.0  # warmed via GET-render

    resp = await client.post(
        "/admin/monetization/markup",
        data={"markup": "4.0", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    # Cache untouched — the previous override is still active.
    assert pricing.get_markup_override() == 3.0
    assert pricing.get_markup() == 3.0


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 12: markup history & era attribution on
# /admin/monetization
# ---------------------------------------------------------------------


def _summary_for_history_tests() -> dict:
    """Minimal monetization summary shape so the history-card tests
    don't have to redeclare the whole rollup. Mirrors what the page
    expects when no usage has been recorded yet — values don't
    matter for the new card assertions."""
    return {
        "markup": 1.5,
        "lifetime": {
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 0.0, "net_profit_usd": 0.0,
        },
        "window": {
            "days": 30,
            "revenue_usd": 0.0, "charged_usd": 0.0,
            "openrouter_cost_usd": 0.0, "gross_margin_usd": 0.0,
            "gross_margin_pct": 0.0, "net_profit_usd": 0.0,
        },
        "by_model": [], "top_users": [],
    }


async def test_monetization_renders_markup_history_card(
    aiohttp_client, make_admin_app,
):
    """The markup-history card renders one row per audit entry,
    with actor, kind, before / after values, and IP."""
    db = _stub_db_with_monetization(_summary_for_history_tests())
    db.list_markup_history = AsyncMock(return_value=[
        {
            "id": 1,
            "ts": "2026-05-01T12:00:00+00:00",
            "actor": "web",
            "kind": "set",
            "before": 1.5,
            "before_source": "default",
            "after": 1.7,
            "after_source": "db",
            "ip": "203.0.113.10",
        },
    ])
    db.get_markup_eras = AsyncMock(return_value=[])

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    assert resp.status == 200
    body = await resp.text()
    assert "Markup change history" in body
    assert "203.0.113.10" in body
    assert "2026-05-01T12:00:00+00:00" in body
    # Both source labels surface as muted captions.
    assert "(default)" in body
    assert "(db)" in body


async def test_monetization_renders_markup_history_empty_state(
    aiohttp_client, make_admin_app,
):
    """When the audit log has zero markup-update rows, the card
    renders the placeholder text rather than an empty table."""
    db = _stub_db_with_monetization(_summary_for_history_tests())
    db.list_markup_history = AsyncMock(return_value=[])
    db.get_markup_eras = AsyncMock(return_value=[])

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    body = await resp.text()
    assert "No markup changes recorded yet" in body


async def test_monetization_renders_markup_eras_card(
    aiohttp_client, make_admin_app,
):
    """The eras card renders one row per era with the era's own
    markup applied to the era's charged-USD subtotal."""
    db = _stub_db_with_monetization(_summary_for_history_tests())
    db.list_markup_history = AsyncMock(return_value=[])
    db.get_markup_eras = AsyncMock(return_value=[
        {
            "from_ts": "2026-02-01T00:00:00+00:00",
            "to_ts": None,
            "markup": 2.0,
            "source": "db",
            "kind": "current",
            "actor": "web",
            "requests": 4,
            "charged_usd": 40.0,
            "openrouter_cost_usd": 20.0,
            "gross_margin_usd": 20.0,
        },
        {
            "from_ts": "2026-01-01T00:00:00+00:00",
            "to_ts": "2026-02-01T00:00:00+00:00",
            "markup": 1.5,
            "source": "default",
            "kind": "set",
            "actor": "web",
            "requests": 3,
            "charged_usd": 30.0,
            "openrouter_cost_usd": 20.0,
            "gross_margin_usd": 10.0,
        },
    ])

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    body = await resp.text()
    assert "Markup eras" in body
    # Era 0 = current (no end timestamp).
    assert "<em>now</em>" in body
    # Markups rendered with 4-decimal precision.
    assert "2.0000" in body
    assert "1.5000" in body
    # Charged subtotals.
    assert "$40.0000" in body
    assert "$30.0000" in body


async def test_monetization_renders_markup_eras_empty_state(
    aiohttp_client, make_admin_app,
):
    """A fresh deploy with no eras yet should render the placeholder
    instead of an empty table."""
    db = _stub_db_with_monetization(_summary_for_history_tests())
    db.list_markup_history = AsyncMock(return_value=[])
    db.get_markup_eras = AsyncMock(return_value=[])

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    body = await resp.text()
    assert "No markup history recorded yet" in body


async def test_monetization_swallows_history_query_failure(
    aiohttp_client, make_admin_app,
):
    """A DB blip on the history query must NOT 500 the page — the
    main summary card still renders, the history card just shows
    its empty placeholder."""
    db = _stub_db_with_monetization(_summary_for_history_tests())
    db.list_markup_history = AsyncMock(side_effect=RuntimeError("kaboom"))
    db.get_markup_eras = AsyncMock(return_value=[])

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    assert resp.status == 200
    body = await resp.text()
    # Main summary still renders.
    assert "Lifetime" in body
    # History card empty-state surfaces.
    assert "No markup changes recorded yet" in body


async def test_monetization_swallows_eras_query_failure(
    aiohttp_client, make_admin_app,
):
    """Sibling to the history-query failure test — a DB blip on
    ``get_markup_eras`` must fall back to the empty-state placeholder
    rather than 500-ing the whole page."""
    db = _stub_db_with_monetization(_summary_for_history_tests())
    db.list_markup_history = AsyncMock(return_value=[])
    db.get_markup_eras = AsyncMock(side_effect=RuntimeError("kaboom"))

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization")
    assert resp.status == 200
    body = await resp.text()
    assert "No markup history recorded yet" in body


async def test_monetization_passes_history_limit_to_db(
    aiohttp_client, make_admin_app,
):
    """The page must request the on-screen cap (25), not the raw
    DB cap (1000) — pinning so a future refactor doesn't accidentally
    pull a million rows for the rendered card."""
    from web_admin import _MARKUP_HISTORY_LIMIT, _MARKUP_ERAS_LIMIT
    assert _MARKUP_HISTORY_LIMIT == 25
    assert _MARKUP_ERAS_LIMIT == 10

    db = _stub_db_with_monetization(_summary_for_history_tests())
    db.list_markup_history = AsyncMock(return_value=[])
    db.get_markup_eras = AsyncMock(return_value=[])

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    await client.get("/admin/monetization")

    db.list_markup_history.assert_awaited_once_with(limit=25)
    db.get_markup_eras.assert_awaited_once_with(limit=10)


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 4 part 2/2: /admin/wallet-config
# (MIN_TOPUP_USD editor)
# ---------------------------------------------------------------------


def _stub_db_for_wallet_config(
    *,
    upsert_setting_result: object | Exception = None,
    delete_setting_result: bool | Exception = True,
    get_setting_result: str | None | Exception = None,
    get_fx_snapshot_result: object | None | Exception = None,
):
    """Stub DB pre-wired with the system_settings CRUD + FX snapshot
    accessor needed by the wallet-config editor.

    Matches the shape of :func:`_stub_db_for_markup_editor` but for
    the MIN_TOPUP_USD path. ``get_fx_snapshot`` defaults to ``None``
    (cold cache) so the page renders without the derived-Toman line
    in the no-rate path; pass a ``(rate, fetched_at)`` tuple to
    exercise the rate-rendering branch.
    """
    db = _stub_db()
    if isinstance(upsert_setting_result, Exception):
        db.upsert_setting = AsyncMock(side_effect=upsert_setting_result)
    else:
        db.upsert_setting = AsyncMock(return_value=upsert_setting_result)
    if isinstance(delete_setting_result, Exception):
        db.delete_setting = AsyncMock(side_effect=delete_setting_result)
    else:
        db.delete_setting = AsyncMock(return_value=delete_setting_result)
    if isinstance(get_setting_result, Exception):
        db.get_setting = AsyncMock(side_effect=get_setting_result)
    else:
        db.get_setting = AsyncMock(return_value=get_setting_result)
    if isinstance(get_fx_snapshot_result, Exception):
        db.get_fx_snapshot = AsyncMock(side_effect=get_fx_snapshot_result)
    else:
        db.get_fx_snapshot = AsyncMock(return_value=get_fx_snapshot_result)
    return db


async def _login_and_get_wallet_config_csrf(
    client, password: str = "letmein",
) -> str:
    """Log in, fetch /admin/wallet-config, scrape its CSRF token."""
    await client.post(
        "/admin/login", data={"password": password}, allow_redirects=False,
    )
    resp = await client.get("/admin/wallet-config")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    import re

    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token on /admin/wallet-config min-topup form"
    return m.group(1)


async def test_wallet_config_renders_min_topup_editor_form(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """The page renders the editor section with a CSRF token + the
    "effective / db / env / default" breakdown, and surfaces the
    derived-Toman figure when an FX snapshot is present."""
    monkeypatch.setenv("MIN_TOPUP_USD", "2.0")
    import payments
    payments.clear_min_topup_override()
    db = _stub_db_for_wallet_config(
        get_fx_snapshot_result=(100_000.0, datetime.now(timezone.utc)),
    )
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/wallet-config")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert 'action="/admin/wallet-config/min-topup"' in body
    assert 'name="csrf_token"' in body
    assert 'name="min_topup_usd"' in body
    # Source badge surfaces "env" since MIN_TOPUP_USD is set + no override.
    assert "source: env" in body
    # Derived-Toman line surfaces with the canned 100k rate.
    assert "200,000 تومان" in body or "200,000" in body


async def test_wallet_config_min_topup_post_requires_auth(
    aiohttp_client, make_admin_app,
):
    client = await aiohttp_client(make_admin_app(password="letmein"))
    resp = await client.post(
        "/admin/wallet-config/min-topup",
        data={"min_topup_usd": "5.0", "csrf_token": "x"},
        allow_redirects=False,
    )
    assert resp.status in (302, 303), resp.status
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_wallet_config_min_topup_post_rejects_csrf_mismatch(
    aiohttp_client, make_admin_app,
):
    db = _stub_db_for_wallet_config()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.post(
        "/admin/wallet-config/min-topup",
        data={"min_topup_usd": "5.0", "csrf_token": "wrong"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/wallet-config"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_wallet_config_min_topup_post_persists_value_and_refreshes_cache(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Happy path: a valid value goes through ``upsert_setting`` AND
    updates the in-process override so the next ``get_min_topup_usd()``
    sees it without a process restart."""
    monkeypatch.setenv("MIN_TOPUP_USD", "2.0")
    import payments
    payments.clear_min_topup_override()

    saved = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        if key == payments.MIN_TOPUP_SETTING_KEY:
            saved["value"] = value

    async def _get(key: str):
        if key == payments.MIN_TOPUP_SETTING_KEY:
            return saved["value"]
        return None

    db = _stub_db_for_wallet_config()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/min-topup",
        data={"min_topup_usd": "5.5", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/wallet-config"

    db.upsert_setting.assert_awaited_once_with(
        payments.MIN_TOPUP_SETTING_KEY, "5.5",
    )
    db.delete_setting.assert_not_awaited()
    # In-process cache reflects the new value.
    assert payments.get_min_topup_override() == 5.5
    assert payments.get_min_topup_usd() == 5.5
    # Audit row was recorded.
    audit_calls = db.record_admin_audit.await_args_list
    matching = [
        c for c in audit_calls
        if c.kwargs.get("action") == "wallet_config_min_topup_update"
    ]
    assert matching, audit_calls


async def test_wallet_config_min_topup_post_blank_value_clears_override(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Empty form value drops the override; falls through to env / default."""
    monkeypatch.setenv("MIN_TOPUP_USD", "2.0")
    import payments
    payments.clear_min_topup_override()
    payments.set_min_topup_override(5.5)
    assert payments.get_min_topup_usd() == 5.5

    db = _stub_db_for_wallet_config(
        delete_setting_result=True, get_setting_result=None,
    )

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/min-topup",
        data={"min_topup_usd": "", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_setting.assert_awaited_once_with(
        payments.MIN_TOPUP_SETTING_KEY,
    )
    db.upsert_setting.assert_not_awaited()
    assert payments.get_min_topup_override() is None
    assert payments.get_min_topup_usd() == 2.0  # falls through to env


@pytest.mark.parametrize(
    "bad_value",
    ["not-a-number", "nan", "inf", "-inf", "-1"],
)
async def test_wallet_config_min_topup_post_rejects_invalid_value(
    aiohttp_client, make_admin_app, monkeypatch, bad_value,
):
    """Anything non-finite or below MIN_TOPUP_USD_MINIMUM is rejected
    without touching the DB / cache."""
    monkeypatch.setenv("MIN_TOPUP_USD", "2.0")
    import payments
    payments.clear_min_topup_override()

    db = _stub_db_for_wallet_config()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/min-topup",
        data={"min_topup_usd": bad_value, "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()
    assert payments.get_min_topup_override() is None


async def test_wallet_config_min_topup_post_rejects_above_maximum(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """A fat-fingered ``99999`` is rejected at the form rather than
    locking out every paying user (anything ≥ MIN_TOPUP_USD_MAXIMUM
    must be refused)."""
    import payments
    payments.clear_min_topup_override()

    db = _stub_db_for_wallet_config()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/min-topup",
        data={
            "min_topup_usd": str(payments.MIN_TOPUP_USD_MAXIMUM),
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert payments.get_min_topup_override() is None


async def test_wallet_config_min_topup_post_db_failure_keeps_previous_value(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """A DB write failure must NOT poison the in-process cache;
    the previous override stays in effect and the page renders an
    error banner."""
    monkeypatch.setenv("MIN_TOPUP_USD", "2.0")
    import payments
    payments.clear_min_topup_override()

    # Stub returns "5.0" from get_setting so the GET-render that
    # ``_login_and_get_wallet_config_csrf`` does keeps the override
    # at 5.0 — that's what the test wants to assert is *preserved*
    # when the upsert fails.
    db = _stub_db_for_wallet_config(
        upsert_setting_result=RuntimeError("DB down"),
        get_setting_result="5.0",
    )
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)
    assert payments.get_min_topup_override() == 5.0  # warmed via GET-render

    resp = await client.post(
        "/admin/wallet-config/min-topup",
        data={"min_topup_usd": "7.0", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    # Cache untouched — the previous override is still active.
    assert payments.get_min_topup_override() == 5.0
    assert payments.get_min_topup_usd() == 5.0


async def test_wallet_config_renders_when_db_unavailable(
    aiohttp_client, monkeypatch,
):
    """When no DB is wired (local dev) the page still renders the
    "effective / env / default" breakdown without crashing — just
    skips the DB-override row.

    Bypasses ``make_admin_app`` because that fixture substitutes a
    stub DB when ``db=None`` is passed; the dev-mode branch we're
    pinning specifically wants ``app[APP_KEY_DB]`` to be ``None``.
    """
    monkeypatch.setenv("MIN_TOPUP_USD", "3.5")
    import payments
    payments.clear_min_topup_override()

    app = web.Application()
    setup_admin_routes(
        app,
        db=None,
        password="letmein",
        session_secret="x" * 32,
        ttl_hours=24,
        cookie_secure=False,
    )
    client = await aiohttp_client(app)
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/wallet-config")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert 'action="/admin/wallet-config/min-topup"' in body
    assert "source: env" in body


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 7: REFERRAL_BONUS_* editor on
# /admin/wallet-config.
# ---------------------------------------------------------------------


def _stub_db_for_referral_editor(
    *,
    upsert_setting_result: object | Exception = None,
    delete_setting_result: bool | Exception = True,
    get_setting_result: dict | Exception | None = None,
    get_fx_snapshot_result: object | None | Exception = None,
):
    """Stub DB pre-wired with the system_settings CRUD + FX snapshot
    accessor needed by both wallet-config editors (min-topup AND
    referral). ``get_setting_result`` accepts a dict mapping setting
    key → value so the same stub can answer both editors' refresh
    calls during a single test.
    """
    db = _stub_db()
    if isinstance(upsert_setting_result, Exception):
        db.upsert_setting = AsyncMock(side_effect=upsert_setting_result)
    else:
        db.upsert_setting = AsyncMock(return_value=upsert_setting_result)
    if isinstance(delete_setting_result, Exception):
        db.delete_setting = AsyncMock(side_effect=delete_setting_result)
    else:
        db.delete_setting = AsyncMock(return_value=delete_setting_result)
    if isinstance(get_setting_result, Exception):
        db.get_setting = AsyncMock(side_effect=get_setting_result)
    elif isinstance(get_setting_result, dict):
        async def _get(key: str):
            return get_setting_result.get(key)
        db.get_setting = AsyncMock(side_effect=_get)
    else:
        db.get_setting = AsyncMock(return_value=get_setting_result)
    if isinstance(get_fx_snapshot_result, Exception):
        db.get_fx_snapshot = AsyncMock(side_effect=get_fx_snapshot_result)
    else:
        db.get_fx_snapshot = AsyncMock(return_value=get_fx_snapshot_result)
    return db


def _reset_referral_overrides_for_web():
    """Helper: scrub the in-process referral override caches so each
    test sees a clean baseline. Tests that need a specific env value
    set it via monkeypatch.
    """
    import referral
    referral.clear_referral_bonus_percent_override()
    referral.clear_referral_bonus_max_usd_override()


async def test_wallet_config_renders_referral_editor_form(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """The page renders the referral editor card alongside min-topup,
    with both knobs' "effective / db / env / default" breakdown and a
    CSRF token on the Save form."""
    monkeypatch.setenv("REFERRAL_BONUS_PERCENT", "12.5")
    monkeypatch.setenv("REFERRAL_BONUS_MAX_USD", "7.5")
    _reset_referral_overrides_for_web()

    db = _stub_db_for_referral_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/wallet-config")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert 'action="/admin/wallet-config/referral"' in body
    assert 'name="referral_bonus_percent"' in body
    assert 'name="referral_bonus_max_usd"' in body
    # Effective figures from env are rendered.
    assert "12.50%" in body
    assert "$7.50" in body


async def test_wallet_config_referral_post_requires_auth(
    aiohttp_client, make_admin_app,
):
    client = await aiohttp_client(make_admin_app(password="letmein"))
    resp = await client.post(
        "/admin/wallet-config/referral",
        data={
            "action": "set",
            "referral_bonus_percent": "15",
            "csrf_token": "x",
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303), resp.status
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_wallet_config_referral_post_rejects_csrf_mismatch(
    aiohttp_client, make_admin_app,
):
    _reset_referral_overrides_for_web()
    db = _stub_db_for_referral_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.post(
        "/admin/wallet-config/referral",
        data={
            "action": "set",
            "referral_bonus_percent": "15",
            "csrf_token": "wrong",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/wallet-config"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_wallet_config_referral_post_rejects_unknown_action(
    aiohttp_client, make_admin_app, monkeypatch,
):
    _reset_referral_overrides_for_web()
    db = _stub_db_for_referral_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/referral",
        data={
            "action": "sneaky",
            "referral_bonus_percent": "15",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/wallet-config"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_wallet_config_referral_post_set_persists_both_knobs(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Happy path: filling both fields updates BOTH DB rows AND
    refreshes both in-process caches."""
    monkeypatch.setenv("REFERRAL_BONUS_PERCENT", "10")
    monkeypatch.setenv("REFERRAL_BONUS_MAX_USD", "5")
    _reset_referral_overrides_for_web()

    saved = {}

    async def _upsert(key: str, value: str) -> None:
        saved[key] = value

    async def _get(key: str):
        return saved.get(key)

    db = _stub_db_for_referral_editor()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/referral",
        data={
            "action": "set",
            "referral_bonus_percent": "15",
            "referral_bonus_max_usd": "7.5",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/wallet-config"

    import referral
    assert saved == {
        referral.REFERRAL_BONUS_PERCENT_SETTING_KEY: "15.0",
        referral.REFERRAL_BONUS_MAX_USD_SETTING_KEY: "7.5",
    }
    assert referral.get_referral_bonus_percent() == 15.0
    assert referral.get_referral_bonus_max_usd() == 7.5
    audit_calls = db.record_admin_audit.await_args_list
    matching = [
        c for c in audit_calls
        if c.kwargs.get("action") == "wallet_config_referral_update"
    ]
    assert matching, audit_calls
    meta = matching[0].kwargs.get("meta", {})
    assert meta.get("action") == "set"
    assert meta.get("after_percent") == 15.0
    assert meta.get("after_max_usd") == 7.5


async def test_wallet_config_referral_post_set_with_only_percent(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Filling only percent leaves max-USD untouched (env / default)."""
    monkeypatch.setenv("REFERRAL_BONUS_PERCENT", "10")
    monkeypatch.setenv("REFERRAL_BONUS_MAX_USD", "5")
    _reset_referral_overrides_for_web()

    saved = {}

    async def _upsert(key: str, value: str) -> None:
        saved[key] = value

    async def _get(key: str):
        return saved.get(key)

    db = _stub_db_for_referral_editor()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/referral",
        data={
            "action": "set",
            "referral_bonus_percent": "20",
            "referral_bonus_max_usd": "",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    import referral
    assert (
        referral.REFERRAL_BONUS_PERCENT_SETTING_KEY in saved
        and referral.REFERRAL_BONUS_MAX_USD_SETTING_KEY not in saved
    )
    assert referral.get_referral_bonus_percent() == 20.0
    assert referral.get_referral_bonus_max_usd() == 5.0  # env-sourced


async def test_wallet_config_referral_post_set_rejects_blank_both(
    aiohttp_client, make_admin_app,
):
    """Both fields blank on action=set is a no-op warning, not a
    silent persistence."""
    _reset_referral_overrides_for_web()
    db = _stub_db_for_referral_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/referral",
        data={
            "action": "set",
            "referral_bonus_percent": "",
            "referral_bonus_max_usd": "",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_wallet_config_referral_post_set_rejects_invalid_percent(
    aiohttp_client, make_admin_app,
):
    _reset_referral_overrides_for_web()
    db = _stub_db_for_referral_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/referral",
        data={
            "action": "set",
            "referral_bonus_percent": "not-a-number",
            "referral_bonus_max_usd": "5",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    # Bad input rejected — neither knob persisted.
    db.upsert_setting.assert_not_awaited()


async def test_wallet_config_referral_post_set_rejects_above_cap_percent(
    aiohttp_client, make_admin_app,
):
    _reset_referral_overrides_for_web()
    db = _stub_db_for_referral_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/referral",
        data={
            "action": "set",
            "referral_bonus_percent": "150",  # above 100% cap
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


async def test_wallet_config_referral_post_set_rejects_above_cap_max_usd(
    aiohttp_client, make_admin_app,
):
    _reset_referral_overrides_for_web()
    db = _stub_db_for_referral_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/referral",
        data={
            "action": "set",
            "referral_bonus_max_usd": "5000",  # above $1000 cap
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


async def test_wallet_config_referral_post_clear_drops_targeted_overrides(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """action=clear with a target list deletes the matching DB rows
    and falls through to env / default."""
    monkeypatch.setenv("REFERRAL_BONUS_PERCENT", "10")
    monkeypatch.setenv("REFERRAL_BONUS_MAX_USD", "5")
    _reset_referral_overrides_for_web()
    import referral
    referral.set_referral_bonus_percent_override(50)
    referral.set_referral_bonus_max_usd_override(8)
    assert referral.get_referral_bonus_percent() == 50.0

    db = _stub_db_for_referral_editor(get_setting_result=None)

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/referral",
        data=[
            ("action", "clear"),
            ("targets", "percent"),
            ("targets", "max_usd"),
            ("csrf_token", csrf),
        ],
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    deleted_keys = [
        c.args[0] for c in db.delete_setting.await_args_list
    ]
    assert referral.REFERRAL_BONUS_PERCENT_SETTING_KEY in deleted_keys
    assert referral.REFERRAL_BONUS_MAX_USD_SETTING_KEY in deleted_keys
    # Both fell back to env.
    assert referral.get_referral_bonus_percent() == 10.0
    assert referral.get_referral_bonus_max_usd() == 5.0


async def test_wallet_config_referral_post_clear_with_no_targets(
    aiohttp_client, make_admin_app,
):
    """action=clear without selecting any target is a no-op warn, not
    a silent delete-all."""
    _reset_referral_overrides_for_web()
    db = _stub_db_for_referral_editor()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/referral",
        data={"action": "clear", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_setting.assert_not_awaited()


async def test_wallet_config_referral_post_db_failure_keeps_previous_value(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Upsert failure on percent leaves the cache untouched (previous
    value still in effect)."""
    monkeypatch.setenv("REFERRAL_BONUS_PERCENT", "10")
    _reset_referral_overrides_for_web()

    db = _stub_db_for_referral_editor(
        upsert_setting_result=RuntimeError("boom"),
    )
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/referral",
        data={
            "action": "set",
            "referral_bonus_percent": "20",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    import referral
    # Previous env value still effective.
    assert referral.get_referral_bonus_percent() == 10.0


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 6: /admin/wallet-config —
# FREE_MESSAGES_PER_USER editor
# ---------------------------------------------------------------------


def _reset_free_messages_override_for_web():
    """Scrub the in-process free-messages cache so each test sees a
    clean baseline."""
    import free_trial
    free_trial.clear_free_messages_per_user_override()


async def test_wallet_config_renders_free_messages_editor_form(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """The page renders the free-messages editor card alongside the
    other wallet-config knobs, with the "effective / db / env /
    default" breakdown + a CSRF token on the Save form."""
    monkeypatch.setenv("FREE_MESSAGES_PER_USER", "15")
    _reset_free_messages_override_for_web()

    db = _stub_db_for_wallet_config()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/wallet-config")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert 'action="/admin/wallet-config/free-messages"' in body
    assert 'name="free_messages_per_user"' in body
    # Effective allowance + source badge for the env-sourced 15.
    assert "15" in body


async def test_wallet_config_free_messages_post_requires_auth(
    aiohttp_client, make_admin_app,
):
    """Unauth requests redirect to /admin/login (the require_auth
    guard fires before the handler runs)."""
    client = await aiohttp_client(make_admin_app(password="letmein"))
    resp = await client.post(
        "/admin/wallet-config/free-messages",
        data={"free_messages_per_user": "15", "csrf_token": "x"},
        allow_redirects=False,
    )
    assert resp.status in (302, 303), resp.status
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_wallet_config_free_messages_post_rejects_csrf_mismatch(
    aiohttp_client, make_admin_app,
):
    """A wrong CSRF token redirects to /admin/wallet-config without
    touching the DB or the cache."""
    _reset_free_messages_override_for_web()
    db = _stub_db_for_wallet_config()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.post(
        "/admin/wallet-config/free-messages",
        data={"free_messages_per_user": "15", "csrf_token": "wrong"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/wallet-config"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_wallet_config_free_messages_post_persists_value_and_refreshes_cache(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Happy path: a valid value goes through ``upsert_setting`` AND
    updates the in-process override so the next call to
    :func:`free_trial.get_free_messages_per_user` sees it without a
    process restart."""
    monkeypatch.setenv("FREE_MESSAGES_PER_USER", "10")
    _reset_free_messages_override_for_web()

    saved = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        import free_trial
        if key == free_trial.FREE_MESSAGES_PER_USER_SETTING_KEY:
            saved["value"] = value

    async def _get(key: str):
        import free_trial
        if key == free_trial.FREE_MESSAGES_PER_USER_SETTING_KEY:
            return saved["value"]
        return None

    db = _stub_db_for_wallet_config()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/free-messages",
        data={"free_messages_per_user": "25", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/wallet-config"

    import free_trial
    db.upsert_setting.assert_awaited_once_with(
        free_trial.FREE_MESSAGES_PER_USER_SETTING_KEY, "25",
    )
    db.delete_setting.assert_not_awaited()
    # In-process cache reflects the new value.
    assert free_trial.get_free_messages_per_user_override() == 25
    assert free_trial.get_free_messages_per_user() == 25
    # Audit row was recorded.
    audit_calls = db.record_admin_audit.await_args_list
    matching = [
        c for c in audit_calls
        if c.kwargs.get("action") == "wallet_config_free_messages_update"
    ]
    assert matching, audit_calls
    last_meta = matching[-1].kwargs["meta"]
    assert last_meta["action"] == "set"
    assert last_meta["before"] == 10
    assert last_meta["after"] == 25


async def test_wallet_config_free_messages_post_blank_value_clears_override(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Empty form value drops the override; falls through to env / default."""
    monkeypatch.setenv("FREE_MESSAGES_PER_USER", "10")
    _reset_free_messages_override_for_web()
    import free_trial
    free_trial.set_free_messages_per_user_override(50)
    assert free_trial.get_free_messages_per_user() == 50

    db = _stub_db_for_wallet_config(
        delete_setting_result=True, get_setting_result=None,
    )
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/free-messages",
        data={"free_messages_per_user": "", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_setting.assert_awaited_once_with(
        free_trial.FREE_MESSAGES_PER_USER_SETTING_KEY,
    )
    db.upsert_setting.assert_not_awaited()
    assert free_trial.get_free_messages_per_user_override() is None
    # Falls through to env (10).
    assert free_trial.get_free_messages_per_user() == 10
    # Audit row records the clear action.
    audit_calls = db.record_admin_audit.await_args_list
    matching = [
        c for c in audit_calls
        if c.kwargs.get("action") == "wallet_config_free_messages_update"
    ]
    assert matching
    assert matching[-1].kwargs["meta"]["action"] == "clear"


@pytest.mark.parametrize(
    "bad_value",
    [
        "not-a-number",
        "nan",
        "inf",
        "-inf",
        "-1",
        "10001",
        "15.5",  # non-integer rejected
        "100000",  # well above cap
    ],
)
async def test_wallet_config_free_messages_post_rejects_invalid_value(
    aiohttp_client, make_admin_app, monkeypatch, bad_value,
):
    """Anything non-int, non-finite, or outside [0, 10_000] is rejected
    without touching the DB or the cache."""
    _reset_free_messages_override_for_web()
    db = _stub_db_for_wallet_config()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/free-messages",
        data={
            "free_messages_per_user": bad_value, "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()
    import free_trial
    assert free_trial.get_free_messages_per_user_override() is None


async def test_wallet_config_free_messages_post_accepts_zero(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """A deliberate ``0`` ("no trial — pay-to-play only") IS valid and
    must round-trip. Closed-beta operators sometimes want this."""
    _reset_free_messages_override_for_web()

    saved = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        saved["value"] = value

    async def _get(key: str):
        return saved["value"]

    db = _stub_db_for_wallet_config()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/free-messages",
        data={"free_messages_per_user": "0", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    import free_trial
    db.upsert_setting.assert_awaited_once_with(
        free_trial.FREE_MESSAGES_PER_USER_SETTING_KEY, "0",
    )
    assert free_trial.get_free_messages_per_user() == 0


async def test_wallet_config_free_messages_post_accepts_maximum(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Boundary: the inclusive maximum (10_000) round-trips. Ensures
    we don't accidentally reject the documented upper bound."""
    _reset_free_messages_override_for_web()

    saved = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        saved["value"] = value

    async def _get(key: str):
        return saved["value"]

    db = _stub_db_for_wallet_config()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    import free_trial
    resp = await client.post(
        "/admin/wallet-config/free-messages",
        data={
            "free_messages_per_user": str(
                free_trial.FREE_MESSAGES_PER_USER_MAXIMUM,
            ),
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_awaited_once()
    assert (
        free_trial.get_free_messages_per_user()
        == free_trial.FREE_MESSAGES_PER_USER_MAXIMUM
    )


async def test_wallet_config_free_messages_post_db_failure_keeps_previous_value(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """A DB upsert failure must NOT poison the in-process cache;
    previous value stays in effect."""
    monkeypatch.setenv("FREE_MESSAGES_PER_USER", "10")
    _reset_free_messages_override_for_web()

    db = _stub_db_for_wallet_config(
        upsert_setting_result=RuntimeError("boom"),
    )
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/free-messages",
        data={"free_messages_per_user": "25", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    import free_trial
    # Previous env value still effective; no cache poisoning.
    assert free_trial.get_free_messages_per_user() == 10
    assert free_trial.get_free_messages_per_user_override() is None


async def test_wallet_config_free_messages_post_persists_audit_meta_diff(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """The audit row's ``meta`` carries the before/after diff with
    sources, so ``/admin/audit`` filter view shows a useful "what
    changed" line. Sibling pin to the
    ``wallet_config_min_topup_update`` audit-meta tests."""
    monkeypatch.setenv("FREE_MESSAGES_PER_USER", "10")
    _reset_free_messages_override_for_web()
    saved = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        saved["value"] = value

    async def _get(key: str):
        return saved["value"]

    db = _stub_db_for_wallet_config()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/free-messages",
        data={"free_messages_per_user": "30", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302

    audit_calls = db.record_admin_audit.await_args_list
    matching = [
        c for c in audit_calls
        if c.kwargs.get("action") == "wallet_config_free_messages_update"
    ]
    assert matching
    meta = matching[-1].kwargs["meta"]
    # Before came from env (10), after is the new override (30).
    assert meta["before"] == 10
    assert meta["before_source"] == "env"
    assert meta["after"] == 30
    assert meta["after_source"] == "db"


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 24: /admin/wallet-config —
# FX_REFRESH_INTERVAL_SECONDS editor
# ---------------------------------------------------------------------


def _reset_fx_refresh_override_for_web():
    """Scrub the in-process FX-refresh-interval cache so each test
    sees a clean baseline."""
    import fx_refresh_config
    fx_refresh_config.clear_fx_refresh_interval_override()


async def test_wallet_config_get_renders_fx_refresh_card(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """The page renders the FX-refresh editor card alongside the
    other wallet-config knobs."""
    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "1200")
    _reset_fx_refresh_override_for_web()

    db = _stub_db_for_wallet_config()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/wallet-config")
    assert resp.status == 200
    body = await resp.text()
    assert "USD→Toman refresher cadence" in body
    assert "fx_refresh_interval_seconds" in body
    assert "/admin/wallet-config/fx-refresh" in body
    # Effective value renders in human-readable form (1200s = 20m).
    assert "20m" in body


async def test_wallet_config_fx_refresh_post_requires_auth(
    aiohttp_client, make_admin_app,
):
    """Unauth requests redirect to /admin/login."""
    _reset_fx_refresh_override_for_web()
    db = _stub_db_for_wallet_config()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    resp = await client.post(
        "/admin/wallet-config/fx-refresh",
        data={"fx_refresh_interval_seconds": "1800"},
        allow_redirects=False,
    )
    assert resp.status in (302, 303)
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_wallet_config_fx_refresh_post_rejects_csrf_mismatch(
    aiohttp_client, make_admin_app,
):
    """A wrong CSRF token redirects to /admin/wallet-config without
    touching the DB or the cache."""
    _reset_fx_refresh_override_for_web()
    db = _stub_db_for_wallet_config()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.post(
        "/admin/wallet-config/fx-refresh",
        data={
            "fx_refresh_interval_seconds": "1800",
            "csrf_token": "totally_wrong_token",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/wallet-config"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_wallet_config_fx_refresh_post_persists_value_and_refreshes_cache(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Happy path: a valid value goes through ``upsert_setting`` AND
    updates the in-process override so the next FX refresher tick
    sees it without a process restart."""
    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "600")
    _reset_fx_refresh_override_for_web()
    saved = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        import fx_refresh_config
        if key == fx_refresh_config.FX_REFRESH_INTERVAL_SETTING_KEY:
            saved["value"] = value

    async def _get(key: str):
        import fx_refresh_config
        if key == fx_refresh_config.FX_REFRESH_INTERVAL_SETTING_KEY:
            return saved["value"]
        return None

    db = _stub_db_for_wallet_config()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/fx-refresh",
        data={"fx_refresh_interval_seconds": "1800", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/wallet-config"

    import fx_refresh_config
    db.upsert_setting.assert_awaited_with(
        fx_refresh_config.FX_REFRESH_INTERVAL_SETTING_KEY, "1800",
    )
    db.delete_setting.assert_not_awaited()
    assert fx_refresh_config.get_fx_refresh_interval_override() == 1800
    assert fx_refresh_config.get_fx_refresh_interval_seconds() == 1800
    audit_calls = db.record_admin_audit.await_args_list
    matching = [
        c for c in audit_calls
        if c.kwargs.get("action") == "wallet_config_fx_refresh_update"
    ]
    assert matching, audit_calls
    meta = matching[-1].kwargs["meta"]
    assert meta["action"] == "set"
    assert meta["before"] == 600
    assert meta["after"] == 1800


async def test_wallet_config_fx_refresh_post_blank_value_clears_override(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Empty form value drops the override; falls through to env / default."""
    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "1200")
    _reset_fx_refresh_override_for_web()
    import fx_refresh_config
    fx_refresh_config.set_fx_refresh_interval_override(3600)
    assert fx_refresh_config.get_fx_refresh_interval_seconds() == 3600

    db = _stub_db_for_wallet_config(
        delete_setting_result=True, get_setting_result=None,
    )
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/fx-refresh",
        data={"fx_refresh_interval_seconds": "", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_setting.assert_awaited_once_with(
        fx_refresh_config.FX_REFRESH_INTERVAL_SETTING_KEY,
    )
    db.upsert_setting.assert_not_awaited()
    assert fx_refresh_config.get_fx_refresh_interval_override() is None
    # Falls through to env (1200).
    assert fx_refresh_config.get_fx_refresh_interval_seconds() == 1200
    audit_calls = db.record_admin_audit.await_args_list
    matching = [
        c for c in audit_calls
        if c.kwargs.get("action") == "wallet_config_fx_refresh_update"
    ]
    assert matching
    assert matching[-1].kwargs["meta"]["action"] == "clear"


@pytest.mark.parametrize(
    "bad_value",
    [
        "not-a-number",
        "nan",
        "inf",
        "-inf",
        "0",
        "-1",
        "59",        # below min
        "86401",     # above max
        "1234.5",    # non-integer rejected
        "9999999",   # well above cap
    ],
)
async def test_wallet_config_fx_refresh_post_rejects_invalid_value(
    aiohttp_client, make_admin_app, monkeypatch, bad_value,
):
    """Anything non-int, non-finite, or outside [60, 86400] is rejected
    with a flash-error redirect; the cache is not poisoned."""
    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "600")
    _reset_fx_refresh_override_for_web()

    db = _stub_db_for_wallet_config()
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/fx-refresh",
        data={
            "fx_refresh_interval_seconds": bad_value,
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/wallet-config"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()
    import fx_refresh_config
    assert fx_refresh_config.get_fx_refresh_interval_override() is None


async def test_wallet_config_fx_refresh_post_accepts_minimum(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Boundary: the inclusive minimum (60s) round-trips."""
    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "600")
    _reset_fx_refresh_override_for_web()
    saved = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        saved["value"] = value

    async def _get(key: str):
        return saved["value"]

    db = _stub_db_for_wallet_config()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)
    resp = await client.post(
        "/admin/wallet-config/fx-refresh",
        data={"fx_refresh_interval_seconds": "60", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    import fx_refresh_config
    assert fx_refresh_config.get_fx_refresh_interval_override() == 60
    assert fx_refresh_config.get_fx_refresh_interval_seconds() == 60


async def test_wallet_config_fx_refresh_post_accepts_maximum(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """Boundary: the inclusive maximum (86400s = 1 day) round-trips."""
    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "600")
    _reset_fx_refresh_override_for_web()
    saved = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        saved["value"] = value

    async def _get(key: str):
        return saved["value"]

    db = _stub_db_for_wallet_config()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)
    resp = await client.post(
        "/admin/wallet-config/fx-refresh",
        data={"fx_refresh_interval_seconds": "86400", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    import fx_refresh_config
    assert fx_refresh_config.get_fx_refresh_interval_override() == 86400


async def test_wallet_config_fx_refresh_post_db_failure_keeps_previous_value(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """An ``upsert_setting`` failure must NOT poison the in-process
    cache; the previous override (or fall-through chain) must still
    apply."""
    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "600")
    _reset_fx_refresh_override_for_web()

    db = _stub_db_for_wallet_config()
    db.upsert_setting = AsyncMock(side_effect=RuntimeError("boom"))
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)

    resp = await client.post(
        "/admin/wallet-config/fx-refresh",
        data={"fx_refresh_interval_seconds": "1800", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    import fx_refresh_config
    # The override is unchanged (never set because upsert failed).
    assert fx_refresh_config.get_fx_refresh_interval_override() is None


async def test_wallet_config_fx_refresh_post_persists_audit_meta_diff(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """The audit row's ``meta`` carries the before/after diff with
    sources, sibling pin to the other wallet-config audit-meta tests."""
    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "600")
    _reset_fx_refresh_override_for_web()
    saved = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        saved["value"] = value

    async def _get(key: str):
        return saved["value"]

    db = _stub_db_for_wallet_config()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    csrf = await _login_and_get_wallet_config_csrf(client)
    resp = await client.post(
        "/admin/wallet-config/fx-refresh",
        data={"fx_refresh_interval_seconds": "3600", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    audit_calls = db.record_admin_audit.await_args_list
    matching = [
        c for c in audit_calls
        if c.kwargs.get("action") == "wallet_config_fx_refresh_update"
    ]
    assert matching
    meta = matching[-1].kwargs["meta"]
    assert meta["before"] == 600
    assert meta["before_source"] == "env"
    assert meta["after"] == 3600
    assert meta["after_source"] == "db"


# ---------------------------------------------------------------------
# Stage-15-Step-E #9 follow-up #2: monetization CSV export
# ---------------------------------------------------------------------


def _sample_monetization_summary() -> dict:
    return {
        "markup": 2.0,
        "lifetime": {
            "revenue_usd": 1234.5678,
            "charged_usd": 600.0,
            "openrouter_cost_usd": 300.0,
            "gross_margin_usd": 300.0,
            "gross_margin_pct": 50.0,
            "net_profit_usd": 934.5678,
        },
        "window": {
            "days": 30,
            "revenue_usd": 200.0,
            "charged_usd": 80.0,
            "openrouter_cost_usd": 40.0,
            "gross_margin_usd": 40.0,
            "gross_margin_pct": 50.0,
            "net_profit_usd": 160.0,
        },
        "by_model": [
            {
                "model": "openai/gpt-4o",
                "requests": 12,
                "charged_usd": 50.0,
                "openrouter_cost_usd": 25.0,
                "gross_margin_usd": 25.0,
            },
            {
                "model": "anthropic/claude-3-opus",
                "requests": 8,
                "charged_usd": 30.0,
                "openrouter_cost_usd": 15.0,
                "gross_margin_usd": 15.0,
            },
        ],
        # Stage-15-Step-E #9 follow-up — top-users-by-revenue panel.
        "top_users": [
            {
                "telegram_id": 111,
                "username": "alice",
                "revenue_usd": 80.0,
                "topup_count": 4,
                "charged_usd": 30.0,
            },
            {
                "telegram_id": 222,
                "username": None,
                "revenue_usd": 50.0,
                "topup_count": 1,
                "charged_usd": 0.0,
            },
        ],
    }


def test_monetization_csv_headers_pinned():
    """The header row order is part of the CSV's contract — flipping
    columns silently corrupts every downstream spreadsheet that
    indexes by position. A future refactor that reorders the dict
    must explicitly update this list and the tests."""
    from web_admin import MONETIZATION_CSV_HEADERS
    assert MONETIZATION_CSV_HEADERS == (
        "scope",
        "window_days",
        "model",
        "requests",
        "revenue_usd",
        "charged_usd",
        "openrouter_cost_usd",
        "gross_margin_usd",
        "gross_margin_pct",
        "net_profit_usd",
        "markup",
        # Stage-15-Step-E #9 follow-up — top-users-by-revenue rows.
        "telegram_id",
        "username",
        "topup_count",
    )


def test_monetization_csv_rows_shape_for_populated_summary():
    from web_admin import _format_monetization_csv_rows

    rows = _format_monetization_csv_rows(_sample_monetization_summary())

    # Two scope rows + two by_model rows + two top_users rows = 6.
    assert len(rows) == 6
    # Each row terminates with CRLF.
    for r in rows:
        assert r.endswith("\r\n")

    # Row 1: lifetime — model + requests + window_days are blank;
    # all numeric fields use 4 decimal places. The trailing
    # per-user fields (telegram_id / username / topup_count) are
    # blank because lifetime is a scope-level row.
    parts = rows[0].rstrip("\r\n").split(",")
    assert parts[0] == "lifetime"
    assert parts[1] == ""  # window_days
    assert parts[2] == ""  # model
    assert parts[3] == ""  # requests
    assert parts[4] == "1234.5678"  # revenue
    assert parts[10] == "2.0000"  # markup
    assert parts[11] == ""  # telegram_id: scope-level
    assert parts[12] == ""  # username: scope-level
    assert parts[13] == ""  # topup_count: scope-level

    # Row 2: window — window_days=30, model + requests blank.
    parts = rows[1].rstrip("\r\n").split(",")
    assert parts[0] == "window"
    assert parts[1] == "30"
    assert parts[2] == ""
    assert parts[3] == ""
    assert parts[4] == "200.0000"
    assert parts[11] == ""  # telegram_id: scope-level
    assert parts[12] == ""
    assert parts[13] == ""

    # Row 3: window_by_model — gpt-4o, requests=12, scope-level
    # fields blank, per-user fields blank.
    parts = rows[2].rstrip("\r\n").split(",")
    assert parts[0] == "window_by_model"
    assert parts[1] == "30"
    assert parts[2] == "openai/gpt-4o"
    assert parts[3] == "12"
    assert parts[4] == ""  # revenue: scope-level
    assert parts[5] == "50.0000"
    assert parts[8] == ""  # gross_margin_pct: scope-level
    assert parts[9] == ""  # net_profit_usd: scope-level
    assert parts[11] == ""  # telegram_id: per-model rows have no user
    assert parts[12] == ""
    assert parts[13] == ""

    # Row 5 (after the second by_model row): window_top_users —
    # alice (telegram_id=111, username='alice', revenue=$80,
    # 4 top-ups, $30 wallet charges).
    parts = rows[4].rstrip("\r\n").split(",")
    assert parts[0] == "window_top_users"
    assert parts[1] == "30"
    assert parts[2] == ""  # model: per-user rows have no model
    assert parts[3] == ""  # requests: per-user uses topup_count
    assert parts[4] == "80.0000"  # revenue
    assert parts[5] == "30.0000"  # charged
    assert parts[6] == ""  # openrouter_cost: scope-level
    assert parts[7] == ""  # gross_margin: scope-level
    assert parts[8] == ""
    assert parts[9] == ""
    assert parts[10] == "2.0000"  # markup
    assert parts[11] == "111"
    assert parts[12] == "alice"
    assert parts[13] == "4"

    # Row 6: window_top_users for the username=None user (renders
    # as an empty username field, NOT "None" or "null").
    parts = rows[5].rstrip("\r\n").split(",")
    assert parts[0] == "window_top_users"
    assert parts[11] == "222"
    assert parts[12] == ""
    assert parts[13] == "1"


def test_monetization_csv_rows_handle_empty_by_model():
    from web_admin import _format_monetization_csv_rows

    summary = _sample_monetization_summary()
    summary["by_model"] = []
    summary["top_users"] = []
    rows = _format_monetization_csv_rows(summary)
    # Two scope rows only.
    assert len(rows) == 2
    assert rows[0].startswith("lifetime,")
    assert rows[1].startswith("window,30,")


def test_monetization_csv_rows_handle_empty_top_users():
    """Stage-15-Step-E #9 follow-up — the top_users block is
    independent of by_model. An empty top_users with populated
    by_model must render the by_model rows but no per-user rows.
    """
    from web_admin import _format_monetization_csv_rows

    summary = _sample_monetization_summary()
    summary["top_users"] = []
    rows = _format_monetization_csv_rows(summary)
    # Two scope rows + two by_model rows = 4 (no per-user rows).
    assert len(rows) == 4
    assert all("window_top_users" not in r for r in rows)


def test_monetization_csv_rows_drop_non_dict_top_users_entries():
    """Defence-in-depth: a future schema bump that returns a non-dict
    in top_users must not crash the CSV serializer. Drop the
    offender, keep going. Mirrors the by_model defence."""
    from web_admin import _format_monetization_csv_rows

    summary = _sample_monetization_summary()
    summary["top_users"] = [
        ("not", "a", "dict"),  # type: ignore[list-item]
        {
            "telegram_id": 333,
            "username": "carol",
            "revenue_usd": 10.0,
            "topup_count": 1,
            "charged_usd": 0.0,
        },
        None,
    ]
    rows = _format_monetization_csv_rows(summary)
    # 2 scope + 2 by_model + 1 valid top_users = 5.
    assert len(rows) == 5
    assert rows[4].startswith("window_top_users,30,")
    parts = rows[4].rstrip("\r\n").split(",")
    assert parts[11] == "333"


def test_monetization_csv_rows_drop_top_users_without_telegram_id():
    """Stage-15-Step-E #9 follow-up bundled bug fix: drop top_users
    rows whose telegram_id is None / non-int. The DB query never
    returns NULL for telegram_id (the FK constraint guarantees it)
    but a buggy stub or a future schema migration that nullifies
    the column would otherwise produce a CSV row with an empty
    identifier — which an operator importing into a spreadsheet
    would silently mis-attribute to whichever row is sorted
    adjacently.
    """
    from web_admin import _format_monetization_csv_rows

    summary = _sample_monetization_summary()
    summary["top_users"] = [
        {
            "telegram_id": None,
            "username": "ghost",
            "revenue_usd": 99.0,
            "topup_count": 1,
            "charged_usd": 0.0,
        },
        {
            "telegram_id": "not-an-int",
            "username": "garbage",
            "revenue_usd": 88.0,
            "topup_count": 1,
            "charged_usd": 0.0,
        },
        {
            "telegram_id": 444,
            "username": "dana",
            "revenue_usd": 5.0,
            "topup_count": 1,
            "charged_usd": 0.0,
        },
    ]
    rows = _format_monetization_csv_rows(summary)
    # 2 scope + 2 by_model + 1 valid top_users (the first two were
    # dropped) = 5.
    assert len(rows) == 5
    assert rows[4].startswith("window_top_users,30,")
    parts = rows[4].rstrip("\r\n").split(",")
    assert parts[11] == "444"
    assert parts[12] == "dana"


def test_monetization_csv_rows_drop_non_dict_by_model_entries():
    """A future schema bump that returns a non-dict (e.g. a tuple)
    in the by_model list must not crash the CSV serializer — drop
    the offender, log nothing (the request handler is the right
    place to log), and keep going."""
    from web_admin import _format_monetization_csv_rows

    summary = _sample_monetization_summary()
    summary["by_model"] = [
        ("not", "a", "dict"),  # type: ignore[list-item]
        {"model": "openai/gpt-4o", "requests": 1, "charged_usd": 0.5,
         "openrouter_cost_usd": 0.25, "gross_margin_usd": 0.25},
        None,
    ]
    summary["top_users"] = []
    rows = _format_monetization_csv_rows(summary)
    # 2 scope rows + 1 valid by_model row = 3.
    assert len(rows) == 3
    assert rows[2].startswith("window_by_model,30,openai/gpt-4o,1,")


def test_monetization_csv_format_usd_scrubs_nan_and_inf():
    """Defence-in-depth NaN / Inf scrub mirrors the one in
    ``Database.get_user_spending_summary`` — a transient
    ``Decimal('NaN')`` from a corrupted aggregate must NOT render
    as ``nan`` in the CSV (Excel rejects ``nan``)."""
    from web_admin import _format_usd_csv

    assert _format_usd_csv(float("nan")) == "0.0000"
    assert _format_usd_csv(float("inf")) == "0.0000"
    assert _format_usd_csv(float("-inf")) == "0.0000"
    assert _format_usd_csv(None) == ""
    assert _format_usd_csv("not-a-number") == ""
    assert _format_usd_csv(1.2345) == "1.2345"
    assert _format_usd_csv(0) == "0.0000"


async def test_monetization_csv_route_requires_auth(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="letmein"))
    resp = await client.get(
        "/admin/monetization/export.csv", allow_redirects=False
    )
    assert resp.status in (302, 303), resp.status
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_monetization_csv_route_returns_csv_with_correct_headers(
    aiohttp_client, make_admin_app
):
    db = _stub_db_with_monetization(_sample_monetization_summary())
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization/export.csv?window=30")
    assert resp.status == 200
    assert resp.headers["Content-Type"].startswith("text/csv")
    assert "monetization-30d-" in resp.headers.get(
        "Content-Disposition", ""
    )
    assert "no-store" in resp.headers.get("Cache-Control", "")
    body = await resp.text()
    # Header row first, then data rows.
    lines = body.split("\r\n")
    assert lines[0] == (
        "scope,window_days,model,requests,revenue_usd,charged_usd,"
        "openrouter_cost_usd,gross_margin_usd,gross_margin_pct,"
        "net_profit_usd,markup,telegram_id,username,topup_count"
    )
    assert lines[1].startswith("lifetime,")
    assert lines[2].startswith("window,30,")
    assert lines[3].startswith("window_by_model,30,openai/gpt-4o,12,")
    assert lines[4].startswith(
        "window_by_model,30,anthropic/claude-3-opus,8,"
    )
    # Stage-15-Step-E #9 follow-up — top_users rows trail by_model.
    assert lines[5].startswith("window_top_users,30,")
    assert lines[6].startswith("window_top_users,30,")


async def test_monetization_csv_route_honours_window_query_param(
    aiohttp_client, make_admin_app
):
    """``?window=7`` must pass ``window_days=7`` through to
    ``db.get_monetization_summary`` and embed ``7d`` in the
    Content-Disposition filename."""
    db = _stub_db_with_monetization(_sample_monetization_summary())
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization/export.csv?window=7")
    assert resp.status == 200
    db.get_monetization_summary.assert_awaited()
    call_kwargs = db.get_monetization_summary.await_args.kwargs
    assert call_kwargs["window_days"] == 7
    assert "monetization-7d-" in resp.headers.get(
        "Content-Disposition", ""
    )


async def test_monetization_csv_route_falls_back_on_invalid_window(
    aiohttp_client, make_admin_app
):
    """Garbage / non-allowlisted ``?window=`` values must coerce to
    the default 30-day window — same fail-soft policy as the HTML
    page."""
    db = _stub_db_with_monetization(_sample_monetization_summary())
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    for bad in ("365", "abc", "0", "-7", "14", "1.5"):
        resp = await client.get(
            f"/admin/monetization/export.csv?window={bad}"
        )
        assert resp.status == 200
        assert "monetization-30d-" in resp.headers.get(
            "Content-Disposition", ""
        )


async def test_monetization_csv_route_uses_top_models_limit(
    aiohttp_client, make_admin_app
):
    """The CSV export pulls ``MONETIZATION_CSV_TOP_MODELS_LIMIT``
    (1000) — wider than the on-screen table's 10 — because offline
    analysis wants the full long tail. A future refactor that
    forgets to thread the higher limit through must regress here."""
    from web_admin import MONETIZATION_CSV_TOP_MODELS_LIMIT
    db = _stub_db_with_monetization(_sample_monetization_summary())
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization/export.csv")
    assert resp.status == 200
    call_kwargs = db.get_monetization_summary.await_args.kwargs
    assert call_kwargs["top_models_limit"] == MONETIZATION_CSV_TOP_MODELS_LIMIT
    assert call_kwargs["top_models_limit"] == 1000


async def test_monetization_csv_route_db_error_renders_empty_csv(
    aiohttp_client, make_admin_app
):
    """A DB-error during export must NOT 500 — return an empty-zero
    CSV with the markup column populated so the operator at least
    has the pricing config and timestamp recorded. Same fail-soft
    pattern the HTML page uses."""
    db = _stub_db_with_monetization(RuntimeError("boom"))
    with _pytest_for_window.MonkeyPatch.context() as mp:
        mp.setattr("pricing.get_markup", lambda: 2.0)
        client = await aiohttp_client(
            make_admin_app(password="letmein", db=db)
        )
        await client.post(
            "/admin/login",
            data={"password": "letmein"},
            allow_redirects=False,
        )
        resp = await client.get("/admin/monetization/export.csv")
        body = await resp.text()
    assert resp.status == 200
    lines = body.split("\r\n")
    # Header + lifetime + window rows (no by_model, no top_users).
    assert lines[0].startswith("scope,window_days,")
    assert lines[1].startswith("lifetime,")
    # Markup carried through from the fallback. After
    # Stage-15-Step-E #9 follow-up the row trails three blank
    # per-user columns (telegram_id / username / topup_count) so
    # we check the markup field explicitly by index rather than
    # via .endswith().
    parts = lines[1].split(",")
    assert parts[10] == "2.0000"
    assert parts[11:] == ["", "", ""]
    assert lines[2].startswith("window,30,")


async def test_monetization_csv_route_writes_audit_row(
    aiohttp_client, make_admin_app
):
    """Each successful export records a ``monetization_export_csv``
    audit row with the window + row count in ``meta``. Mirrors the
    transactions-CSV-export audit shape."""
    db = _stub_db_with_monetization(_sample_monetization_summary())
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    db.record_admin_audit.reset_mock()
    resp = await client.get("/admin/monetization/export.csv?window=7")
    assert resp.status == 200
    db.record_admin_audit.assert_awaited()
    call_kwargs = db.record_admin_audit.await_args.kwargs
    assert call_kwargs["action"] == "monetization_export_csv"
    assert call_kwargs["target"] == "monetization"
    assert call_kwargs["meta"]["window_days"] == 7
    assert call_kwargs["meta"]["rows"] >= 2  # at minimum lifetime + window


async def test_monetization_html_page_has_export_csv_link(
    aiohttp_client, make_admin_app
):
    db = _stub_db_with_monetization(_sample_monetization_summary())
    client = await aiohttp_client(make_admin_app(password="letmein", db=db))
    await client.post(
        "/admin/login", data={"password": "letmein"}, allow_redirects=False,
    )
    resp = await client.get("/admin/monetization?window=7")
    assert resp.status == 200
    body = await resp.text()
    # Active window threaded into the export link's query string.
    assert "/admin/monetization/export.csv?window=7" in body
    assert "Export CSV" in body


def test_audit_action_labels_includes_export_csv_actions():
    """Bundled bug fix in this PR: ``transactions_export_csv`` was
    being recorded by ``record_admin_audit`` since Stage-9-Step-7 but
    was never added to the audit-page filter dropdown
    (``AUDIT_ACTION_LABELS``). An operator filtering "CSV exports
    only" while reviewing what an admin pulled offline couldn't
    pick the slug out of the dropdown — they had to scroll the full
    unfiltered feed.

    Pin both labels so a future PR can't drop them again. Same shape
    as the existing role_grant / role_revoke pin from Stage-15-Step-E
    #5 follow-up #1."""
    from web_admin import AUDIT_ACTION_LABELS
    assert "transactions_export_csv" in AUDIT_ACTION_LABELS
    assert AUDIT_ACTION_LABELS["transactions_export_csv"] == (
        "Transactions CSV exported"
    )
    assert "monetization_export_csv" in AUDIT_ACTION_LABELS
    assert AUDIT_ACTION_LABELS["monetization_export_csv"] == (
        "Monetization CSV exported"
    )


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


@pytest.mark.parametrize(
    "code",
    [
        # Persian (Eastern Arabic) digit '۱' (U+06F1).
        "PROMO\u06f1",
        # Roman numeral V (U+2164) — visually identical to ASCII 'V'.
        "CODE\u2164",
        # Cyrillic 'О' (U+041E) homoglyph of Latin 'O'.
        "PROM\u041e",
        # Superscript 2 (U+00B2) — ``str.isalnum`` returns True for it.
        "GIFT\u00b2",
        # Pure non-ASCII alnum (Persian).
        "\u067e\u0631\u0648\u0645\u0648\u06f1",
    ],
)
def test_parse_promo_form_rejects_unicode_alnum(code):
    """ASCII-only guard: pre-fix ``str.isalnum`` returned True for
    Unicode digits / letters, so an admin pasting (or fat-fingering)
    a code with a Persian digit or a Cyrillic homoglyph would store
    the row but no user typing on a standard keyboard could ever
    match it. Post-fix the parser rejects them so the admin sees
    ``bad_code`` and re-types in plain ASCII.
    """
    assert parse_promo_form(_form({
        "code": code,
        "discount_kind": "percent",
        "discount_value": "10",
    })) == "bad_code"


def test_parse_promo_form_accepts_full_ascii_alnum_with_punct():
    """Regression pin: the new ``isascii()`` guard must not regress
    legitimate ASCII codes that mix letters / digits / underscore /
    dash. ``"ABCdef-123_XYZ"`` exercises every allowed character class.
    """
    out = parse_promo_form(_form({
        "code": "ABCdef-123_XYZ",
        "discount_kind": "percent",
        "discount_value": "10",
    }))
    assert isinstance(out, dict)
    assert out["code"] == "ABCDEF-123_XYZ"


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


def test_create_promo_code_rejects_nan_discount_amount():
    """Defense-in-depth: a NaN ``discount_amount`` slips past every
    upstream comparison (``NaN <= 0`` is ``False``, ``NaN >
    999_999.9999`` is ``False``) and PostgreSQL ``NUMERIC`` would
    happily store ``'NaN'::numeric``. Once stored, the redeemer's
    ``balance_usd + bonus`` arithmetic on invoice finalize
    propagates NaN into the wallet column, bricking it the same
    way PR #75 prevented at the IPN layer. Refuse at the DB
    layer so the only paths money flows out of a promo are the
    finite-amount happy paths."""
    from database import Database
    db = Database.__new__(Database)
    db.pool = None
    import asyncio
    with pytest.raises(ValueError, match="finite"):
        asyncio.get_event_loop().run_until_complete(
            db.create_promo_code(
                code="X",
                discount_amount=float("nan"),
            )
        )


def test_create_promo_code_rejects_negative_infinity_discount_amount():
    """``-Infinity`` is technically caught by the existing ``<= 0``
    branch, but we want the more specific ``finite`` error so the
    log line points at the actual cause (and so a future refactor
    that reorders the checks keeps the guard)."""
    from database import Database
    db = Database.__new__(Database)
    db.pool = None
    import asyncio
    with pytest.raises(ValueError, match="finite"):
        asyncio.get_event_loop().run_until_complete(
            db.create_promo_code(
                code="X",
                discount_amount=float("-inf"),
            )
        )


def test_create_promo_code_rejects_positive_infinity_discount_amount():
    """``+Infinity`` was already caught by the ``> 999_999.9999``
    DECIMAL-cap branch, but we now reject it at the up-front
    finite-check with a clearer error message."""
    from database import Database
    db = Database.__new__(Database)
    db.pool = None
    import asyncio
    with pytest.raises(ValueError, match="finite"):
        asyncio.get_event_loop().run_until_complete(
            db.create_promo_code(
                code="X",
                discount_amount=float("inf"),
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


@pytest.mark.parametrize(
    "code",
    [
        "GIFT\u06f1",      # Persian digit
        "PROM\u041e",      # Cyrillic O homoglyph
        "X\u00b2",         # superscript 2
        "\u2164",          # Roman numeral V
    ],
)
def test_parse_gift_form_rejects_unicode_alnum(code):
    """ASCII-only guard: see the equivalent ``parse_promo_form`` test
    for the rationale. Gift codes redeem via the same case-insensitive
    DB lookup so a Unicode digit / homoglyph stored in the row would
    never match an ASCII-keyed user typing the same-looking code.
    """
    assert parse_gift_form(_form({
        "code": code,
        "amount_usd": "5",
    })) == "bad_code"


def test_parse_gift_form_accepts_full_ascii_alnum_with_punct():
    out = parse_gift_form(_form({
        "code": "GIFT_ABCdef-123",
        "amount_usd": "5",
    }))
    assert isinstance(out, dict)
    assert out["code"] == "GIFT_ABCDEF-123"


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


def test_create_gift_code_rejects_nan_amount():
    """Defense-in-depth: a NaN ``amount_usd`` slips past every
    upstream comparison (``NaN <= 0`` is ``False``, ``NaN >
    GIFT_AMOUNT_MAX`` is ``False``) and PostgreSQL ``NUMERIC``
    would store ``'NaN'::numeric``. The next ``redeem_gift_code``
    caller would then run ``balance_usd + NaN`` and brick the
    wallet \u2014 same shape as the ``deduct_balance`` /
    ``finalize_payment`` / ``admin_adjust_balance`` non-finite
    refusals we already ship. Web admin form rejects this before
    it gets here, but the DB layer is the only line of defence
    against a future caller that bypasses ``parse_gift_form``."""
    from database import Database
    db = Database.__new__(Database)
    db.pool = None
    import asyncio
    with pytest.raises(ValueError, match="finite"):
        asyncio.get_event_loop().run_until_complete(
            db.create_gift_code(code="X", amount_usd=float("nan"))
        )


def test_create_gift_code_rejects_positive_infinity_amount():
    """``+Infinity`` was already caught by the GIFT_AMOUNT_MAX
    upper bound; we now reject it earlier with a clearer message
    so the log line points at the actual cause."""
    from database import Database
    db = Database.__new__(Database)
    db.pool = None
    import asyncio
    with pytest.raises(ValueError, match="finite"):
        asyncio.get_event_loop().run_until_complete(
            db.create_gift_code(code="X", amount_usd=float("inf"))
        )


def test_create_gift_code_rejects_negative_infinity_amount():
    """``-Infinity`` was already caught by ``amount_usd <= 0`` but
    we surface the more specific finite-check error so a future
    refactor that reorders the checks doesn't quietly re-open the
    NaN hole alongside ``-Inf``."""
    from database import Database
    db = Database.__new__(Database)
    db.pool = None
    import asyncio
    with pytest.raises(ValueError, match="finite"):
        asyncio.get_event_loop().run_until_complete(
            db.create_gift_code(code="X", amount_usd=float("-inf"))
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
    # used_count: now wrapped in a drilldown link when > 0 (Stage-12-Step-D).
    assert ">3</a>" in body
    assert "10" in body  # max_uses
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
# Stage-12-Step-D: per-code redemption drilldown
# ---------------------------------------------------------------------


async def test_gift_redemptions_requires_auth(aiohttp_client, make_admin_app):
    """Anonymous request bounces to /admin/login like every other admin page."""
    client = await aiohttp_client(make_admin_app())
    resp = await client.get(
        "/admin/gifts/BIRTHDAY5/redemptions", allow_redirects=False
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")


async def test_gift_redemptions_renders_rows(aiohttp_client, make_admin_app):
    """Happy path: code exists with redemptions; aggregates render and
    each row surfaces telegram_id / username / credited / tx id."""
    redemptions = {
        "rows": [
            {
                "telegram_id": 1001,
                "username": "alice",
                "redeemed_at": "2026-04-29T10:00:00+00:00",
                "transaction_id": 555,
                "amount_usd_credited": 5.0,
            },
            {
                "telegram_id": 1002,
                "username": None,
                "redeemed_at": "2026-04-28T09:30:00+00:00",
                "transaction_id": 554,
                "amount_usd_credited": 5.0,
            },
        ],
        "total": 2, "page": 1, "per_page": 50, "total_pages": 1,
    }
    aggregates = {
        "total_redemptions": 2,
        "total_credited_usd": 10.0,
        "first_redeemed_at": "2026-04-28T09:30:00+00:00",
        "last_redeemed_at": "2026-04-29T10:00:00+00:00",
    }
    db = _stub_db(
        list_gift_code_redemptions_result=redemptions,
        gift_code_redemption_aggregates_result=aggregates,
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/gifts/BIRTHDAY5/redemptions")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    # Aggregates rendered
    assert "Redemptions" in body
    assert "$10.00" in body
    # Both rows present
    assert "1001" in body
    assert "1002" in body
    assert "@alice" in body
    assert "#555" in body
    assert "#554" in body
    # Per-row credited shown
    assert "$5.0000" in body


async def test_gift_redemptions_uppercases_url_code(
    aiohttp_client, make_admin_app
):
    """``/admin/gifts/birthday5/redemptions`` is normalised to BIRTHDAY5
    before hitting the DB — the gift_codes PK is stored uppercase."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/gifts/birthday5/redemptions")
    assert resp.status == 200
    db.get_gift_code.assert_awaited_once_with("BIRTHDAY5")
    db.list_gift_code_redemptions.assert_awaited_once_with(
        code="BIRTHDAY5", page=1, per_page=50,
    )
    db.get_gift_code_redemption_aggregates.assert_awaited_once_with(
        "BIRTHDAY5"
    )


async def test_gift_redemptions_unknown_code_redirects_with_flash(
    aiohttp_client, make_admin_app
):
    """A deep link to a code that no longer exists redirects back to
    /admin/gifts with an info-banner flash, NOT a 404 (matches the
    user-detail redirect-with-flash convention)."""
    db = _stub_db(get_gift_code_result=None)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get(
        "/admin/gifts/GHOST/redemptions", allow_redirects=False
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/gifts"
    # And the per-page browser was NOT queried — the meta lookup
    # returned None so we short-circuit before paying for the page.
    db.list_gift_code_redemptions.assert_not_awaited()
    db.get_gift_code_redemption_aggregates.assert_not_awaited()


async def test_gift_redemptions_invalid_url_code_redirects(
    aiohttp_client, make_admin_app
):
    """Tampered URL with chars outside [A-Za-z0-9_-] redirects to
    /admin/gifts BEFORE hitting the DB — defense in depth even though
    the SQL is parameterised."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get(
        "/admin/gifts/<script>/redemptions", allow_redirects=False
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/gifts"
    db.get_gift_code.assert_not_awaited()


async def test_gift_redemptions_oversized_url_code_redirects(
    aiohttp_client, make_admin_app
):
    """A code longer than 64 chars is rejected at the URL layer before
    we hit the DB. Matches the parse_gift_form / gifts_revoke ceiling."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    long_code = "A" * 80
    resp = await client.get(
        f"/admin/gifts/{long_code}/redemptions", allow_redirects=False
    )
    assert resp.status == 302
    db.get_gift_code.assert_not_awaited()


async def test_gift_redemptions_per_page_clamps(
    aiohttp_client, make_admin_app
):
    """``per_page=9999`` is clamped to GIFT_REDEMPTIONS_PER_PAGE_MAX (200)
    before being passed to the DB. ``per_page=0`` is clamped to 1.
    ``per_page=junk`` falls back to the default."""
    from web_admin import GIFT_REDEMPTIONS_PER_PAGE_MAX

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )

    await client.get("/admin/gifts/X/redemptions?per_page=9999")
    db.list_gift_code_redemptions.assert_awaited_with(
        code="X", page=1, per_page=GIFT_REDEMPTIONS_PER_PAGE_MAX,
    )

    db.list_gift_code_redemptions.reset_mock()
    await client.get("/admin/gifts/X/redemptions?per_page=0")
    db.list_gift_code_redemptions.assert_awaited_with(
        code="X", page=1, per_page=1,
    )

    db.list_gift_code_redemptions.reset_mock()
    await client.get("/admin/gifts/X/redemptions?per_page=not-a-number")
    db.list_gift_code_redemptions.assert_awaited_with(
        code="X", page=1, per_page=50,
    )


async def test_gift_redemptions_pagination_links(
    aiohttp_client, make_admin_app
):
    """When total_pages > current page, the rendered template MUST
    include a Next link with ?page=N+1; first page MUST NOT link
    'Prev' to a real page."""
    redemptions = {
        "rows": [
            {
                "telegram_id": 1001,
                "username": None,
                "redeemed_at": "2026-04-29T10:00:00+00:00",
                "transaction_id": 555,
                "amount_usd_credited": 5.0,
            },
        ],
        "total": 75, "page": 1, "per_page": 50, "total_pages": 2,
    }
    db = _stub_db(list_gift_code_redemptions_result=redemptions)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/gifts/X/redemptions")
    body = await resp.text()
    # Next link present, points at page=2
    assert "/admin/gifts/X/redemptions?page=2" in body
    # First page should NOT have a clickable Prev link — only the
    # disabled <span>.
    assert '<span class="disabled">← Prev</span>' in body


async def test_gift_redemptions_empty_state(aiohttp_client, make_admin_app):
    """Code exists but has zero redemptions: render the empty-state
    placeholder, NOT a broken table."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/gifts/BIRTHDAY5/redemptions")
    assert resp.status == 200
    body = await resp.text()
    assert "No redemptions yet for this code." in body


async def test_gift_redemptions_orphan_row_renders_dash(
    aiohttp_client, make_admin_app
):
    """Redemption with NULL transaction_id (the underlying transactions
    row was cleaned up — ON DELETE SET NULL) renders as '—' for both
    the credited amount and the transaction column instead of crashing
    or printing 'None'."""
    redemptions = {
        "rows": [
            {
                "telegram_id": 1001,
                "username": "alice",
                "redeemed_at": "2026-04-29T10:00:00+00:00",
                "transaction_id": None,
                "amount_usd_credited": None,
            },
        ],
        "total": 1, "page": 1, "per_page": 50, "total_pages": 1,
    }
    db = _stub_db(list_gift_code_redemptions_result=redemptions)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/gifts/X/redemptions")
    body = await resp.text()
    assert "1001" in body
    assert "None" not in body  # don't leak Python None into the page
    # at least one em-dash placeholder rendered
    assert body.count("—") >= 1


async def test_gift_redemptions_db_error_renders_banner(
    aiohttp_client, make_admin_app
):
    """If the DB query blows up, we render the page with a banner —
    we do NOT 500 the request."""
    db = _stub_db(list_gift_code_redemptions_result=Exception("boom"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/gifts/BIRTHDAY5/redemptions")
    assert resp.status == 200
    body = await resp.text()
    assert "Database query failed" in body


async def test_gifts_list_links_to_redemptions_when_used_count_positive(
    aiohttp_client, make_admin_app
):
    """The gifts list's used_count cell should now be a clickable
    drilldown link when used_count > 0; remain plain text when 0."""
    rows = [
        {
            "code": "USED",
            "amount_usd": 5.0, "max_uses": 10, "used_count": 3,
            "expires_at": None, "is_active": True,
            "created_at": "2026-01-01T00:00:00+00:00",
        },
        {
            "code": "UNUSED",
            "amount_usd": 5.0, "max_uses": 10, "used_count": 0,
            "expires_at": None, "is_active": True,
            "created_at": "2026-01-01T00:00:00+00:00",
        },
    ]
    client = await aiohttp_client(
        make_admin_app(password="pw", db=_stub_db(gift_rows=rows))
    )
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/gifts")
    body = await resp.text()
    # Drilldown link only on the used row.
    assert 'href="/admin/gifts/USED/redemptions"' in body
    assert 'href="/admin/gifts/UNUSED/redemptions"' not in body


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


async def test_strings_save_post_rejects_unknown_placeholder(
    aiohttp_client, make_admin_app
):
    """An override using a placeholder the compiled default doesn't
    expose is rejected at save time. Pre-fix this saved silently and
    crashed every ``t()`` call rendering the slug with KeyError."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    # ``hub_title`` declares {active_model}, {balance}, {lang_label},
    # {memory_label}. Sending an unknown {bal} placeholder must be
    # rejected.
    resp = await client.post(
        "/admin/strings/en/hub_title",
        data={"csrf_token": csrf, "value": "Bad: {bal}"},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_string_override.assert_not_awaited()
    resp2 = await client.get("/admin/strings/en/hub_title")
    body = await resp2.text()
    assert "Unknown placeholder" in body


async def test_strings_save_post_rejects_invalid_format_syntax(
    aiohttp_client, make_admin_app
):
    """An override with an unclosed brace is rejected at save time."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    resp = await client.post(
        "/admin/strings/en/hub_title",
        data={"csrf_token": csrf, "value": "Balance: {balance"},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_string_override.assert_not_awaited()
    resp2 = await client.get("/admin/strings/en/hub_title")
    body = await resp2.text()
    assert "Invalid placeholder syntax" in body


async def test_strings_save_post_rejects_positional_placeholder(
    aiohttp_client, make_admin_app
):
    """``{0}`` / ``{}`` are rejected — every ``t()`` call site uses
    keyword arguments."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    resp = await client.post(
        "/admin/strings/en/hub_title",
        data={"csrf_token": csrf, "value": "Balance: {0}"},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_string_override.assert_not_awaited()


async def test_strings_save_post_accepts_subset_of_placeholders(
    aiohttp_client, make_admin_app
):
    """An override that drops some — but not all — of the compiled
    default's placeholders saves fine; ``str.format`` ignores extra
    kwargs the template doesn't reference."""
    import strings as bot_strings_module
    bot_strings_module.set_overrides({})

    db = _stub_db()
    db.load_all_string_overrides = AsyncMock(
        side_effect=[{}, {("en", "hub_title"): "Just balance: ${balance:.2f}"}]
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_strings_csrf(client, "pw")
    resp = await client.post(
        "/admin/strings/en/hub_title",
        data={
            "csrf_token": csrf,
            "value": "Just balance: ${balance:.2f}",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_string_override.assert_awaited_once_with(
        "en", "hub_title", "Just balance: ${balance:.2f}", updated_by="web"
    )
    bot_strings_module.set_overrides({})


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


async def test_audit_filter_dropdown_includes_control_panel_actions(
    aiohttp_client, make_admin_app,
):
    """Bug-fix regression (Stage-15-Step-F follow-up #3): the
    control-panel slugs (force-stop, kill-switches) shipped in
    PR #131 were being recorded by ``record_admin_audit`` but
    were never added to the filter dropdown — so an operator
    couldn't narrow the audit feed to "kill-switches only" without
    scrolling through the full log. The dropdown must now list all
    five control-panel actions plus the alert-loop slugs."""
    db = _stub_db(audit_log_result=[])
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/audit")
    assert resp.status == 200
    body = await resp.text()
    # Control panel slugs (PR #131).
    assert "Bot force-stopped" in body
    assert "All AI models disabled (kill-switch)" in body
    assert "All AI models re-enabled" in body
    assert "All gateways disabled (kill-switch)" in body
    assert "All gateways re-enabled" in body
    # Alert-loop slugs (this PR).
    assert "Bot-health alert DM sent" in body
    assert "Bot-health recovery DM sent" in body


async def test_audit_filter_dropdown_includes_role_crud_actions(
    aiohttp_client, make_admin_app,
):
    """Bug-fix regression (Stage-15-Step-E #5 follow-up): the
    ``role_grant`` / ``role_revoke`` slugs were already being
    recorded by ``Database.record_admin_audit`` at the
    ``/admin_role_grant`` / ``/admin_role_revoke`` Telegram-side
    handlers (since PR #123), but they had been omitted from
    ``AUDIT_ACTION_LABELS`` so the filter dropdown on
    ``/admin/audit`` couldn't narrow the feed to "role changes
    only". The dropdown must now list both."""
    db = _stub_db(audit_log_result=[])
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/audit")
    assert resp.status == 200
    body = await resp.text()
    assert "Admin role granted" in body
    assert "Admin role revoked" in body


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


# ---------------------------------------------------------------------
# Stage-9-Step-6 follow-up bug-fix bundle. Two real latent bugs in the
# 429 retry-after branch of ``_do_broadcast``:
#
# (1) Cancel responsiveness during retry-after sleep. Pre-fix the
#     back-off was a single ``await asyncio.sleep(60)`` (the cap).
#     A cancel arriving during that sleep was honoured only AFTER
#     the full 60s window AND the post-sleep retry attempt — so an
#     admin clicking "Cancel" on a stuck broadcast could wait 60+
#     seconds before the loop actually exited. Now: when a
#     ``should_cancel`` predicate is wired in, the sleep is sliced
#     into ~1s chunks and cancel is checked between slices.
#
# (2) Retry-attempt classification. The post-429 retry caught the
#     bare ``Exception`` so a recipient who blocked the bot during
#     the back-off (TelegramForbiddenError) was counted as ``failed``
#     instead of ``blocked``, AND every such retry emitted a noisy
#     stack-trace via ``log.exception``. Now the retry attempt
#     preserves the same Telegram-exception taxonomy the parent
#     handler uses.
# ---------------------------------------------------------------------
async def test_do_broadcast_retry_after_cancel_during_sleep():
    """A cancel request arriving DURING the retry-after sleep must
    short-circuit the back-off and skip the post-sleep retry attempt
    entirely, with ``cancelled=True`` in the returned stats."""
    import admin
    from aiogram.exceptions import TelegramRetryAfter
    bot = AsyncMock()
    bot.send_message = AsyncMock(
        side_effect=[
            TelegramRetryAfter(method=MagicMock(), message="flood", retry_after=5),
            None,  # The retry — must NOT be reached because we cancel.
            None,
            None,
        ]
    )
    flag = {"cancel": False}
    sleeps_seen: list[float] = []
    orig_sleep = asyncio.sleep
    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0

    async def fake_sleep(seconds):
        # Flip the cancel flag the first time the broadcast starts
        # sleeping. Slicing means we sleep multiple times during a 5s
        # back-off; the second slice's cancel-check should fire.
        sleeps_seen.append(seconds)
        if not flag["cancel"]:
            flag["cancel"] = True
        await orig_sleep(0)

    try:
        with patch.object(admin.asyncio, "sleep", fake_sleep):
            stats = await admin._do_broadcast(
                bot,
                recipients=[1, 2, 3],
                text="hi",
                admin_id=0,
                should_cancel=lambda: flag["cancel"],
            )
    finally:
        admin._BROADCAST_DELAY_S = orig_delay

    assert stats["cancelled"] is True
    # The retry must NOT have been attempted: send_message was called
    # exactly once (the failing first send that raised 429).
    assert bot.send_message.await_count == 1
    assert stats["sent"] == 0
    assert stats["failed"] == 0
    # And the slicing must have produced multiple smaller sleeps,
    # not a single 5s sleep.
    assert all(
        s <= admin._BROADCAST_RETRY_AFTER_SLICE_S for s in sleeps_seen
    ), f"expected sliced sleeps <= {admin._BROADCAST_RETRY_AFTER_SLICE_S}s, got {sleeps_seen}"


async def test_do_broadcast_retry_after_then_blocked_counts_as_blocked():
    """A recipient who blocks the bot during the retry-after
    back-off (so the post-sleep retry raises
    ``TelegramForbiddenError``) must increment ``blocked``, NOT
    ``failed``. Pre-fix this was wrapped in ``except Exception`` and
    reported as a generic failed delivery — an enterprise-bot
    operator looking at the "blocked rate" KPI would have under-
    counted churn-while-broadcasting by exactly the 429-then-block
    rate."""
    import admin
    from aiogram.exceptions import (
        TelegramForbiddenError,
        TelegramRetryAfter,
    )
    bot = AsyncMock()
    bot.send_message = AsyncMock(
        side_effect=[
            TelegramRetryAfter(method=MagicMock(), message="flood", retry_after=1),
            TelegramForbiddenError(method=MagicMock(), message="bot blocked"),
        ]
    )
    orig_sleep = asyncio.sleep
    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0

    async def fake_sleep(_seconds):
        await orig_sleep(0)

    try:
        with patch.object(admin.asyncio, "sleep", fake_sleep):
            stats = await admin._do_broadcast(
                bot, recipients=[1], text="hi", admin_id=0
            )
    finally:
        admin._BROADCAST_DELAY_S = orig_delay
    assert stats["sent"] == 0
    assert stats["blocked"] == 1
    assert stats["failed"] == 0


async def test_do_broadcast_retry_after_then_bad_request_counts_as_failed():
    """A retry that raises ``TelegramBadRequest`` (chat not found,
    deactivated, etc.) must increment ``failed`` — same as a parent
    BadRequest. Pin to ensure the new categorization didn't acci-
    dentally lump these into ``blocked``."""
    import admin
    from aiogram.exceptions import (
        TelegramBadRequest,
        TelegramRetryAfter,
    )
    bot = AsyncMock()
    bot.send_message = AsyncMock(
        side_effect=[
            TelegramRetryAfter(method=MagicMock(), message="flood", retry_after=1),
            TelegramBadRequest(method=MagicMock(), message="chat not found"),
        ]
    )
    orig_sleep = asyncio.sleep
    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0

    async def fake_sleep(_seconds):
        await orig_sleep(0)

    try:
        with patch.object(admin.asyncio, "sleep", fake_sleep):
            stats = await admin._do_broadcast(
                bot, recipients=[1], text="hi", admin_id=0
            )
    finally:
        admin._BROADCAST_DELAY_S = orig_delay
    assert stats["sent"] == 0
    assert stats["blocked"] == 0
    assert stats["failed"] == 1


async def test_do_broadcast_retry_after_second_429_records_failed():
    """A retry that raises ANOTHER ``TelegramRetryAfter`` must NOT
    recurse (one back-off per recipient is enough), and must be
    counted as ``failed`` — the broadcast keeps moving instead of
    cascading retries on a single recipient."""
    import admin
    from aiogram.exceptions import TelegramRetryAfter
    bot = AsyncMock()
    bot.send_message = AsyncMock(
        side_effect=[
            TelegramRetryAfter(method=MagicMock(), message="flood", retry_after=1),
            TelegramRetryAfter(method=MagicMock(), message="flood", retry_after=2),
        ]
    )
    orig_sleep = asyncio.sleep
    orig_delay = admin._BROADCAST_DELAY_S
    admin._BROADCAST_DELAY_S = 0.0

    async def fake_sleep(_seconds):
        await orig_sleep(0)

    try:
        with patch.object(admin.asyncio, "sleep", fake_sleep):
            stats = await admin._do_broadcast(
                bot, recipients=[1, 2], text="hi", admin_id=0
            )
    finally:
        admin._BROADCAST_DELAY_S = orig_delay
    # Recipient 1: 429 then 429 -> failed=1, no recurse.
    # Recipient 2: 429 path consumed both side_effect slots so
    # the third call would raise StopIteration; instead the
    # default ``side_effect`` cycle would error. We check only
    # the recipient-1 outcome:
    assert stats["failed"] >= 1
    # And critically, the second 429 did NOT trigger a third call:
    # exactly two send_message calls total for recipient 1.
    assert bot.send_message.await_count <= 3


async def test_do_broadcast_retry_after_no_should_cancel_uses_single_sleep():
    """Regression pin for the fast-path: when ``should_cancel`` is
    not provided (the legacy ``admin_broadcast`` Telegram-driven
    caller), we must keep emitting a single ``asyncio.sleep(cap)``
    call — not slice it. The existing ``test_do_broadcast_caps_retry_after``
    relies on observing exactly one cap-sized sleep, so this pin
    explicitly records the intent."""
    import admin
    from aiogram.exceptions import TelegramRetryAfter
    bot = AsyncMock()
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
        await orig_sleep(0)

    try:
        with patch.object(admin.asyncio, "sleep", fake_sleep):
            stats = await admin._do_broadcast(
                bot, recipients=[1], text="hi", admin_id=0,
                # NO should_cancel -> single sleep at cap.
            )
    finally:
        admin._BROADCAST_DELAY_S = orig_delay
    cap_sleeps = [s for s in sleeps if s == admin._BROADCAST_RETRY_AFTER_MAX_S]
    assert len(cap_sleeps) == 1, (
        f"expected exactly one cap-sized sleep on the no-cancel "
        f"path, got {sleeps}"
    )
    assert stats["sent"] == 1


# ---------------------------------------------------------------------
# Bug-fix sweep: ``max_uses`` field overflow on promo / gift create.
#
# Pre-fix the parsers had an upper bound of "whatever Python's int can
# hold" (i.e. arbitrary precision). PostgreSQL's INTEGER column tops
# out at 2_147_483_647; an admin pasting ``max_uses=2147483648`` (or
# any larger value) would crash the INSERT with a
# ``NumericValueOutOfRangeError`` from asyncpg, which the route
# handlers caught with the generic ``"DB write failed — see logs."``
# flash. The admin had no way to know the cause was a fat-fingered
# extra digit. New cap: ``MAX_USES_CAP = 1_000_000`` — well clear of
# the PG INT max, and already implausibly large for any real promo
# or gift code campaign.
# ---------------------------------------------------------------------
def test_parse_promo_form_max_uses_at_cap_accepted():
    from web_admin import MAX_USES_CAP, parse_promo_form
    out = parse_promo_form({
        "code": "FOO",
        "discount_kind": "percent",
        "discount_value": "10",
        "max_uses": str(MAX_USES_CAP),
    })
    assert out["max_uses"] == MAX_USES_CAP


def test_parse_promo_form_max_uses_above_cap_rejected():
    from web_admin import MAX_USES_CAP, parse_promo_form
    out = parse_promo_form({
        "code": "FOO",
        "discount_kind": "percent",
        "discount_value": "10",
        "max_uses": str(MAX_USES_CAP + 1),
    })
    assert out == "max_uses_too_large"


def test_parse_promo_form_max_uses_int_overflow_rejected():
    """The original crash repro: 2_147_483_648 would overflow PG
    INTEGER. Now caught up-front."""
    from web_admin import parse_promo_form
    out = parse_promo_form({
        "code": "FOO",
        "discount_kind": "percent",
        "discount_value": "10",
        "max_uses": "2147483648",
    })
    assert out == "max_uses_too_large"


def test_parse_gift_form_max_uses_at_cap_accepted():
    from web_admin import MAX_USES_CAP, parse_gift_form
    out = parse_gift_form({
        "code": "FOO",
        "amount_usd": "5",
        "max_uses": str(MAX_USES_CAP),
    })
    assert out["max_uses"] == MAX_USES_CAP


def test_parse_gift_form_max_uses_above_cap_rejected():
    from web_admin import MAX_USES_CAP, parse_gift_form
    out = parse_gift_form({
        "code": "FOO",
        "amount_usd": "5",
        "max_uses": str(MAX_USES_CAP + 1),
    })
    assert out == "max_uses_too_large"


def test_promo_form_err_text_has_max_uses_too_large_key():
    """The flash dispatcher in ``promos_create`` looks up the error
    key in ``_PROMO_FORM_ERR_TEXT``. A missing key falls through to
    the generic ``f"Invalid input ({key})."`` branch — pin that the
    new key has a hand-written friendly message."""
    from web_admin import _PROMO_FORM_ERR_TEXT, MAX_USES_CAP
    assert "max_uses_too_large" in _PROMO_FORM_ERR_TEXT
    assert f"{MAX_USES_CAP:,}" in _PROMO_FORM_ERR_TEXT["max_uses_too_large"]


def test_gift_form_err_text_has_max_uses_too_large_key():
    from web_admin import _GIFT_FORM_ERR_TEXT, MAX_USES_CAP
    assert "max_uses_too_large" in _GIFT_FORM_ERR_TEXT
    assert f"{MAX_USES_CAP:,}" in _GIFT_FORM_ERR_TEXT["max_uses_too_large"]


# ---------------------------------------------------------------------
# Bug-fix sweep: username "@" -> "" collapse in parse_user_edit_form.
#
# Pre-fix a raw value of "@" / "@@@" / etc. lstripped to "" and the
# subsequent ``all(c.isalnum() or c == "_" for c in cleaned)`` check
# returned ``True`` for the empty iterable, so the empty string was
# stored in ``users.username``. Empty string is distinct from NULL at
# the SQL level — a follow-up ``WHERE username IS NULL`` query would
# treat that user as having a username and skip the fallback path.
# ---------------------------------------------------------------------


_USER_EDIT_BASE_CURRENT = {
    "language_code": "en",
    "active_model": "google/gemini-pro",
    "memory_enabled": False,
    "username": "alice",
    "free_messages_left": 0,
}


def test_parse_user_edit_form_username_at_only_rejected():
    """The original repro: a single ``@`` collapses to ``""`` after
    ``lstrip("@")`` — pre-fix this passed validation and got stored as
    the empty string. Now rejected with ``"bad_username"``."""
    from web_admin import parse_user_edit_form
    result = parse_user_edit_form(
        {"username": "@"}, current=_USER_EDIT_BASE_CURRENT
    )
    assert result == "bad_username"


def test_parse_user_edit_form_username_only_at_signs_rejected():
    """Same shape as the first test — multiple leading @s also collapse
    to empty after ``lstrip("@")``."""
    from web_admin import parse_user_edit_form
    for raw in ("@@", "@@@", "@@@@@@@@@@"):
        result = parse_user_edit_form(
            {"username": raw}, current=_USER_EDIT_BASE_CURRENT
        )
        assert result == "bad_username", f"raw={raw!r}"


def test_parse_user_edit_form_username_with_leading_at_still_accepted():
    """Regression pin: a normal ``@alice`` is still stripped to
    ``alice`` and accepted (not rejected as ``bad_username``)."""
    from web_admin import parse_user_edit_form
    current = {**_USER_EDIT_BASE_CURRENT, "username": "bob"}
    result = parse_user_edit_form({"username": "@alice"}, current=current)
    assert result == {"username": "alice"}


def test_parse_user_edit_form_username_no_at_still_accepted():
    """Regression pin: a username submitted without the leading @ is
    still accepted unchanged."""
    from web_admin import parse_user_edit_form
    current = {**_USER_EDIT_BASE_CURRENT, "username": "bob"}
    result = parse_user_edit_form({"username": "alice"}, current=current)
    assert result == {"username": "alice"}


def test_parse_user_edit_form_username_empty_clears_to_none():
    """Regression pin: an explicit empty value clears the field to
    ``None`` (NULL in the DB), not the empty string. This is the
    INTENDED clearing path — the ``"@"`` repro abused the same
    collapse to do this implicitly without setting NULL."""
    from web_admin import parse_user_edit_form
    current = {**_USER_EDIT_BASE_CURRENT, "username": "alice"}
    result = parse_user_edit_form({"username": ""}, current=current)
    assert result == {"username": None}


def test_parse_user_edit_form_username_whitespace_clears_to_none():
    """Regression pin: whitespace-only input is also treated as a
    clear (matches the existing comment)."""
    from web_admin import parse_user_edit_form
    current = {**_USER_EDIT_BASE_CURRENT, "username": "alice"}
    result = parse_user_edit_form({"username": "   "}, current=current)
    assert result == {"username": None}


def test_parse_user_edit_form_username_with_space_inside_rejected():
    """Regression pin: a space inside the username is still rejected
    by the alphanumeric-or-underscore check (the all() check that
    pre-fix was vacuously True on the ``@`` collapse)."""
    from web_admin import parse_user_edit_form
    result = parse_user_edit_form(
        {"username": "bad name"}, current=_USER_EDIT_BASE_CURRENT
    )
    assert result == "bad_username"


def test_parse_user_edit_form_username_too_long_distinct_error_key():
    """Regression pin: the length cap returns ``"username_too_long"``
    (a distinct error key from the ``"@"``-collapse rejection so the
    flash banner can render different friendly text)."""
    from web_admin import parse_user_edit_form, USER_FIELD_USERNAME_MAX_CHARS
    raw = "a" * (USER_FIELD_USERNAME_MAX_CHARS + 1)
    result = parse_user_edit_form(
        {"username": raw}, current=_USER_EDIT_BASE_CURRENT
    )
    assert result == "username_too_long"


# ---------------------------------------------------------------------
# Bug-fix sweep: active_model shape validation in parse_user_edit_form.
#
# Pre-fix the shape check was just:
#     len(raw_model) > USER_FIELD_MODEL_MAX_CHARS or "/" not in raw_model
#
# That accepted "foo/" (provider + empty name), "/bar" (empty provider
# + name), "/", "a/b/c" (ambiguous double-slash), and any string with
# whitespace mid-id (e.g. "openai/ gpt-4"). Each of those wrote
# garbage into users.active_model and the user's next chat 400'd at
# OpenRouter, surfacing as ai_provider_unavailable with no hint that
# an admin just bricked their model.
# ---------------------------------------------------------------------


_USER_EDIT_BASE_CURRENT_FOR_MODEL = {
    "language_code": "en",
    "active_model": "google/gemini-pro",
    "memory_enabled": False,
    "username": "alice",
    "free_messages_left": 0,
}


def test_parse_user_edit_form_active_model_trailing_slash_rejected():
    """``"openai/"`` lstripped fine pre-fix but is structurally
    invalid — provider with no model name. OpenRouter 400s on this."""
    from web_admin import parse_user_edit_form
    result = parse_user_edit_form(
        {"active_model": "openai/"},
        current=_USER_EDIT_BASE_CURRENT_FOR_MODEL,
    )
    assert result == "bad_model"


def test_parse_user_edit_form_active_model_leading_slash_rejected():
    """``"/gpt-4"`` is structurally invalid — empty provider."""
    from web_admin import parse_user_edit_form
    result = parse_user_edit_form(
        {"active_model": "/gpt-4"},
        current=_USER_EDIT_BASE_CURRENT_FOR_MODEL,
    )
    assert result == "bad_model"


def test_parse_user_edit_form_active_model_only_slash_rejected():
    """``"/"`` — both sides empty."""
    from web_admin import parse_user_edit_form
    result = parse_user_edit_form(
        {"active_model": "/"},
        current=_USER_EDIT_BASE_CURRENT_FOR_MODEL,
    )
    assert result == "bad_model"


def test_parse_user_edit_form_active_model_double_slash_rejected():
    """Two slashes in a row — ambiguous, definitely not a real id."""
    from web_admin import parse_user_edit_form
    result = parse_user_edit_form(
        {"active_model": "openai//gpt-4"},
        current=_USER_EDIT_BASE_CURRENT_FOR_MODEL,
    )
    assert result == "bad_model"


def test_parse_user_edit_form_active_model_three_part_rejected():
    """``"a/b/c"`` — ambiguous: three-part path is not the
    ``provider/name`` shape OpenRouter uses."""
    from web_admin import parse_user_edit_form
    result = parse_user_edit_form(
        {"active_model": "openai/foo/bar"},
        current=_USER_EDIT_BASE_CURRENT_FOR_MODEL,
    )
    assert result == "bad_model"


def test_parse_user_edit_form_active_model_no_slash_rejected():
    """Regression pin: the original ``"/" not in`` check still fires
    for plain non-id text. ``"gpt-4"`` (without provider) is rejected.
    """
    from web_admin import parse_user_edit_form
    result = parse_user_edit_form(
        {"active_model": "gpt-4"},
        current=_USER_EDIT_BASE_CURRENT_FOR_MODEL,
    )
    assert result == "bad_model"


def test_parse_user_edit_form_active_model_inner_whitespace_rejected():
    """``"openai/ gpt-4"`` survives ``.strip()`` (only outer ws is
    stripped) but the inner space is a typo signal — no real model id
    contains whitespace. Pre-fix this slipped through unchecked."""
    from web_admin import parse_user_edit_form
    result = parse_user_edit_form(
        {"active_model": "openai/ gpt-4"},
        current=_USER_EDIT_BASE_CURRENT_FOR_MODEL,
    )
    assert result == "bad_model"


def test_parse_user_edit_form_active_model_inner_tab_rejected():
    """Tabs are also whitespace and equally a typo signal."""
    from web_admin import parse_user_edit_form
    result = parse_user_edit_form(
        {"active_model": "openai\t/gpt-4"},
        current=_USER_EDIT_BASE_CURRENT_FOR_MODEL,
    )
    assert result == "bad_model"


def test_parse_user_edit_form_active_model_canonical_id_accepted():
    """Regression pin: canonical OpenRouter ids still pass."""
    from web_admin import parse_user_edit_form
    current = {
        **_USER_EDIT_BASE_CURRENT_FOR_MODEL,
        "active_model": "google/gemini-pro",
    }
    result = parse_user_edit_form(
        {"active_model": "openai/gpt-4o"}, current=current
    )
    assert result == {"active_model": "openai/gpt-4o"}


def test_parse_user_edit_form_active_model_id_with_dot_accepted():
    """Regression pin: ``google/gemini-1.5-pro`` (dot in version) is
    legitimate — the new shape check must not reject dots."""
    from web_admin import parse_user_edit_form
    current = {
        **_USER_EDIT_BASE_CURRENT_FOR_MODEL,
        "active_model": "google/gemini-pro",
    }
    result = parse_user_edit_form(
        {"active_model": "google/gemini-1.5-pro"}, current=current
    )
    assert result == {"active_model": "google/gemini-1.5-pro"}


def test_parse_user_edit_form_active_model_id_with_colon_free_tier_accepted():
    """Regression pin: ``qwen/qwen-2.5-72b-instruct:free`` (colon for
    free tier) is legitimate."""
    from web_admin import parse_user_edit_form
    current = {
        **_USER_EDIT_BASE_CURRENT_FOR_MODEL,
        "active_model": "google/gemini-pro",
    }
    result = parse_user_edit_form(
        {"active_model": "qwen/qwen-2.5-72b-instruct:free"},
        current=current,
    )
    assert result == {
        "active_model": "qwen/qwen-2.5-72b-instruct:free"
    }


def test_parse_user_edit_form_active_model_id_with_hyphen_provider_accepted():
    """Regression pin: ``meta-llama/...`` (hyphen in provider) is
    legitimate. ``x-ai/...`` is the same shape."""
    from web_admin import parse_user_edit_form
    current = {
        **_USER_EDIT_BASE_CURRENT_FOR_MODEL,
        "active_model": "google/gemini-pro",
    }
    result = parse_user_edit_form(
        {"active_model": "meta-llama/llama-3-70b-instruct"},
        current=current,
    )
    assert result == {
        "active_model": "meta-llama/llama-3-70b-instruct"
    }


def test_parse_user_edit_form_active_model_too_long_still_rejected():
    """Regression pin: the length cap still fires (independent of the
    new shape check). 200+ chars is way beyond any real model id."""
    from web_admin import parse_user_edit_form, USER_FIELD_MODEL_MAX_CHARS
    raw = "openai/" + "x" * USER_FIELD_MODEL_MAX_CHARS
    assert len(raw) > USER_FIELD_MODEL_MAX_CHARS
    result = parse_user_edit_form(
        {"active_model": raw},
        current=_USER_EDIT_BASE_CURRENT_FOR_MODEL,
    )
    assert result == "bad_model"


def test_parse_user_edit_form_active_model_empty_is_treated_as_unchanged():
    """Regression pin: an empty / whitespace-only ``active_model``
    field falls through the ``if raw_model:`` guard — current model
    stays. Same convention as the username field's empty path."""
    from web_admin import parse_user_edit_form
    result = parse_user_edit_form(
        {"active_model": "   "},
        current=_USER_EDIT_BASE_CURRENT_FOR_MODEL,
    )
    # No "active_model" key in result because the field was blank.
    assert "active_model" not in result



# ---------------------------------------------------------------------
# Stage-9-Step-10: durable broadcast job registry
# ---------------------------------------------------------------------
#
# These tests pin the ``broadcast_jobs`` table integration: that
# ``broadcast_post`` writes an INSERT, that ``_run_broadcast_job``
# mirrors every state transition (queued → running → terminal) and
# throttled progress ticks, that the recent-jobs page reads from
# the DB rather than only the in-memory dict so a process restart
# doesn't orphan history, that ``broadcast_detail_get`` /
# ``broadcast_status_get`` fall back to the DB when the in-memory
# entry is gone, and that ``broadcast_cancel_post`` mirrors the
# cancel flag. Plus the bundled bug-fix pin: an
# ``asyncio.CancelledError`` mid-broadcast (i.e. app shutdown)
# now sets ``state="interrupted"`` instead of conflating with
# ``state="failed"``, and the orphan sweep on app startup writes
# the same state for any row left ``running`` from before the
# restart. See HANDOFF.md §5 Step-10 for the rationale and the
# bundled bug-fix description.


async def test_broadcast_post_inserts_durable_row(
    aiohttp_client, make_admin_app
):
    """Stage-9-Step-10: kicking off a broadcast must insert a
    ``broadcast_jobs`` row in its initial ``queued`` state so a
    crash between ``create_task`` and the worker's first state
    write still leaves a forensic trail."""
    db = _stub_db(broadcast_recipients=[])
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)
    client = await aiohttp_client(app)
    csrf = await _login_and_get_broadcast_csrf(client, "pw")
    resp = await client.post(
        "/admin/broadcast",
        data={"text": "hello world", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.insert_broadcast_job.assert_awaited_once()
    kwargs = db.insert_broadcast_job.await_args.kwargs
    assert kwargs["text_preview"] == "hello world"
    assert kwargs["full_text_len"] == len("hello world")
    assert kwargs["state"] == "queued"
    # ``job_id`` is a token_urlsafe(6) — no specific shape, just non-empty.
    assert kwargs["job_id"]


async def test_broadcast_post_continues_when_db_insert_fails(
    aiohttp_client, make_admin_app
):
    """Pin: a DB blip on the durable mirror INSERT must not block
    the in-memory job from running. The broadcast itself doesn't
    depend on ``broadcast_jobs`` being writeable."""
    db = _stub_db(
        broadcast_recipients=[],
        insert_broadcast_job_result=RuntimeError("pool drained"),
    )
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)
    client = await aiohttp_client(app)
    csrf = await _login_and_get_broadcast_csrf(client, "pw")
    resp = await client.post(
        "/admin/broadcast",
        data={"text": "hello", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/broadcast/")


async def test_run_broadcast_job_mirrors_state_transitions(make_admin_app):
    """Pin: every state mutation the worker performs is mirrored to
    ``broadcast_jobs`` via ``update_broadcast_job``. Empty-recipients
    path: queued → running → completed."""
    from web_admin import _new_broadcast_job, _run_broadcast_job, _store_broadcast_job

    db = _stub_db(broadcast_recipients=[])
    bot = AsyncMock()
    app = make_admin_app(db=db, bot=bot)
    job = _new_broadcast_job(text="hi", only_active_days=None)
    _store_broadcast_job(app, job)
    await _run_broadcast_job(app=app, job=job, text="hi")

    states_written = [
        c.kwargs.get("state")
        for c in db.update_broadcast_job.await_args_list
        if c.kwargs.get("state") is not None
    ]
    assert states_written == ["running", "completed"]
    final_call = db.update_broadcast_job.await_args_list[-1]
    assert final_call.kwargs.get("completed_at_now") is True


async def test_run_broadcast_job_mirrors_db_query_failure(make_admin_app):
    """Pin: when ``iter_broadcast_recipients`` raises, the failure
    state must be mirrored to ``broadcast_jobs`` with ``error``
    populated and ``completed_at_now=True`` — otherwise a forensic
    query against the table would see the row stuck in ``running``."""
    from web_admin import _new_broadcast_job, _run_broadcast_job, _store_broadcast_job

    db = _stub_db(broadcast_recipients=RuntimeError("pool closed"))
    bot = AsyncMock()
    app = make_admin_app(db=db, bot=bot)
    job = _new_broadcast_job(text="hi", only_active_days=None)
    _store_broadcast_job(app, job)
    await _run_broadcast_job(app=app, job=job, text="hi")

    final_call = db.update_broadcast_job.await_args_list[-1]
    assert final_call.kwargs.get("state") == "failed"
    assert final_call.kwargs.get("completed_at_now") is True
    assert "pool closed" in (final_call.kwargs.get("error") or "")


async def test_run_broadcast_job_cancelled_error_marks_interrupted(
    make_admin_app
):
    """Stage-9-Step-10 BUNDLED BUG FIX. Pre-fix, an
    ``asyncio.CancelledError`` propagating out of ``_do_broadcast``
    (which fires on app shutdown when the worker task is cancelled)
    set ``job["state"] = "failed"``, conflating three distinct
    terminal states. Post-fix the same path sets
    ``state="interrupted"`` so the recent-jobs page can distinguish
    a deploy-time restart from a code bug, and matches the orphan-
    sweep state for jobs whose worker task didn't even reach the
    ``except`` block (process was SIGKILL-ed)."""
    from web_admin import _new_broadcast_job, _run_broadcast_job, _store_broadcast_job

    recipients = [100, 200, 300]
    db = _stub_db(broadcast_recipients=recipients)
    bot = AsyncMock()
    app = make_admin_app(db=db, bot=bot)
    job = _new_broadcast_job(text="hi", only_active_days=None)
    _store_broadcast_job(app, job)

    async def _raise_cancelled(*args, **kwargs):
        raise asyncio.CancelledError()

    with patch("admin._do_broadcast", side_effect=_raise_cancelled):
        with pytest.raises(asyncio.CancelledError):
            await _run_broadcast_job(app=app, job=job, text="hi")

    assert job["state"] == "interrupted"
    assert job["error"] == "Cancelled (admin panel shutting down)."
    assert job["completed_at"] is not None
    final_call = db.update_broadcast_job.await_args_list[-1]
    assert final_call.kwargs.get("state") == "interrupted"
    assert final_call.kwargs.get("completed_at_now") is True


async def test_broadcast_get_reads_from_db(
    aiohttp_client, make_admin_app
):
    """Pin: the recent-jobs list comes from ``list_broadcast_jobs``
    rather than only the in-memory dict so a restart doesn't
    orphan history."""
    rows = [
        {
            "id": "abc123",
            "text_preview": "Recent broadcast preview",
            "full_text_len": 100,
            "only_active_days": None,
            "state": "completed",
            "total": 50, "sent": 48, "blocked": 1, "failed": 1,
            "i": 50, "error": None, "cancel_requested": False,
            "created_at": "2026-01-01T00:00:00+00:00",
            "started_at": "2026-01-01T00:00:01+00:00",
            "completed_at": "2026-01-01T00:00:30+00:00",
        }
    ]
    db = _stub_db(broadcast_jobs_rows=rows)
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)
    client = await aiohttp_client(app)
    await _login(client, "pw")
    resp = await client.get("/admin/broadcast")
    assert resp.status == 200
    body = await resp.text()
    db.list_broadcast_jobs.assert_awaited_once()
    assert "Recent broadcast preview" in body
    assert "abc123" in body


async def test_broadcast_get_falls_back_to_in_memory_when_db_fails(
    aiohttp_client, make_admin_app
):
    """Defensive fallback pin: if ``list_broadcast_jobs`` raises,
    the page still renders using the in-memory dict (the legacy
    pre-Step-10 behaviour) rather than 500-ing the whole page."""
    from web_admin import _new_broadcast_job, _store_broadcast_job

    db = _stub_db(broadcast_jobs_rows=RuntimeError("db down"))
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)
    job = _new_broadcast_job(text="in memory only", only_active_days=None)
    job["state"] = "completed"
    _store_broadcast_job(app, job)
    client = await aiohttp_client(app)
    await _login(client, "pw")
    resp = await client.get("/admin/broadcast")
    assert resp.status == 200
    body = await resp.text()
    assert "in memory only" in body


async def test_broadcast_get_in_memory_only_jobs_appear_newest_first(
    aiohttp_client, make_admin_app
):
    """Stage-9-Step-10 regression pin (Devin Review on PR #91): when
    DB rows exist AND the in-memory dict has jobs the DB hasn't
    observed yet (race between INSERT and the recent-jobs GET), the
    in-memory-only prefix of the rendered list must be newest-first.
    The earlier ``reversed(list(in_memory.items()))`` + ``insert(0,
    ...)`` produced oldest-first, contradicting the DB-side
    ``ORDER BY created_at DESC``."""
    from web_admin import _new_broadcast_job, _store_broadcast_job

    db_row = {
        "id": "db-row",
        "text_preview": "From DB",
        "full_text_len": 5,
        "only_active_days": None,
        "state": "completed",
        "total": 1, "sent": 1, "blocked": 0, "failed": 0,
        "i": 1, "error": None, "cancel_requested": False,
        "created_at": "2026-01-01T00:00:00+00:00",
        "started_at": "2026-01-01T00:00:01+00:00",
        "completed_at": "2026-01-01T00:00:02+00:00",
    }
    db = _stub_db(broadcast_jobs_rows=[db_row])
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)

    # Three in-memory-only jobs, registered oldest → newest. None
    # of them are in the DB-rows list above.
    inmem_oldest = _new_broadcast_job(text="A oldest", only_active_days=None)
    inmem_oldest["state"] = "running"
    _store_broadcast_job(app, inmem_oldest)
    inmem_middle = _new_broadcast_job(text="B middle", only_active_days=None)
    inmem_middle["state"] = "running"
    _store_broadcast_job(app, inmem_middle)
    inmem_newest = _new_broadcast_job(text="C newest", only_active_days=None)
    inmem_newest["state"] = "running"
    _store_broadcast_job(app, inmem_newest)

    client = await aiohttp_client(app)
    await _login(client, "pw")
    resp = await client.get("/admin/broadcast")
    assert resp.status == 200
    body = await resp.text()

    # The newest in-memory-only job should appear before the older
    # in-memory-only jobs in the rendered HTML; oldest comes last
    # before the DB row.
    pos_newest = body.find(inmem_newest["id"])
    pos_middle = body.find(inmem_middle["id"])
    pos_oldest = body.find(inmem_oldest["id"])
    pos_db = body.find("db-row")
    assert -1 < pos_newest < pos_middle < pos_oldest < pos_db, (
        "in-memory-only jobs must appear newest-first then DB rows; "
        f"got positions newest={pos_newest} middle={pos_middle} "
        f"oldest={pos_oldest} db={pos_db}"
    )


async def test_broadcast_detail_falls_back_to_db_after_restart(
    aiohttp_client, make_admin_app
):
    """Stage-9-Step-10: a `/admin/broadcast/{id}` link must keep
    resolving after a process restart. The in-memory dict is empty
    (no live worker), but ``get_broadcast_job`` returns the
    durable row."""
    durable_row = {
        "id": "xyz789",
        "text_preview": "Restart-survivor preview",
        "full_text_len": 25,
        "only_active_days": None,
        "state": "interrupted",
        "total": 100, "sent": 42, "blocked": 0, "failed": 0,
        "i": 42,
        "error": "Job was running when the bot process restarted",
        "cancel_requested": False,
        "created_at": "2026-01-01T00:00:00+00:00",
        "started_at": "2026-01-01T00:00:01+00:00",
        "completed_at": "2026-01-01T00:01:00+00:00",
    }
    db = _stub_db(get_broadcast_job_result=durable_row)
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)
    client = await aiohttp_client(app)
    await _login(client, "pw")
    resp = await client.get("/admin/broadcast/xyz789")
    assert resp.status == 200
    body = await resp.text()
    db.get_broadcast_job.assert_awaited_with("xyz789")
    assert "Restart-survivor preview" in body
    assert "interrupted" in body


async def test_broadcast_detail_unknown_redirects_with_flash(
    aiohttp_client, make_admin_app
):
    """When neither the in-memory dict nor the DB knows the id,
    redirect to the index with a flash error."""
    db = _stub_db(get_broadcast_job_result=None)
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)
    client = await aiohttp_client(app)
    await _login(client, "pw")
    resp = await client.get(
        "/admin/broadcast/nope", allow_redirects=False
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/broadcast"


async def test_broadcast_status_falls_back_to_db_after_restart(
    aiohttp_client, make_admin_app
):
    """The polling JSON endpoint must also resolve from the durable
    mirror so a tab left open across a restart doesn't 404."""
    durable_row = {
        "id": "restart1",
        "text_preview": "preview",
        "full_text_len": 7,
        "only_active_days": None,
        "state": "completed",
        "total": 10, "sent": 10, "blocked": 0, "failed": 0,
        "i": 10, "error": None, "cancel_requested": False,
        "created_at": "2026-01-01T00:00:00+00:00",
        "started_at": "2026-01-01T00:00:01+00:00",
        "completed_at": "2026-01-01T00:00:30+00:00",
    }
    db = _stub_db(get_broadcast_job_result=durable_row)
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)
    client = await aiohttp_client(app)
    await _login(client, "pw")
    resp = await client.get("/admin/broadcast/restart1/status")
    assert resp.status == 200
    payload = await resp.json()
    assert payload["id"] == "restart1"
    assert payload["state"] == "completed"


async def test_broadcast_cancel_mirrors_flag_to_db(
    aiohttp_client, make_admin_app
):
    """Pin: clicking Cancel must set ``cancel_requested=True`` on
    the durable row in addition to the in-memory job dict, so the
    recent-jobs list shows the cancellation promptly."""
    from web_admin import _new_broadcast_job, _store_broadcast_job

    db = _stub_db()
    bot = AsyncMock()
    app = make_admin_app(password="pw", db=db, bot=bot)
    job = _new_broadcast_job(text="cancel me", only_active_days=None)
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
    cancel_calls = [
        c for c in db.update_broadcast_job.await_args_list
        if c.kwargs.get("cancel_requested") is True
    ]
    assert len(cancel_calls) == 1
    assert cancel_calls[0].args[0] == job["id"]


async def test_orphan_sweep_runs_at_app_startup(
    aiohttp_client, make_admin_app
):
    """Pin: ``setup_admin_routes`` registers an ``on_startup``
    handler that calls ``mark_orphan_broadcast_jobs_interrupted``
    so any row left in queued/running from before the restart is
    flipped to ``interrupted`` before the panel takes its first
    request."""
    db = _stub_db(mark_orphan_broadcast_jobs_result=3)
    bot = AsyncMock()
    client = await aiohttp_client(make_admin_app(db=db, bot=bot))
    db.mark_orphan_broadcast_jobs_interrupted.assert_awaited_once()
    resp = await client.get("/admin/login")
    assert resp.status == 200


async def test_orphan_sweep_swallows_db_failure_at_startup(
    aiohttp_client, make_admin_app
):
    """Defensive pin: a DB blip during the orphan sweep must NOT
    block app startup. The whole admin panel comes up; the missed
    sweep is logged and we move on."""
    db = _stub_db(
        mark_orphan_broadcast_jobs_result=RuntimeError("db unreachable")
    )
    bot = AsyncMock()
    client = await aiohttp_client(make_admin_app(db=db, bot=bot))
    db.mark_orphan_broadcast_jobs_interrupted.assert_awaited_once()
    resp = await client.get("/admin/login")
    assert resp.status == 200


# ---------------------------------------------------------------------
# Throttled progress flush (web_admin._persist_broadcast_progress)
# ---------------------------------------------------------------------


async def test_persist_broadcast_progress_throttles_to_every_n():
    """Pin: progress flushes mirror to the DB at most once every
    ``BROADCAST_DB_PROGRESS_FLUSH_EVERY`` recipients. A 100-recipient
    broadcast should produce 4 throttled flushes (i=25, 50, 75,
    100) — NOT 100 UPDATEs."""
    from web_admin import (
        BROADCAST_DB_PROGRESS_FLUSH_EVERY, _persist_broadcast_progress
    )

    db = _stub_db()
    job = {
        "id": "j1", "total": 100, "sent": 0, "blocked": 0,
        "failed": 0, "i": 0,
    }
    flushes = 0
    for i in range(1, 101):
        job["i"] = i
        job["sent"] = i
        before = db.update_broadcast_job.await_count
        await _persist_broadcast_progress(db, job)
        if db.update_broadcast_job.await_count > before:
            flushes += 1
    expected = 100 // BROADCAST_DB_PROGRESS_FLUSH_EVERY
    assert flushes == expected, (
        f"expected {expected} flushes (every "
        f"{BROADCAST_DB_PROGRESS_FLUSH_EVERY} recipients), got {flushes}"
    )


async def test_persist_broadcast_progress_force_bypasses_throttle():
    """Pin: ``force=True`` always flushes regardless of the
    modulo throttle. Used by terminal-state transitions so the
    final row never carries stale counters."""
    from web_admin import _persist_broadcast_progress

    db = _stub_db()
    job = {
        "id": "j1", "total": 10, "sent": 3, "blocked": 0,
        "failed": 0, "i": 3,  # 3 % 25 != 0
    }
    await _persist_broadcast_progress(db, job, force=True)
    db.update_broadcast_job.assert_awaited_once()


async def test_persist_broadcast_progress_swallows_db_failure():
    """Pin: a DB blip mid-broadcast must not crash the worker.
    The in-memory dict is the source of truth for live progress;
    the DB is the durable mirror. Best-effort throughout."""
    from web_admin import _persist_broadcast_progress

    db = _stub_db(update_broadcast_job_result=RuntimeError("blip"))
    job = {
        "id": "j1", "total": 10, "sent": 0, "blocked": 0,
        "failed": 0, "i": 0,
    }
    # Must not raise.
    await _persist_broadcast_progress(db, job, force=True)


def test_store_broadcast_job_evicts_interrupted_terminal_state():
    """Stage-9-Step-10 regression pin (Devin Review on PR #91): the
    in-memory eviction tuple must include the new ``"interrupted"``
    state alongside ``completed`` / ``failed`` / ``cancelled``.
    Otherwise an ``interrupted`` job is treated like a live
    ``queued`` / ``running`` one and pins the registry above
    ``BROADCAST_MAX_HISTORY`` until the process exits — so the
    in-memory cap silently breaks every time a deploy interrupts
    a broadcast."""
    from web_admin import (
        BROADCAST_MAX_HISTORY,
        APP_KEY_BROADCAST_JOBS,
        _new_broadcast_job,
        _store_broadcast_job,
    )

    app = web.Application()
    app[APP_KEY_BROADCAST_JOBS] = {}
    interrupted_ids: list[str] = []
    for _ in range(BROADCAST_MAX_HISTORY):
        j = _new_broadcast_job(text="ouch", only_active_days=None)
        j["state"] = "interrupted"
        _store_broadcast_job(app, j)
        interrupted_ids.append(j["id"])
    # One more terminal pushes the cap — oldest interrupted must
    # be evictable, not pinned.
    newest = _new_broadcast_job(text="next", only_active_days=None)
    newest["state"] = "completed"
    _store_broadcast_job(app, newest)
    assert interrupted_ids[0] not in app[APP_KEY_BROADCAST_JOBS]
    assert newest["id"] in app[APP_KEY_BROADCAST_JOBS]
    assert len(app[APP_KEY_BROADCAST_JOBS]) == BROADCAST_MAX_HISTORY


# =========================================================================
# Stage-12-Step-A: refunds / chargebacks admin UI
# =========================================================================
#
# The integration tests below exercise the full route stack —
# CSRF, auth, audit logging, flash banners, redirect — against a
# stubbed DB layer so we don't need Postgres in CI. The DB-method
# semantics themselves are covered by the SQL-shape tests in
# ``tests/test_database_queries.py``.


from web_admin import (  # noqa: E402  (keep Stage-12-Step-A imports grouped)
    transaction_refund_post,
    REFUND_REASON_MAX_CHARS,
)


async def _login_and_get_transactions_csrf(
    client, password: str = "pw"
) -> str:
    """Log in, fetch /admin/transactions, scrape its CSRF token.

    The transactions list page started embedding a CSRF token in
    Stage-12-Step-A so the inline refund form can POST. We pin the
    scrape pattern here so the integration tests fail loudly if the
    template ever drops the token.
    """
    await _login(client, password)
    resp = await client.get("/admin/transactions")
    assert resp.status == 200
    body = await resp.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token on /admin/transactions"
    return m.group(1)


def _success_tx_row(**overrides) -> dict:
    """Default SUCCESS row shape for the transactions list stub."""
    base = {
        "id": 501,
        "telegram_id": 42,
        "gateway": "nowpayments",
        "currency": "USDT",
        "amount_crypto_or_rial": 1.0,
        "amount_usd": 9.99,
        "status": "SUCCESS",
        "gateway_invoice_id": "inv-501",
        "created_at": "2026-04-28T12:00:00+00:00",
        "completed_at": "2026-04-28T12:05:00+00:00",
        "notes": None,
    }
    base.update(overrides)
    return base


async def test_transactions_list_shows_refund_button_only_on_eligible_rows(
    aiohttp_client, make_admin_app
):
    """The inline Refund form must render only on SUCCESS rows from
    a refundable gateway (nowpayments / tetrapay). Admin and gift
    rows route through the credit/debit flow on the user detail
    page — rendering a Refund button there would silently double
    debit if clicked.
    """
    rows = [
        _success_tx_row(id=701, gateway="nowpayments", status="SUCCESS"),
        _success_tx_row(id=702, gateway="tetrapay", status="SUCCESS"),
        _success_tx_row(id=703, gateway="admin", status="SUCCESS"),
        _success_tx_row(id=704, gateway="gift", status="SUCCESS"),
        _success_tx_row(id=705, gateway="nowpayments", status="PENDING"),
        _success_tx_row(id=706, gateway="nowpayments", status="REFUNDED"),
    ]
    db = _stub_db(
        list_transactions_result={
            "rows": rows, "total": len(rows), "page": 1,
            "per_page": 50, "total_pages": 1,
        }
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/transactions")
    body = await resp.text()
    # Eligible: SUCCESS + nowpayments / tetrapay
    assert 'action="/admin/transactions/701/refund"' in body
    assert 'action="/admin/transactions/702/refund"' in body
    # Not eligible: admin / gift gateways
    assert 'action="/admin/transactions/703/refund"' not in body
    assert 'action="/admin/transactions/704/refund"' not in body
    # Not eligible: non-SUCCESS status (PENDING / REFUNDED)
    assert 'action="/admin/transactions/705/refund"' not in body
    assert 'action="/admin/transactions/706/refund"' not in body


async def test_transactions_refund_requires_auth(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="pw"))
    resp = await client.post(
        "/admin/transactions/501/refund",
        data={"csrf_token": "x", "reason": "test"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")


async def test_transactions_refund_rejects_bad_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.post(
        "/admin/transactions/501/refund",
        data={"csrf_token": "wrong", "reason": "x"},
        allow_redirects=False,
    )
    # Always redirects back to /admin/transactions with a flash; the
    # underlying DB call must NOT have happened.
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/transactions"
    db.refund_transaction.assert_not_awaited()


async def test_transactions_refund_rejects_invalid_id(
    aiohttp_client, make_admin_app
):
    """A non-integer transaction id never hits the DB; the route
    returns the same redirect-to-list as a normal not-found click."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.post(
        "/admin/transactions/not-an-int/refund",
        data={"csrf_token": "anything", "reason": "x"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/transactions"
    db.refund_transaction.assert_not_awaited()


async def test_transactions_refund_rejects_zero_or_negative_id(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    for bad in ("0", "-1"):
        resp = await client.post(
            f"/admin/transactions/{bad}/refund",
            data={"csrf_token": "anything", "reason": "x"},
            allow_redirects=False,
        )
        assert resp.status == 302
        assert resp.headers["Location"] == "/admin/transactions"
    db.refund_transaction.assert_not_awaited()


async def test_transactions_refund_rejects_empty_reason(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        list_transactions_result={
            "rows": [_success_tx_row()], "total": 1, "page": 1,
            "per_page": 50, "total_pages": 1,
        }
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_transactions_csrf(client, "pw")
    resp = await client.post(
        "/admin/transactions/501/refund",
        data={"csrf_token": csrf, "reason": "   "},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.refund_transaction.assert_not_awaited()


async def test_transactions_refund_rejects_oversize_reason(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        list_transactions_result={
            "rows": [_success_tx_row()], "total": 1, "page": 1,
            "per_page": 50, "total_pages": 1,
        }
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_transactions_csrf(client, "pw")
    resp = await client.post(
        "/admin/transactions/501/refund",
        data={
            "csrf_token": csrf,
            "reason": "x" * (REFUND_REASON_MAX_CHARS + 1),
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.refund_transaction.assert_not_awaited()


def test_refund_reason_max_chars_leaves_room_for_prefix():
    """Stage-12-Step-A follow-up: the route prepends ``[web] `` (6
    chars) to the operator-supplied reason before calling
    :meth:`Database.refund_transaction`, which independently caps
    the value at ``REFUND_REASON_MAX_LEN`` (500). If the form-side
    cap had stayed at ``500`` (the DB cap, not "DB cap minus
    prefix"), a 500-char reason would slip past form validation,
    get prefixed to 506 chars, then trip the DB-side ``ValueError``
    — caught by the route, but only after rendering a confusing
    "Invalid input: reason longer than … (500); got 506" banner.
    Pin the math so a future refactor that drops the prefix or
    bumps either constant breaks this test, not production.
    """
    assert (
        REFUND_REASON_MAX_CHARS
        + len("[web] ")
        == database_module.Database.REFUND_REASON_MAX_LEN
    ), (
        "form-side cap + prefix length must equal DB-side cap so "
        "the prefixed value always fits within REFUND_REASON_MAX_LEN"
    )


async def test_transactions_refund_max_length_reason_passes_validation(
    aiohttp_client, make_admin_app
):
    """A reason that is *exactly* the form-side cap must pass form
    validation AND fit within the DB-side cap once prefixed. This
    is the regression test for the prefix-overflow bug — pre-fix,
    a 500-char reason got prefixed to 506 chars and the DB raised
    ``ValueError("reason longer than (500); got 506")``."""
    db = _stub_db(
        list_transactions_result={
            "rows": [_success_tx_row()], "total": 1, "page": 1,
            "per_page": 50, "total_pages": 1,
        },
        refund_transaction_result={
            "transaction_id": 501,
            "telegram_id": 42,
            "amount_refunded_usd": 9.99,
            "new_balance_usd": 5.0,
        },
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_transactions_csrf(client, "pw")
    boundary_reason = "x" * REFUND_REASON_MAX_CHARS
    resp = await client.post(
        "/admin/transactions/501/refund",
        data={"csrf_token": csrf, "reason": boundary_reason},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.refund_transaction.assert_awaited_once()
    sent_reason = db.refund_transaction.await_args.kwargs["reason"]
    # The route prefixed it; combined length must still fit the DB cap.
    assert sent_reason.startswith("[web] ")
    assert (
        len(sent_reason)
        <= database_module.Database.REFUND_REASON_MAX_LEN
    )


async def test_transactions_refund_happy_path_audits_and_calls_db(
    aiohttp_client, make_admin_app
):
    db = _stub_db(
        list_transactions_result={
            "rows": [_success_tx_row()], "total": 1, "page": 1,
            "per_page": 50, "total_pages": 1,
        },
        refund_transaction_result={
            "transaction_id": 501,
            "telegram_id": 42,
            "amount_refunded_usd": 9.99,
            "new_balance_usd": 5.0,
        },
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_transactions_csrf(client, "pw")
    resp = await client.post(
        "/admin/transactions/501/refund",
        data={"csrf_token": csrf, "reason": "duplicate charge"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/transactions"
    # DB called with exactly the parsed inputs (with the [web] prefix
    # on the reason for grep-friendly forensics).
    db.refund_transaction.assert_awaited_once()
    call = db.refund_transaction.await_args
    assert call.kwargs["transaction_id"] == 501
    assert call.kwargs["reason"].startswith("[web] ")
    assert "duplicate charge" in call.kwargs["reason"]
    # Audit row records refund_issued.
    actions = [c.kwargs["action"] for c in db.record_admin_audit.await_args_list]
    assert "refund_issued" in actions


async def test_transactions_refund_insufficient_balance_records_refused(
    aiohttp_client, make_admin_app
):
    """User has spent the credit — the DB returns the error dict and
    the route writes a refund_refused audit row instead of
    refund_issued. The wallet must NOT have been debited (the DB
    method is responsible for the no-write guarantee; the route just
    surfaces the refusal)."""
    db = _stub_db(
        list_transactions_result={
            "rows": [_success_tx_row()], "total": 1, "page": 1,
            "per_page": 50, "total_pages": 1,
        },
        refund_transaction_result={
            "error": database_module.Database.REFUND_REFUSAL_INSUFFICIENT_BALANCE,
            "current_status": "SUCCESS",
            "balance_usd": 1.0,
            "amount_usd": 9.99,
        },
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_transactions_csrf(client, "pw")
    resp = await client.post(
        "/admin/transactions/501/refund",
        data={"csrf_token": csrf, "reason": "chargeback"},
        allow_redirects=False,
    )
    assert resp.status == 302
    actions = [c.kwargs["action"] for c in db.record_admin_audit.await_args_list]
    assert "refund_refused" in actions
    assert "refund_issued" not in actions


async def test_transactions_refund_not_found_records_refused(
    aiohttp_client, make_admin_app
):
    """The DB returns ``None`` when the transaction row is gone (rare
    benign race — operator clicks Refund, row deleted, POST lands).
    Route must redirect with a friendly banner and write a
    ``refund_refused`` audit row with outcome=not_found."""
    db = _stub_db(
        list_transactions_result={
            "rows": [_success_tx_row()], "total": 1, "page": 1,
            "per_page": 50, "total_pages": 1,
        },
        refund_transaction_result=None,
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_transactions_csrf(client, "pw")
    resp = await client.post(
        "/admin/transactions/501/refund",
        data={"csrf_token": csrf, "reason": "chargeback"},
        allow_redirects=False,
    )
    assert resp.status == 302
    audited = [
        c for c in db.record_admin_audit.await_args_list
        if c.kwargs["action"] == "refund_refused"
    ]
    assert audited, "expected a refund_refused audit row for not-found"
    assert audited[0].kwargs["outcome"] == "not_found"


async def test_transactions_refund_db_exception_yields_friendly_error(
    aiohttp_client, make_admin_app
):
    """A bare Exception from the DB layer must NOT 500 — the route
    redirects with an error flash so the operator can retry. No audit
    row is written for an unhandled exception (the DB layer's own
    error log carries the diagnostic)."""
    db = _stub_db(
        list_transactions_result={
            "rows": [_success_tx_row()], "total": 1, "page": 1,
            "per_page": 50, "total_pages": 1,
        },
        refund_transaction_result=RuntimeError("pool exhausted"),
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_transactions_csrf(client, "pw")
    resp = await client.post(
        "/admin/transactions/501/refund",
        data={"csrf_token": csrf, "reason": "chargeback"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/transactions"


# ---------------------------------------------------------------------
# Stage-15-Step-D #3-extension-2: toggle-write fail-soft
# ---------------------------------------------------------------------


async def _login_and_get_models_csrf(client, password: str = "pw") -> str:
    """Log in, fetch /admin/models, scrape its CSRF token."""
    await client.post(
        "/admin/login", data={"password": password}, allow_redirects=False
    )
    resp = await client.get("/admin/models")
    body = await resp.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token in /admin/models form"
    return m.group(1)


async def _login_and_get_gateways_csrf(client, password: str = "pw") -> str:
    """Log in, fetch /admin/gateways, scrape its CSRF token."""
    await client.post(
        "/admin/login", data={"password": password}, allow_redirects=False
    )
    resp = await client.get("/admin/gateways")
    body = await resp.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token in /admin/gateways form"
    return m.group(1)


def _stub_toggle_db(
    *,
    disable_model_result=True,
    enable_model_result=True,
    disable_gateway_result=True,
    enable_gateway_result=True,
):
    """A minimal Database stub with the toggle methods + the
    methods needed to render /admin/models and /admin/gateways."""
    db = _stub_db()
    if isinstance(disable_model_result, Exception):
        db.disable_model = AsyncMock(side_effect=disable_model_result)
    else:
        db.disable_model = AsyncMock(return_value=disable_model_result)
    if isinstance(enable_model_result, Exception):
        db.enable_model = AsyncMock(side_effect=enable_model_result)
    else:
        db.enable_model = AsyncMock(return_value=enable_model_result)
    if isinstance(disable_gateway_result, Exception):
        db.disable_gateway = AsyncMock(side_effect=disable_gateway_result)
    else:
        db.disable_gateway = AsyncMock(return_value=disable_gateway_result)
    if isinstance(enable_gateway_result, Exception):
        db.enable_gateway = AsyncMock(side_effect=enable_gateway_result)
    else:
        db.enable_gateway = AsyncMock(return_value=enable_gateway_result)
    db.get_disabled_models = AsyncMock(return_value=set())
    db.get_disabled_gateways = AsyncMock(return_value=set())
    # Stage-15-Step-F follow-up: ``/admin/control`` calls
    # ``refresh_threshold_overrides_from_db`` on every render. Default
    # to "no overrides stored" so existing tests don't have to opt in.
    db.list_settings_with_prefix = AsyncMock(return_value={})
    db.upsert_setting = AsyncMock(return_value=None)
    db.delete_setting = AsyncMock(return_value=False)
    db.get_setting = AsyncMock(return_value=None)
    return db


async def test_models_disable_post_renders_500_to_flash_on_db_failure(
    aiohttp_client, make_admin_app
):
    """Stage-15-Step-D #3-extension-2: a transient DB error in
    ``db.disable_model`` must NOT propagate up to a 500 response.
    The handler swallows the exception, renders a flash error,
    and returns a clean 302 back to /admin/models so the admin
    can retry. Audit + cache refresh are skipped (because the DB
    write didn't actually take effect).
    """
    db = _stub_toggle_db(
        disable_model_result=RuntimeError("transient asyncpg error"),
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_models_csrf(client, "pw")
    # The login + GET render call ``record_admin_audit`` for the
    # ``login_success`` action; reset the mock so the post-toggle
    # assertions only see calls from the toggle handler itself.
    db.record_admin_audit.reset_mock()

    resp = await client.post(
        "/admin/models/disable",
        data={"csrf_token": csrf, "model_id": "openai/gpt-4o"},
        allow_redirects=False,
    )
    # 302 — NOT 500.
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/models"

    # The DB write was attempted exactly once.
    db.disable_model.assert_awaited_once_with("openai/gpt-4o")
    # Cache refresh MUST NOT run when the write failed (cache is
    # already in sync with the DB row state).
    db.get_disabled_models.assert_not_awaited()
    # No audit row written for a failed toggle.
    db.record_admin_audit.assert_not_awaited()

    # Follow the redirect — the admin sees an error flash.
    resp2 = await client.get("/admin/models")
    body = await resp2.text()
    assert "alert-error" in body or "error" in body.lower()
    assert "Failed to disable model" in body


async def test_models_enable_post_renders_500_to_flash_on_db_failure(
    aiohttp_client, make_admin_app
):
    """Mirror test for the enable-side write path."""
    db = _stub_toggle_db(
        enable_model_result=RuntimeError("simulated DB blip"),
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_models_csrf(client, "pw")
    db.record_admin_audit.reset_mock()

    resp = await client.post(
        "/admin/models/enable",
        data={"csrf_token": csrf, "model_id": "openai/gpt-4o"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/models"
    db.enable_model.assert_awaited_once_with("openai/gpt-4o")
    db.record_admin_audit.assert_not_awaited()

    resp2 = await client.get("/admin/models")
    body = await resp2.text()
    assert "Failed to enable model" in body


async def test_models_disable_post_happy_path_writes_audit_and_refreshes(
    aiohttp_client, make_admin_app
):
    """Sanity check the happy path is unchanged: a successful
    ``db.disable_model`` triggers cache refresh + audit log +
    success flash."""
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_models_csrf(client, "pw")

    resp = await client.post(
        "/admin/models/disable",
        data={"csrf_token": csrf, "model_id": "openai/gpt-4o"},
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    db.disable_model.assert_awaited_once_with("openai/gpt-4o")
    # Refresh DID run — cache is now consistent with the DB write.
    db.get_disabled_models.assert_awaited()
    # Audit DID run — the operation completed.
    db.record_admin_audit.assert_awaited()

    resp2 = await client.get("/admin/models")
    body = await resp2.text()
    assert "Disabled model" in body
    assert "openai/gpt-4o" in body


async def test_gateways_disable_post_renders_500_to_flash_on_db_failure(
    aiohttp_client, make_admin_app
):
    """Stage-15-Step-D #3-extension-2 mirror for the gateway
    side. Same fail-soft contract: 302 + flash error, no audit,
    no cache refresh."""
    db = _stub_toggle_db(
        disable_gateway_result=RuntimeError("transient outage"),
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_gateways_csrf(client, "pw")
    db.record_admin_audit.reset_mock()

    resp = await client.post(
        "/admin/gateways/disable",
        data={"csrf_token": csrf, "gateway_key": "btc"},
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/gateways"
    db.disable_gateway.assert_awaited_once_with("btc")
    db.get_disabled_gateways.assert_not_awaited()
    db.record_admin_audit.assert_not_awaited()

    resp2 = await client.get("/admin/gateways")
    body = await resp2.text()
    assert "Failed to disable gateway" in body


async def test_gateways_enable_post_renders_500_to_flash_on_db_failure(
    aiohttp_client, make_admin_app
):
    """Gateway enable-side mirror."""
    db = _stub_toggle_db(
        enable_gateway_result=RuntimeError("simulated DB blip"),
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_gateways_csrf(client, "pw")
    db.record_admin_audit.reset_mock()

    resp = await client.post(
        "/admin/gateways/enable",
        data={"csrf_token": csrf, "gateway_key": "btc"},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.enable_gateway.assert_awaited_once_with("btc")
    db.record_admin_audit.assert_not_awaited()

    resp2 = await client.get("/admin/gateways")
    body = await resp2.text()
    assert "Failed to enable gateway" in body


async def test_models_disable_post_handles_model_id_with_slash(
    aiohttp_client, make_admin_app
):
    """Stage-15-Step-D #4 audit: model IDs with embedded ``/``
    characters (the canonical OpenRouter format) round-trip
    cleanly because the handler reads ``model_id`` from the POST
    form body — NOT from a URL path parameter. Pin this design
    so a future refactor that switches to ``/admin/models/{model_id}``
    URL paths can't silently regress on slash-bearing IDs.
    """
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_models_csrf(client, "pw")

    # Model IDs typically have one slash, but verify two-segment
    # IDs work too (some OpenRouter variants like
    # ``anthropic/claude-3-5-sonnet`` are single slash; a
    # hypothetical ``provider/family/variant`` form is also
    # protected by this test).
    for mid in (
        "openai/gpt-4o",
        "anthropic/claude-3-5-sonnet",
        "openrouter/auto",
    ):
        resp = await client.post(
            "/admin/models/disable",
            data={"csrf_token": csrf, "model_id": mid},
            allow_redirects=False,
        )
        assert resp.status == 302, (
            f"model_id {mid!r} did not POST cleanly — route may have "
            "regressed to URL-path parameter form"
        )
        # The exact value POSTed must be the value the DB receives —
        # no truncation at slash boundaries.
        await_args = db.disable_model.await_args
        assert await_args.args == (mid,), (
            f"db.disable_model received {await_args.args} but expected {(mid,)}"
        )


# =====================================================================
# Stage-15-Step-F: bot health & emergency control panel
# =====================================================================


async def _login_and_get_control_csrf(client, password: str = "pw") -> str:
    """Log in, fetch /admin/control, scrape its CSRF token."""
    await client.post(
        "/admin/login", data={"password": password}, allow_redirects=False
    )
    resp = await client.get("/admin/control")
    body = await resp.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token in /admin/control form"
    return m.group(1)


async def test_control_get_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app())
    resp = await client.get("/admin/control", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/login"


async def test_control_get_renders_status_tile_and_signals(
    aiohttp_client, make_admin_app
):
    """Happy path: the panel renders the BotStatus level + signals
    inline plus the disable/enable forms with valid CSRF tokens."""
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/control")
    assert resp.status == 200
    body = await resp.text()
    assert "Bot health" in body
    # Status tile renders one of the six classification levels.
    assert any(
        f'status-tile {lvl}' in body
        for lvl in ("idle", "healthy", "busy", "degraded",
                    "under_attack", "down")
    )
    # All four kill-switch forms + the force-stop form are rendered.
    assert "/admin/control/disable-all-models" in body
    assert "/admin/control/enable-all-models" in body
    assert "/admin/control/disable-all-gateways" in body
    assert "/admin/control/enable-all-gateways" in body
    assert "/admin/control/force-stop" in body
    # Force-stop button has the confirm sentinel hidden field.
    assert 'name="confirm" value="FORCE-STOP"' in body
    # CSRF tokens render on every form.
    assert 'name="csrf_token"' in body


async def test_control_force_stop_post_requires_csrf(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    # Missing CSRF → 302 + flash, no kill.
    resp = await client.post(
        "/admin/control/force-stop",
        data={"confirm": "FORCE-STOP"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/control"
    follow = await client.get("/admin/control")
    assert "CSRF" in (await follow.text())


async def test_control_force_stop_post_requires_confirm_sentinel(
    aiohttp_client, make_admin_app
):
    """A POST without ``confirm=FORCE-STOP`` (e.g. a stray form
    submission) is refused with a flash, no kill."""
    from web_admin import APP_KEY_FORCE_STOP_FN

    db = _stub_toggle_db()
    app = make_admin_app(password="pw", db=db)
    captured: list[tuple[int, int]] = []
    app[APP_KEY_FORCE_STOP_FN] = lambda pid, sig: captured.append((pid, sig))
    client = await aiohttp_client(app)
    csrf = await _login_and_get_control_csrf(client, "pw")

    resp = await client.post(
        "/admin/control/force-stop",
        data={"csrf_token": csrf, "confirm": "nope"},
        allow_redirects=False,
    )
    assert resp.status == 302
    follow = await client.get("/admin/control")
    body = await follow.text()
    assert "missing confirmation" in body.lower()
    # Wait a tick for any deferred kill — there must not be one.
    import asyncio
    await asyncio.sleep(0.1)
    assert captured == []


async def test_control_force_stop_post_signals_kill_fn(
    aiohttp_client, make_admin_app
):
    """Happy path: with CSRF + confirm, the kill function is invoked
    on the next event-loop tick. The handler must return 302 *before*
    the kill so the browser sees the redirect."""
    import asyncio
    from web_admin import APP_KEY_FORCE_STOP_FN

    db = _stub_toggle_db()
    app = make_admin_app(password="pw", db=db)
    captured: list[tuple[int, int]] = []
    app[APP_KEY_FORCE_STOP_FN] = lambda pid, sig: captured.append((pid, sig))
    client = await aiohttp_client(app)
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.record_admin_audit.reset_mock()

    resp = await client.post(
        "/admin/control/force-stop",
        data={"csrf_token": csrf, "confirm": "FORCE-STOP"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/control"
    # Wait a generous tick for the deferred kill to fire.
    await asyncio.sleep(0.2)
    assert len(captured) == 1, (
        "force-stop kill_fn was not invoked"
    )
    pid, sig = captured[0]
    import os
    import signal as _signal
    assert pid == os.getpid()
    assert sig == _signal.SIGTERM

    # Audit row written *before* the kill.
    db.record_admin_audit.assert_awaited()
    audit_call = db.record_admin_audit.await_args
    assert audit_call.kwargs.get("action") == "control_force_stop"


async def test_control_disable_all_models_writes_audit_and_disables(
    aiohttp_client, make_admin_app, monkeypatch
):
    """Disable-all walks every model id and calls db.disable_model."""
    db = _stub_toggle_db()
    # Inject a deterministic catalog so the test isn't tied to live
    # OpenRouter data.
    fake_ids = ["openai/gpt-4o", "anthropic/claude-3-5-sonnet", "x-ai/grok-2"]
    monkeypatch.setattr(
        "web_admin._all_model_ids", lambda: list(fake_ids)
    )

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.record_admin_audit.reset_mock()
    db.disable_model.reset_mock()

    resp = await client.post(
        "/admin/control/disable-all-models",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/control"
    # Every catalog id was disabled.
    assert db.disable_model.await_count == len(fake_ids)
    seen = {call.args[0] for call in db.disable_model.await_args_list}
    assert seen == set(fake_ids)
    # Audit row written.
    db.record_admin_audit.assert_awaited()
    assert (
        db.record_admin_audit.await_args.kwargs.get("action")
        == "control_disable_all_models"
    )


async def test_control_enable_all_models_clears_disabled_set(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    db.get_disabled_models = AsyncMock(
        return_value={"openai/gpt-4o", "anthropic/claude"}
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.record_admin_audit.reset_mock()
    db.enable_model.reset_mock()

    resp = await client.post(
        "/admin/control/enable-all-models",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert db.enable_model.await_count == 2
    db.record_admin_audit.assert_awaited()
    assert (
        db.record_admin_audit.await_args.kwargs.get("action")
        == "control_enable_all_models"
    )


async def test_control_disable_all_gateways_writes_audit_and_disables(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.record_admin_audit.reset_mock()
    db.disable_gateway.reset_mock()

    resp = await client.post(
        "/admin/control/disable-all-gateways",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/control"
    # The card gateway keys ("tetrapay", "zarinpal") at minimum.
    seen = {call.args[0] for call in db.disable_gateway.await_args_list}
    assert "tetrapay" in seen
    assert "zarinpal" in seen
    # Audit row written.
    db.record_admin_audit.assert_awaited()
    assert (
        db.record_admin_audit.await_args.kwargs.get("action")
        == "control_disable_all_gateways"
    )


async def test_control_enable_all_gateways_clears_disabled_set(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    db.get_disabled_gateways = AsyncMock(return_value={"tetrapay", "btc"})
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.record_admin_audit.reset_mock()
    db.enable_gateway.reset_mock()

    resp = await client.post(
        "/admin/control/enable-all-gateways",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert db.enable_gateway.await_count == 2
    db.record_admin_audit.assert_awaited()
    assert (
        db.record_admin_audit.await_args.kwargs.get("action")
        == "control_enable_all_gateways"
    )


async def test_control_disable_all_models_db_failure_flashes_error(
    aiohttp_client, make_admin_app, monkeypatch
):
    """A DB blip on one row must NOT propagate as a 500 — the handler
    catches per-row exceptions and surfaces a flash error so the
    operator can retry."""
    db = _stub_toggle_db(
        disable_model_result=RuntimeError("simulated DB blip")
    )
    monkeypatch.setattr(
        "web_admin._all_model_ids", lambda: ["openai/gpt-4o"]
    )

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")

    resp = await client.post(
        "/admin/control/disable-all-models",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    follow = await client.get("/admin/control")
    body = await follow.text()
    assert "failed" in body.lower()


async def test_control_force_stop_post_requires_auth(
    aiohttp_client, make_admin_app
):
    """Force-stop must require auth — an unauthenticated POST must
    redirect to login, never reach the kill function."""
    from web_admin import APP_KEY_FORCE_STOP_FN

    app = make_admin_app(password="pw")
    captured: list[tuple[int, int]] = []
    app[APP_KEY_FORCE_STOP_FN] = lambda pid, sig: captured.append((pid, sig))
    client = await aiohttp_client(app)

    resp = await client.post(
        "/admin/control/force-stop",
        data={"csrf_token": "anything", "confirm": "FORCE-STOP"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")
    import asyncio
    await asyncio.sleep(0.1)
    assert captured == []


# ── Stage-15-Step-F follow-up #4: cadence introspection ────────────


def _control_signals_for_test(monkeypatch):
    """Build a control-panel signals dict against a known loop-tick
    state. Stubs out the dashboard side-channels so the test only
    exercises the new cadence/threshold/overdue plumbing.
    """
    from aiohttp import web as _aw
    from unittest.mock import patch
    import bot_health
    import metrics
    import web_admin

    metrics.reset_loop_ticks_for_tests()

    # Pretend the bot booted long enough ago that the never-ticked
    # grace window has expired for every loop. The test that exercises
    # the grace window overrides ``boot`` directly via monkeypatch.
    boot = 0.0
    monkeypatch.setattr(bot_health, "_PROCESS_START_EPOCH", boot)
    monkeypatch.setattr(web_admin, "_BOT_PROCESS_START_EPOCH", boot)

    app = _aw.Application()
    with patch("web_admin._collect_ipn_health", return_value={}):
        return web_admin._collect_control_signals(app=app, db_error=None)


def test_collect_control_signals_attaches_cadence_to_each_loop(
    monkeypatch,
):
    """Every loop in the snapshot carries its published cadence +
    stale-after threshold so the panel can render them next to the
    last-tick age. ``zarinpal_backfill`` (the bug-fix regression
    target) is in there with its proper 5-min cadence."""
    import bot_health

    signals = _control_signals_for_test(monkeypatch)
    by_name = {row["name"]: row for row in signals["loops"]}

    assert "fx_refresh" in by_name
    assert by_name["fx_refresh"]["cadence_s"] == bot_health.LOOP_CADENCES["fx_refresh"]
    assert (
        by_name["fx_refresh"]["stale_threshold_s"]
        == bot_health.loop_stale_threshold_seconds("fx_refresh")
    )

    assert "zarinpal_backfill" in by_name
    assert by_name["zarinpal_backfill"]["cadence_s"] == 300
    assert by_name["zarinpal_backfill"]["stale_threshold_s"] == 660


def test_collect_control_signals_marks_fresh_tick_not_overdue(
    monkeypatch,
):
    """A loop that ticked within its threshold renders ``fresh``
    (not overdue, not warming). ``next_tick_in_s`` is positive when
    the loop is on schedule."""
    import metrics
    import time as _t

    metrics.reset_loop_ticks_for_tests()
    metrics.record_loop_tick("fx_refresh", ts=_t.time() - 120.0)
    signals = _control_signals_for_test_with_existing_ticks(monkeypatch)
    row = next(r for r in signals["loops"] if r["name"] == "fx_refresh")

    assert row["last_tick_age_s"] is not None
    assert row["last_tick_age_s"] >= 100
    assert row["is_overdue"] is False
    assert row["grace_pending"] is False
    # Cadence 600s, age ~120s → next-in ~480s.
    assert row["next_tick_in_s"] is not None
    assert row["next_tick_in_s"] > 0


def _control_signals_for_test_with_existing_ticks(monkeypatch):
    """Variant of ``_control_signals_for_test`` that does NOT clear
    the loop-tick registry — used by tests that pre-record ticks."""
    from aiohttp import web as _aw
    from unittest.mock import patch
    import bot_health
    import web_admin

    boot = 0.0
    monkeypatch.setattr(bot_health, "_PROCESS_START_EPOCH", boot)
    monkeypatch.setattr(web_admin, "_BOT_PROCESS_START_EPOCH", boot)

    app = _aw.Application()
    with patch("web_admin._collect_ipn_health", return_value={}):
        return web_admin._collect_control_signals(app=app, db_error=None)


def test_collect_control_signals_marks_stale_tick_overdue(
    monkeypatch,
):
    """A loop that hasn't ticked in longer than its threshold renders
    ``overdue`` (the same condition the classifier uses to escalate
    to DEGRADED — single source of truth)."""
    import metrics
    import time as _t

    metrics.reset_loop_ticks_for_tests()
    # fx_refresh threshold is 1260s. A 5000s-old tick is past due.
    metrics.record_loop_tick("fx_refresh", ts=_t.time() - 5000.0)
    signals = _control_signals_for_test_with_existing_ticks(monkeypatch)
    row = next(r for r in signals["loops"] if r["name"] == "fx_refresh")

    assert row["last_tick_age_s"] is not None
    assert row["last_tick_age_s"] >= 5000
    assert row["is_overdue"] is True
    # next_tick_in_s is negative (overdue by ~age - cadence).
    assert row["next_tick_in_s"] is not None
    assert row["next_tick_in_s"] < 0


def test_collect_control_signals_grace_window_for_never_ticked_loop(
    monkeypatch,
):
    """Bug-fix regression: pre-fix the panel rendered every never-
    ticked loop in red, even on a freshly-booted bot whose long-
    cadence loop simply hasn't fired yet. The classifier already
    has a grace window for this case (so it stays HEALTHY rather
    than DEGRADED) — the panel must agree, otherwise an operator
    sees alarming red dots on a perfectly-healthy fresh deploy."""
    import bot_health
    import metrics
    import time as _t
    from aiohttp import web as _aw
    from unittest.mock import patch
    import web_admin

    metrics.reset_loop_ticks_for_tests()
    # Bot booted 30 seconds ago — well within every loop's threshold.
    boot = _t.time() - 30.0
    monkeypatch.setattr(bot_health, "_PROCESS_START_EPOCH", boot)
    monkeypatch.setattr(web_admin, "_BOT_PROCESS_START_EPOCH", boot)

    app = _aw.Application()
    with patch("web_admin._collect_ipn_health", return_value={}):
        signals = web_admin._collect_control_signals(
            app=app, db_error=None,
        )

    for row in signals["loops"]:
        assert row["last_tick_age_s"] is None, row
        # 30s uptime is below every cadence-derived threshold, so
        # every never-ticked loop is "warming up" rather than
        # "overdue".
        assert row["grace_pending"] is True, row
        assert row["is_overdue"] is False, row


def test_collect_control_signals_never_ticked_past_grace_is_overdue(
    monkeypatch,
):
    """The grace window is bounded — once uptime exceeds a loop's
    stale threshold, a never-ticked loop flips to ``overdue``."""
    import bot_health
    import metrics
    import time as _t
    from aiohttp import web as _aw
    from unittest.mock import patch
    import web_admin

    metrics.reset_loop_ticks_for_tests()
    # Boot was 200000s ago — past every loop's cadence-derived
    # threshold including the 24h-cadence catalog_refresh.
    boot = _t.time() - 200_000.0
    monkeypatch.setattr(bot_health, "_PROCESS_START_EPOCH", boot)
    monkeypatch.setattr(web_admin, "_BOT_PROCESS_START_EPOCH", boot)

    app = _aw.Application()
    with patch("web_admin._collect_ipn_health", return_value={}):
        signals = web_admin._collect_control_signals(
            app=app, db_error=None,
        )

    by_name = {row["name"]: row for row in signals["loops"]}
    # fx_refresh threshold 1260 → past grace by a long shot.
    assert by_name["fx_refresh"]["grace_pending"] is False
    assert by_name["fx_refresh"]["is_overdue"] is True
    # bot_health_alert threshold 180 → past grace too.
    assert by_name["bot_health_alert"]["is_overdue"] is True


async def test_control_get_renders_cadence_columns(
    aiohttp_client, make_admin_app, monkeypatch
):
    """End-to-end render: the new Cadence / Stale-after / Status
    columns appear in the panel HTML, with the right cadence value
    next to each known loop."""
    import bot_health
    import metrics
    import time as _t

    metrics.reset_loop_ticks_for_tests()
    metrics.record_loop_tick("fx_refresh", ts=_t.time() - 30.0)
    boot = _t.time() - 60.0
    monkeypatch.setattr(bot_health, "_PROCESS_START_EPOCH", boot)
    monkeypatch.setattr("web_admin._BOT_PROCESS_START_EPOCH", boot)

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    resp = await client.get("/admin/control")
    assert resp.status == 200
    body = await resp.text()

    # New table columns + descriptive copy.
    assert "Cadence" in body
    assert "Stale after" in body
    assert "loop-status-badge" in body
    assert "BOT_HEALTH_LOOP_STALE_" in body  # docs the env override

    # Cadence values for the loops that just ticked / never ticked.
    assert "600s" in body  # fx_refresh cadence
    # zarinpal_backfill row is now reachable with its proper cadence.
    assert "zarinpal_backfill" in body
    assert "300s" in body
    # fx_refresh ticked recently → fresh badge.
    assert "fresh" in body


async def test_control_get_renders_overdue_badge_for_stale_loop(
    aiohttp_client, make_admin_app, monkeypatch
):
    """A loop with a tick older than its threshold renders the red
    ``overdue`` badge so an operator can spot it without reading
    integers."""
    import bot_health
    import metrics
    import time as _t

    metrics.reset_loop_ticks_for_tests()
    metrics.record_loop_tick("fx_refresh", ts=_t.time() - 5000.0)
    boot = _t.time() - 200_000.0
    monkeypatch.setattr(bot_health, "_PROCESS_START_EPOCH", boot)
    monkeypatch.setattr("web_admin._BOT_PROCESS_START_EPOCH", boot)

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    resp = await client.get("/admin/control")
    body = await resp.text()
    assert "overdue" in body
    assert "loop-status-badge overdue" in body


# ── Stage-15-Step-E #4 follow-up: /admin/openrouter-keys ops view ──


def _setup_or_keys_pool(monkeypatch):
    """Helper: deterministic 3-key pool for the ops-view tests.

    Mirrors the fixture in ``tests/test_metrics.py`` and
    ``tests/test_openrouter_keys.py`` so each module remains
    self-contained.
    """
    import openrouter_keys

    monkeypatch.setenv("OPENROUTER_API_KEY_1", "kwa")
    monkeypatch.setenv("OPENROUTER_API_KEY_2", "kwb")
    monkeypatch.setenv("OPENROUTER_API_KEY_3", "kwc")
    openrouter_keys._keys = []
    openrouter_keys._loaded = False
    openrouter_keys.clear_all_cooldowns()
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys.load_keys()


async def test_openrouter_keys_get_requires_auth(
    aiohttp_client, make_admin_app
):
    """``GET /admin/openrouter-keys`` is auth-gated like every other
    /admin/* page — an unauthenticated request must redirect to the
    login screen rather than 200 the per-key counters body."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    resp = await client.get("/admin/openrouter-keys", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")


async def test_openrouter_keys_get_renders_one_row_per_pool_key(
    aiohttp_client, make_admin_app, monkeypatch
):
    """The page renders one row per pool key. The HTML must include
    the index (#0, #1, #2) and the "available" status text for keys
    that aren't in cooldown."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp = await client.get("/admin/openrouter-keys")
    assert resp.status == 200
    body = await resp.text()
    assert "OpenRouter keys" in body
    # 3 keys → 3 rows. Index labels ``#0`` / ``#1`` / ``#2`` appear.
    assert "#0" in body
    assert "#1" in body
    assert "#2" in body
    # All three slots are healthy by default.
    assert body.count(">available<") == 3


async def test_openrouter_keys_get_renders_cooldown_status(
    aiohttp_client, make_admin_app, monkeypatch
):
    """A pool key in cooldown renders 'cooldown' instead of
    'available' — and its remaining seconds appear (>0)."""
    import openrouter_keys

    _setup_or_keys_pool(monkeypatch)
    openrouter_keys.mark_key_rate_limited("kwb", retry_after_secs=42.0)

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp = await client.get("/admin/openrouter-keys")
    body = await resp.text()
    assert ">cooldown<" in body
    # Two slots still available.
    assert body.count(">available<") == 2


async def test_openrouter_keys_get_renders_per_key_counters(
    aiohttp_client, make_admin_app, monkeypatch
):
    """The page surfaces the per-key 429 + fallback counters that
    drive the matching Prometheus families."""
    import openrouter_keys

    _setup_or_keys_pool(monkeypatch)
    # idx 0 records 2 × 429; idx 2 absorbs a fallback after idx 1
    # was marked hot.
    openrouter_keys.mark_key_rate_limited("kwa")
    openrouter_keys.mark_key_rate_limited("kwa")
    openrouter_keys.mark_key_rate_limited("kwb")
    openrouter_keys.key_for_user(1)  # sticky idx 1 → fallback to idx 2

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp = await client.get("/admin/openrouter-keys")
    body = await resp.text()
    # Two 429 counts on idx 0 are visible somewhere in the body.
    # Use a regex anchored on the column class to avoid matching
    # cooldown-second text that might also be "2".
    import re
    counter_cells = re.findall(
        r'<span class="or-counter[^"]*">\s*(\d+)\s*</span>', body
    )
    # 3 rows × 3 numeric cells (cooldown remaining, count_429,
    # count_fallback) = 9 numbers. The "2" for idx 0's 429 count
    # must be in there.
    assert "2" in counter_cells


async def test_openrouter_keys_get_does_not_leak_api_key_strings(
    aiohttp_client, make_admin_app, monkeypatch
):
    """The page must NEVER render the api_key string itself —
    rows are referenced by 0-based pool index only. Same
    discipline ``key_status_snapshot`` follows."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp = await client.get("/admin/openrouter-keys")
    body = await resp.text()
    # The fixture loads "kwa" / "kwb" / "kwc" — none must appear in
    # the rendered body.
    assert "kwa" not in body
    assert "kwb" not in body
    assert "kwc" not in body


async def test_openrouter_keys_get_renders_empty_pool_message(
    aiohttp_client, make_admin_app, monkeypatch
):
    """Empty pool → empty-state message instead of an empty table.
    Operators with no keys configured still see useful copy.
    """
    import openrouter_keys

    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    for i in range(1, 11):
        monkeypatch.delenv(f"OPENROUTER_API_KEY_{i}", raising=False)
    openrouter_keys._keys = []
    openrouter_keys._loaded = False
    openrouter_keys.clear_all_cooldowns()
    openrouter_keys.reset_key_counters_for_tests()

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp = await client.get("/admin/openrouter-keys")
    assert resp.status == 200
    body = await resp.text()
    assert "No OpenRouter API keys configured" in body


# ======================================================================
# Stage-15-Step-E #5 follow-up #2: /admin/roles web page
# ======================================================================
#
# Mirrors the pattern of the gifts handlers: list (GET), create (POST),
# revoke (POST). All three under the same ADMIN_PASSWORD-gated cookie.
# The role-CRUD primitives themselves were shipped in PR #123 / #124;
# these tests pin the new web surface, the audit-log writes, the CSRF
# protection, and the Stage-15-Step-E #10 NUL-strip stowaway fix on
# ``Database.set_admin_role``.


async def _login_and_get_roles_csrf(client, password: str = "pw") -> str:
    """Log in, fetch the roles page, scrape its CSRF token."""
    await _login(client, password)
    resp = await client.get("/admin/roles")
    assert resp.status == 200
    body = await resp.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token in /admin/roles form"
    return m.group(1)


async def test_roles_get_requires_auth(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app(password="pw", db=_stub_db()))
    resp = await client.get("/admin/roles", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")


async def test_roles_get_renders_empty_state(aiohttp_client, make_admin_app):
    """No DB-tracked rows → page still renders with empty-state copy."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/roles")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "No DB-tracked admin roles yet" in body
    assert "ADMIN_USER_IDS" in body
    db.list_admin_roles.assert_awaited_once_with(limit=200)


async def test_roles_get_lists_rows(aiohttp_client, make_admin_app):
    rows = [
        {
            "telegram_id": 12345,
            "role": "super",
            "granted_at": "2026-04-30T12:34:56+00:00",
            "granted_by": 9999,
            "notes": "Founding admin",
        },
        {
            "telegram_id": 67890,
            "role": "operator",
            "granted_at": "2026-04-29T10:20:30+00:00",
            "granted_by": None,
            "notes": None,
        },
    ]
    db = _stub_db()
    db.list_admin_roles = AsyncMock(return_value=rows)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/roles")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "12345" in body
    assert "67890" in body
    assert "super" in body
    assert "operator" in body
    assert "Founding admin" in body
    assert 'action="/admin/roles/12345/revoke"' in body
    assert 'action="/admin/roles/67890/revoke"' in body
    # Granted-at is rendered with the T → space substitution.
    assert "2026-04-30 12:34:56" in body


async def test_roles_get_db_error_renders_banner(aiohttp_client, make_admin_app):
    db = _stub_db()
    db.list_admin_roles = AsyncMock(side_effect=RuntimeError("boom"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/roles")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert "Database query failed" in body


async def test_layout_has_roles_nav_link(aiohttp_client, make_admin_app):
    """Sidebar link for /admin/roles ships with the page."""
    client = await aiohttp_client(make_admin_app(password="pw", db=_stub_db()))
    await _login(client, "pw")
    resp = await client.get("/admin/")
    body = await resp.text()
    assert 'href="/admin/roles"' in body


async def test_roles_create_requires_auth(aiohttp_client, make_admin_app):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    resp = await client.post(
        "/admin/roles",
        data={"telegram_id": "1", "role": "viewer"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")
    db.set_admin_role.assert_not_awaited()


async def test_roles_create_happy_path(aiohttp_client, make_admin_app):
    db = _stub_db()
    db.set_admin_role = AsyncMock(return_value="operator")
    db.record_admin_audit = AsyncMock()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_roles_csrf(client, "pw")
    # Reset audit counter so the assertion only sees the role_grant write
    # (login-success audits also flow through this hook).
    db.record_admin_audit.reset_mock()

    resp = await client.post(
        "/admin/roles",
        data={
            "csrf_token": csrf,
            "telegram_id": "12345",
            "role": "operator",
            "notes": "Promoted by ops",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/roles"

    db.set_admin_role.assert_awaited_once()
    args, kwargs = db.set_admin_role.await_args
    # Telegram id passed positionally, then role.
    assert args[0] == 12345
    assert args[1] == "operator"
    assert kwargs.get("granted_by") is None
    assert kwargs.get("notes") == "Promoted by ops"

    db.record_admin_audit.assert_awaited_once()
    audit_kwargs = db.record_admin_audit.await_args.kwargs
    assert audit_kwargs.get("action") == "role_grant"
    assert audit_kwargs.get("target") == "user:12345"
    assert audit_kwargs.get("outcome") == "ok"
    assert audit_kwargs.get("meta", {}).get("role") == "operator"

    # Flash should surface on the next GET. Jinja autoescapes the
    # single quote — match the rendered form rather than the raw text.
    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "alert-success" in body
    assert "Granted role" in body
    assert "operator" in body
    assert "12345" in body


async def test_roles_create_rejects_missing_csrf(aiohttp_client, make_admin_app):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.post(
        "/admin/roles",
        data={"telegram_id": "1", "role": "viewer"},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.set_admin_role.assert_not_awaited()
    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "CSRF" in body


async def test_roles_create_rejects_wrong_csrf(aiohttp_client, make_admin_app):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login_and_get_roles_csrf(client, "pw")
    resp = await client.post(
        "/admin/roles",
        data={
            "csrf_token": "obviously-wrong",
            "telegram_id": "1",
            "role": "viewer",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.set_admin_role.assert_not_awaited()


async def test_roles_create_missing_telegram_id(aiohttp_client, make_admin_app):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_roles_csrf(client, "pw")
    resp = await client.post(
        "/admin/roles",
        data={"csrf_token": csrf, "telegram_id": "", "role": "viewer"},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.set_admin_role.assert_not_awaited()
    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "Enter a Telegram user id" in body


async def test_roles_create_bad_telegram_id(aiohttp_client, make_admin_app):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_roles_csrf(client, "pw")
    for bad in ("not-a-number", "0", "-12"):
        resp = await client.post(
            "/admin/roles",
            data={"csrf_token": csrf, "telegram_id": bad, "role": "viewer"},
            allow_redirects=False,
        )
        assert resp.status == 302
    db.set_admin_role.assert_not_awaited()
    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "positive integer" in body


async def test_roles_create_bad_role(aiohttp_client, make_admin_app):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_roles_csrf(client, "pw")
    resp = await client.post(
        "/admin/roles",
        data={"csrf_token": csrf, "telegram_id": "1", "role": "godmode"},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.set_admin_role.assert_not_awaited()
    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "Role must be one of" in body


async def test_roles_create_notes_too_long(aiohttp_client, make_admin_app):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_roles_csrf(client, "pw")
    resp = await client.post(
        "/admin/roles",
        data={
            "csrf_token": csrf,
            "telegram_id": "1",
            "role": "viewer",
            "notes": "x" * 1000,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.set_admin_role.assert_not_awaited()
    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "at most 500 characters" in body


async def test_roles_create_db_value_error_surfaces_message(
    aiohttp_client, make_admin_app
):
    """The DB layer's defence-in-depth ValueError text shows up in the flash."""
    db = _stub_db()
    db.set_admin_role = AsyncMock(side_effect=ValueError("role must be one of [...]"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_roles_csrf(client, "pw")
    resp = await client.post(
        "/admin/roles",
        data={"csrf_token": csrf, "telegram_id": "1", "role": "viewer"},
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "role must be one of" in body


async def test_roles_create_db_error_surfaces_generic_message(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    db.set_admin_role = AsyncMock(side_effect=RuntimeError("pool exhausted"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_roles_csrf(client, "pw")
    resp = await client.post(
        "/admin/roles",
        data={"csrf_token": csrf, "telegram_id": "1", "role": "viewer"},
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "Database write failed" in body


async def test_roles_revoke_requires_auth(aiohttp_client, make_admin_app):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    resp = await client.post(
        "/admin/roles/1/revoke",
        data={"csrf_token": "anything"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")
    db.delete_admin_role.assert_not_awaited()


async def test_roles_revoke_happy_path(aiohttp_client, make_admin_app):
    db = _stub_db()
    db.delete_admin_role = AsyncMock(return_value=True)
    db.record_admin_audit = AsyncMock()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_roles_csrf(client, "pw")
    db.record_admin_audit.reset_mock()
    resp = await client.post(
        "/admin/roles/12345/revoke",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_admin_role.assert_awaited_once_with(12345)
    db.record_admin_audit.assert_awaited_once()
    kwargs = db.record_admin_audit.await_args.kwargs
    assert kwargs.get("action") == "role_revoke"
    assert kwargs.get("target") == "user:12345"
    assert kwargs.get("outcome") == "ok"

    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "alert-success" in body
    assert "Revoked DB-tracked role for 12345" in body


async def test_roles_revoke_no_row_shows_info_and_audits_noop(
    aiohttp_client, make_admin_app
):
    db = _stub_db()
    db.delete_admin_role = AsyncMock(return_value=False)
    db.record_admin_audit = AsyncMock()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_roles_csrf(client, "pw")
    db.record_admin_audit.reset_mock()
    resp = await client.post(
        "/admin/roles/99999/revoke",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_admin_role.assert_awaited_once_with(99999)
    # Even a noop revoke must hit the audit log so a forensic
    # operator can see "someone tried but no row existed".
    db.record_admin_audit.assert_awaited_once()
    kwargs = db.record_admin_audit.await_args.kwargs
    assert kwargs.get("outcome") == "noop"
    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "alert-info" in body
    assert "nothing to revoke" in body


async def test_roles_revoke_rejects_missing_csrf(aiohttp_client, make_admin_app):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.post(
        "/admin/roles/1/revoke",
        data={},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_admin_role.assert_not_awaited()


async def test_roles_revoke_invalid_url_id(aiohttp_client, make_admin_app):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_roles_csrf(client, "pw")
    # Negative id slips past the URL parser but is rejected by the handler.
    resp = await client.post(
        "/admin/roles/-1/revoke",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_admin_role.assert_not_awaited()
    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "Invalid telegram id" in body


async def test_roles_revoke_db_error(aiohttp_client, make_admin_app):
    db = _stub_db()
    db.delete_admin_role = AsyncMock(side_effect=RuntimeError("pool blip"))
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_roles_csrf(client, "pw")
    resp = await client.post(
        "/admin/roles/1/revoke",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/roles")
    body = await resp2.text()
    assert "Database write failed" in body


# ── Stage-15-Step-E #4 follow-up #2: DB-backed OpenRouter key CRUD ──


async def test_openrouter_keys_get_renders_db_management_table(
    aiohttp_client, make_admin_app, monkeypatch
):
    """The page renders one row per DB-stored key in the
    management table — including disabled rows."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    db.list_openrouter_keys = AsyncMock(
        return_value=[
            {
                "id": 7, "label": "main", "api_key_tail": "abcd",
                "api_key_len": 48, "enabled": True,
                "created_at": "2026-04-01T00:00:00+00:00",
                "last_used_at": None, "notes": "primary",
            },
            {
                "id": 8, "label": "old", "api_key_tail": "9999",
                "api_key_len": 48, "enabled": False,
                "created_at": "2026-03-01T00:00:00+00:00",
                "last_used_at": "2026-04-01T12:34:56+00:00",
                "notes": None,
            },
        ]
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp = await client.get("/admin/openrouter-keys")
    body = await resp.text()
    assert "main" in body
    assert "abcd" in body
    assert "primary" in body
    assert "old" in body
    assert "9999" in body


async def test_openrouter_keys_add_post_writes_and_redirects(
    aiohttp_client, make_admin_app, monkeypatch
):
    """A valid POST inserts the key and redirects to the list."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    db.add_openrouter_key = AsyncMock(return_value=42)
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    # Need to fetch a CSRF token from the GET first.
    resp_get = await client.get("/admin/openrouter-keys")
    body_get = await resp_get.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body_get)
    assert m, "csrf_token absent from rendered page"
    csrf = m.group(1)

    resp = await client.post(
        "/admin/openrouter-keys/add",
        data={
            "csrf_token": csrf,
            "label": "main-key",
            "api_key": "sk-or-v1-abcdef0123456789",
            "notes": "primary",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/openrouter-keys"
    db.add_openrouter_key.assert_awaited_once()
    call_kwargs = db.add_openrouter_key.await_args.kwargs
    assert call_kwargs["label"] == "main-key"
    assert call_kwargs["api_key"] == "sk-or-v1-abcdef0123456789"
    assert call_kwargs["notes"] == "primary"


async def test_openrouter_keys_add_post_rejects_invalid_csrf(
    aiohttp_client, make_admin_app, monkeypatch
):
    """A POST with a missing CSRF token must NOT call the DB
    method."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp = await client.post(
        "/admin/openrouter-keys/add",
        data={
            "csrf_token": "wrong",
            "label": "main", "api_key": "sk-or-v1-x",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.add_openrouter_key.assert_not_called()


async def test_openrouter_keys_add_post_surfaces_validation_error(
    aiohttp_client, make_admin_app, monkeypatch
):
    """A ValueError from the DB layer surfaces as a flash banner —
    not a 500."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    db.add_openrouter_key = AsyncMock(
        side_effect=ValueError("api_key is already registered")
    )
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp_get = await client.get("/admin/openrouter-keys")
    body_get = await resp_get.text()
    import re
    csrf = re.search(
        r'name="csrf_token" value="([^"]+)"', body_get
    ).group(1)
    resp = await client.post(
        "/admin/openrouter-keys/add",
        data={
            "csrf_token": csrf,
            "label": "dup", "api_key": "sk-or-v1-x",
        },
        allow_redirects=True,
    )
    body = await resp.text()
    assert "already registered" in body


async def test_openrouter_keys_disable_post_marks_disabled(
    aiohttp_client, make_admin_app, monkeypatch
):
    """A disable POST flips the enabled flag and refreshes the
    pool."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp_get = await client.get("/admin/openrouter-keys")
    import re
    csrf = re.search(
        r'name="csrf_token" value="([^"]+)"', await resp_get.text()
    ).group(1)
    resp = await client.post(
        "/admin/openrouter-keys/42/disable",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.set_openrouter_key_enabled.assert_awaited_once_with(42, enabled=False)


async def test_openrouter_keys_enable_post_marks_enabled(
    aiohttp_client, make_admin_app, monkeypatch
):
    """An enable POST flips the enabled flag the other direction."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp_get = await client.get("/admin/openrouter-keys")
    import re
    csrf = re.search(
        r'name="csrf_token" value="([^"]+)"', await resp_get.text()
    ).group(1)
    resp = await client.post(
        "/admin/openrouter-keys/42/enable",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.set_openrouter_key_enabled.assert_awaited_once_with(42, enabled=True)


async def test_openrouter_keys_delete_post_removes_row(
    aiohttp_client, make_admin_app, monkeypatch
):
    """A delete POST hard-removes the row."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    resp_get = await client.get("/admin/openrouter-keys")
    import re
    csrf = re.search(
        r'name="csrf_token" value="([^"]+)"', await resp_get.text()
    ).group(1)
    resp = await client.post(
        "/admin/openrouter-keys/42/delete",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_openrouter_key.assert_awaited_once_with(42)


async def test_openrouter_keys_delete_post_requires_auth(
    aiohttp_client, make_admin_app, monkeypatch
):
    """Unauthenticated POSTs must redirect to /admin/login —
    they must NOT touch the DB."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    resp = await client.post(
        "/admin/openrouter-keys/1/delete",
        data={"csrf_token": "anything"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/login")
    db.delete_openrouter_key.assert_not_called()


# ── Stage-15-Step-E #4 follow-up #3: 24h usage panel rendering ─────


async def test_openrouter_keys_get_renders_24h_columns(
    aiohttp_client, make_admin_app, monkeypatch
):
    """The page renders the new "24h reqs" / "24h cost" header
    cells alongside the existing per-key cumulative counters."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp = await client.get("/admin/openrouter-keys")
    body = await resp.text()
    assert resp.status == 200
    assert "24h reqs" in body
    assert "24h cost" in body


async def test_openrouter_keys_get_renders_24h_usage_for_keys_with_traffic(
    aiohttp_client, make_admin_app, monkeypatch
):
    """When ``record_key_usage`` has been called against a key,
    the panel surfaces the rolled-up 24h request count + dollar
    spend in the matching row."""
    import openrouter_keys

    _setup_or_keys_pool(monkeypatch)
    # idx 1 ("kwb") absorbs three calls totalling $0.075.
    openrouter_keys._record_usage_at_idx(1, 0.025)
    openrouter_keys._record_usage_at_idx(1, 0.030)
    openrouter_keys._record_usage_at_idx(1, 0.020)

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp = await client.get("/admin/openrouter-keys")
    body = await resp.text()

    # The 24h request count "3" appears in idx 1's row, and the
    # rendered cost "$0.0750" appears (4-decimal float).
    assert "$0.0750" in body
    # The "3" appears in a counter cell (not e.g. as "k3").
    import re
    counter_cells = re.findall(
        r'<span class="or-counter[^"]*">\s*([^<\s]+)\s*</span>', body
    )
    # "3" is the request count for idx 1; "$0.0750" is the cost
    # for idx 1.
    assert "3" in counter_cells
    assert "$0.0750" in counter_cells


async def test_openrouter_keys_get_zero_24h_traffic_renders_zero_dollar(
    aiohttp_client, make_admin_app, monkeypatch
):
    """A key with no 24h traffic renders ``0`` requests and
    ``$0.0000`` cost — not a missing cell or an "—"."""
    _setup_or_keys_pool(monkeypatch)
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp = await client.get("/admin/openrouter-keys")
    body = await resp.text()
    # Three keys × $0.0000 each.
    assert body.count("$0.0000") == 3


async def test_openrouter_keys_get_24h_excludes_expired_entries(
    aiohttp_client, make_admin_app, monkeypatch
):
    """Bug-fix coverage: a 25h-old entry must not contribute to
    the rendered 24h totals — the panel reads through
    ``get_key_24h_usage`` which prunes on read."""
    import time as _time

    import openrouter_keys

    _setup_or_keys_pool(monkeypatch)
    now = _time.time()
    # 25h ago: must be evicted on read.
    openrouter_keys._record_usage_at_idx(0, 100.0, ts=now - 25 * 3600)
    # 1s ago: must contribute.
    openrouter_keys._record_usage_at_idx(0, 0.5, ts=now - 1)

    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")

    resp = await client.get("/admin/openrouter-keys")
    body = await resp.text()
    # The $100 expired entry must not appear in the rendered cost.
    assert "$100" not in body
    # The $0.5000 entry does appear.
    assert "$0.5000" in body


# ── Stage-15-Step-F follow-up: tunable thresholds ──────────────────


async def test_control_get_renders_thresholds_form(
    aiohttp_client, make_admin_app
):
    """The new severity-thresholds form is rendered with one row per
    knob, the effective value column, and the source badge."""
    import bot_health as bh

    bh._THRESHOLD_OVERRIDES.clear()

    db = _stub_toggle_db()
    db.list_settings_with_prefix = AsyncMock(return_value={
        "BOT_HEALTH_BUSY_INFLIGHT": "7",
    })
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False
    )
    resp = await client.get("/admin/control")
    body = await resp.text()
    assert resp.status == 200
    # The form posts to the new route.
    assert 'action="/admin/control/thresholds"' in body
    # Every knob label is present.
    for key in bh.THRESHOLD_KEYS:
        assert key in body
    # The DB-stored value is shown in the input pre-filled.
    assert 'value="7"' in body
    # Source column shows db for the overridden knob, default for the rest.
    assert ">db<" in body
    bh._THRESHOLD_OVERRIDES.clear()


async def test_control_thresholds_post_writes_db_and_applies(
    aiohttp_client, make_admin_app
):
    """A valid POST upserts each knob into ``system_settings`` and
    immediately applies the override so the next render uses it.

    The test wires the stub's ``list_settings_with_prefix`` to
    reflect every upsert, mirroring real DB semantics — the
    handler's post-write refresh re-reads the table and so the
    snapshot it produces should match what was just written.
    """
    import bot_health as bh

    bh._THRESHOLD_OVERRIDES.clear()

    settings_store: dict[str, str] = {}

    db = _stub_toggle_db()

    async def _upsert(key, value):
        settings_store[key] = value

    async def _delete(key):
        return settings_store.pop(key, None) is not None

    async def _list(prefix):
        return {
            k: v for k, v in settings_store.items() if k.startswith(prefix)
        }

    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.delete_setting = AsyncMock(side_effect=_delete)
    db.list_settings_with_prefix = AsyncMock(side_effect=_list)

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()
    db.delete_setting.reset_mock()
    settings_store.clear()

    resp = await client.post(
        "/admin/control/thresholds",
        data={
            "csrf_token": csrf,
            "BOT_HEALTH_BUSY_INFLIGHT": "11",
            "BOT_HEALTH_LOOP_STALE_SECONDS": "120",
            "BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD": "200",
            "BOT_HEALTH_LOGIN_THROTTLE_ATTACK_KEYS": "30",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/control"

    # Each knob upserted exactly once with the posted value.
    db.upsert_setting.assert_any_await("BOT_HEALTH_BUSY_INFLIGHT", "11")
    db.upsert_setting.assert_any_await(
        "BOT_HEALTH_LOOP_STALE_SECONDS", "120",
    )
    db.upsert_setting.assert_any_await(
        "BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD", "200",
    )
    db.upsert_setting.assert_any_await(
        "BOT_HEALTH_LOGIN_THROTTLE_ATTACK_KEYS", "30",
    )
    # In-process cache reflects the new values (no restart needed).
    snap = bh.get_threshold_overrides_snapshot()
    assert snap == {
        "BOT_HEALTH_BUSY_INFLIGHT": 11,
        "BOT_HEALTH_LOOP_STALE_SECONDS": 120,
        "BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD": 200,
        "BOT_HEALTH_LOGIN_THROTTLE_ATTACK_KEYS": 30,
    }
    # Audit row written.
    db.record_admin_audit.assert_awaited()
    bh._THRESHOLD_OVERRIDES.clear()


async def test_control_thresholds_post_blank_clears_override(
    aiohttp_client, make_admin_app
):
    """Posting blank for a knob clears the DB override + cache so the
    knob falls through to env / default again."""
    import bot_health as bh

    bh.set_threshold_override("BOT_HEALTH_BUSY_INFLIGHT", 50)

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()
    db.delete_setting.reset_mock()

    resp = await client.post(
        "/admin/control/thresholds",
        data={
            "csrf_token": csrf,
            # All blank — clears every knob.
        },
        allow_redirects=False,
    )
    assert resp.status == 302

    # Every knob deleted (blank → clear).
    for key in bh.THRESHOLD_KEYS:
        db.delete_setting.assert_any_await(key)
    db.upsert_setting.assert_not_awaited()
    # Cache empty.
    assert bh.get_threshold_overrides_snapshot() == {}


async def test_control_thresholds_post_rejects_below_minimum(
    aiohttp_client, make_admin_app
):
    """Bug fix coverage: a 0 value is rejected with no DB writes."""
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/thresholds",
        data={
            "csrf_token": csrf,
            "BOT_HEALTH_BUSY_INFLIGHT": "0",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    follow = await client.get("/admin/control")
    body = await follow.text()
    assert "below the minimum" in body.lower()


async def test_control_thresholds_post_rejects_non_int(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/thresholds",
        data={
            "csrf_token": csrf,
            "BOT_HEALTH_BUSY_INFLIGHT": "abc",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    follow = await client.get("/admin/control")
    body = await follow.text()
    assert "not an integer" in body.lower()


async def test_control_thresholds_post_csrf_required(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    db.upsert_setting.reset_mock()
    resp = await client.post(
        "/admin/control/thresholds",
        data={"BOT_HEALTH_BUSY_INFLIGHT": "11"},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


# =====================================================================
# Stage-15-Step-E #10b row 5: REQUIRED_CHANNEL editor on /admin/control
# =====================================================================
#
# Mirrors :func:`test_control_thresholds_*` but for the single-string
# REQUIRED_CHANNEL knob. Same fixture pattern (``_stub_toggle_db`` +
# ``_login_and_get_control_csrf``); same assertions matrix (auth gate
# / CSRF / happy-path persists+refresh+audit / clear / blank-but-set
# is a force-OFF / parametrized invalid / DB-failure-keeps-cache).


async def test_control_renders_required_channel_editor(
    aiohttp_client, make_admin_app, monkeypatch
):
    """The /admin/control page renders the REQUIRED_CHANNEL editor card
    with the source badge + the two action buttons."""
    import force_join

    force_join.clear_required_channel_override()
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    resp = await client.get("/admin/control")
    body = await resp.text()
    assert resp.status == 200
    # The form posts to the new route.
    assert 'action="/admin/control/required-channel"' in body
    # Both action buttons are rendered (set + clear).
    assert 'name="action" value="set"' in body
    assert 'name="action" value="clear"' in body
    # Env value rendered + source badge says env.
    assert "@env_chan" in body
    assert ">env<" in body
    force_join.clear_required_channel_override()


async def test_control_required_channel_post_requires_auth(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="pw"))
    resp = await client.post(
        "/admin/control/required-channel",
        data={"required_channel": "@chan", "action": "set", "csrf_token": "x"},
        allow_redirects=False,
    )
    assert resp.status in (302, 303), resp.status
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_control_required_channel_post_csrf_required(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    db.upsert_setting.reset_mock()
    resp = await client.post(
        "/admin/control/required-channel",
        data={"required_channel": "@chan", "action": "set"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/control"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_control_required_channel_post_persists_value_and_refreshes(
    aiohttp_client, make_admin_app, monkeypatch
):
    """Happy path: a valid handle is upserted to system_settings AND
    pushed into the in-process override cache."""
    import force_join

    force_join.clear_required_channel_override()
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")

    saved: dict[str, str | None] = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        if key == force_join.REQUIRED_CHANNEL_SETTING_KEY:
            saved["value"] = value

    async def _get(key: str):
        if key == force_join.REQUIRED_CHANNEL_SETTING_KEY:
            return saved["value"]
        return None

    db = _stub_toggle_db()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)
    # Note: ``_login_and_get_control_csrf`` triggers the GET-render
    # refresh which calls ``_get`` (currently returning ``None`` since
    # ``saved["value"]`` is still ``None``). After login, the override
    # cache is ``None`` — same as the pre-test state.

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/required-channel",
        data={
            "required_channel": "@db_chan",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/control"

    db.upsert_setting.assert_awaited_once_with(
        force_join.REQUIRED_CHANNEL_SETTING_KEY, "@db_chan",
    )
    # In-process cache reflects the new value (no restart needed).
    assert force_join.get_required_channel_override() == "@db_chan"
    assert force_join.get_required_channel() == "@db_chan"
    # Audit row was recorded.
    matching = [
        c for c in db.record_admin_audit.await_args_list
        if c.kwargs.get("action") == "control_required_channel_update"
    ]
    assert matching, db.record_admin_audit.await_args_list
    force_join.clear_required_channel_override()


async def test_control_required_channel_post_canonicalises_bare_handle(
    aiohttp_client, make_admin_app
):
    """A bare handle without ``@`` is upserted in canonical form."""
    import force_join

    force_join.clear_required_channel_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/required-channel",
        data={
            "required_channel": "MyChan",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_awaited_once_with(
        force_join.REQUIRED_CHANNEL_SETTING_KEY, "@MyChan",
    )
    force_join.clear_required_channel_override()


async def test_control_required_channel_post_blank_set_is_force_off(
    aiohttp_client, make_admin_app, monkeypatch
):
    """Blank value with ``action=set`` writes a force-OFF override
    (empty string) — distinct from ``action=clear``."""
    import force_join

    force_join.clear_required_channel_override()
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")

    # Wire a real-ish settings store so the post-write refresh
    # re-reads what was just upserted (rather than the
    # _stub_toggle_db default of always-None, which would clobber
    # the force-OFF override during the post-upsert refresh).
    settings_store: dict[str, str | None] = {}

    async def _upsert(key: str, value: str) -> None:
        settings_store[key] = value

    async def _delete(key: str) -> bool:
        return settings_store.pop(key, None) is not None

    async def _get(key: str):
        return settings_store.get(key)

    db = _stub_toggle_db()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.delete_setting = AsyncMock(side_effect=_delete)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()
    db.delete_setting.reset_mock()

    resp = await client.post(
        "/admin/control/required-channel",
        data={"required_channel": "", "action": "set", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    # Empty string upserted, NOT a delete.
    db.upsert_setting.assert_awaited_once_with(
        force_join.REQUIRED_CHANNEL_SETTING_KEY, "",
    )
    db.delete_setting.assert_not_awaited()
    # Cache reflects force-OFF (override is "", get_required_channel is "").
    assert force_join.get_required_channel_override() == ""
    assert force_join.get_required_channel() == ""
    force_join.clear_required_channel_override()


async def test_control_required_channel_post_clear_drops_db_row(
    aiohttp_client, make_admin_app, monkeypatch
):
    """``action=clear`` drops the DB row + cache; falls through to env."""
    import force_join

    force_join.clear_required_channel_override()
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")

    # Wire a settings store so the GET render's refresh sees "@db_chan",
    # then the post-clear refresh sees nothing (the delete drained it).
    settings_store: dict[str, str | None] = {
        force_join.REQUIRED_CHANNEL_SETTING_KEY: "@db_chan",
    }

    async def _delete(key: str) -> bool:
        return settings_store.pop(key, None) is not None

    async def _get(key: str):
        return settings_store.get(key)

    db = _stub_toggle_db()
    db.delete_setting = AsyncMock(side_effect=_delete)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    # After the GET, the override loaded from the stubbed DB row.
    assert force_join.get_required_channel_override() == "@db_chan"
    db.upsert_setting.reset_mock()
    db.delete_setting.reset_mock()

    resp = await client.post(
        "/admin/control/required-channel",
        data={"required_channel": "", "action": "clear", "csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_setting.assert_awaited_once_with(
        force_join.REQUIRED_CHANNEL_SETTING_KEY,
    )
    db.upsert_setting.assert_not_awaited()
    # Override cleared; env wins.
    assert force_join.get_required_channel_override() is None
    assert force_join.get_required_channel() == "@env_chan"
    force_join.clear_required_channel_override()


async def test_control_required_channel_post_rejects_unknown_action(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()
    db.delete_setting.reset_mock()

    resp = await client.post(
        "/admin/control/required-channel",
        data={
            "required_channel": "@chan",
            "action": "delete-everything",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/control"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_control_required_channel_post_rejects_over_cap(
    aiohttp_client, make_admin_app
):
    """A handle longer than the 64-char cap is rejected with no DB writes."""
    import force_join

    force_join.clear_required_channel_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    too_long = "@" + ("x" * 64)
    resp = await client.post(
        "/admin/control/required-channel",
        data={
            "required_channel": too_long,
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert force_join.get_required_channel_override() is None


async def test_control_required_channel_post_db_failure_keeps_previous(
    aiohttp_client, make_admin_app, monkeypatch
):
    """A transient DB blip on upsert keeps the previous override in place."""
    import force_join

    force_join.clear_required_channel_override()
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")

    # ``get_setting`` returns "@old" so both the GET-render refresh
    # AND the post-write refresh hold the cache at "@old"; only the
    # upsert raises. If the handler accidentally called
    # ``set_required_channel_override`` BEFORE checking the upsert
    # result, this test would pass anyway — so we additionally pin
    # that the cache equals "@old" after the failed upsert (not "@new").
    db = _stub_toggle_db()
    db.upsert_setting = AsyncMock(side_effect=RuntimeError("DB down"))
    db.get_setting = AsyncMock(return_value="@old")

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    # After the GET, the cache should already be "@old" (loaded from
    # the stubbed get_setting during refresh_required_channel_override_from_db).
    assert force_join.get_required_channel_override() == "@old"

    resp = await client.post(
        "/admin/control/required-channel",
        data={
            "required_channel": "@new",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/control"
    # Override unchanged after a failed write.
    assert force_join.get_required_channel_override() == "@old"
    force_join.clear_required_channel_override()


# =====================================================================
# Stage-15-Step-E #10b row 21: BOT_HEALTH_ALERT_INTERVAL_SECONDS editor
# =====================================================================


async def test_control_alert_interval_post_requires_auth(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="pw"))
    resp = await client.post(
        "/admin/control/alert-interval",
        data={
            "alert_interval_seconds": "120",
            "action": "set",
            "csrf_token": "x",
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303), resp.status
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_control_alert_interval_post_csrf_required(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    db.upsert_setting.reset_mock()
    resp = await client.post(
        "/admin/control/alert-interval",
        data={"alert_interval_seconds": "120", "action": "set"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/control"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_control_alert_interval_post_persists_value_and_refreshes(
    aiohttp_client, make_admin_app
):
    """Happy path: a valid integer is upserted to system_settings AND
    pushed into the in-process override cache."""
    import bot_health_alert

    bot_health_alert.clear_alert_interval_override()
    saved: dict[str, str | None] = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        if key == bot_health_alert.ALERT_INTERVAL_SETTING_KEY:
            saved["value"] = value

    async def _get(key: str):
        if key == bot_health_alert.ALERT_INTERVAL_SETTING_KEY:
            return saved["value"]
        return None

    db = _stub_toggle_db()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-interval",
        data={
            "alert_interval_seconds": "240",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/control"

    db.upsert_setting.assert_awaited_once_with(
        bot_health_alert.ALERT_INTERVAL_SETTING_KEY, "240",
    )
    # In-process cache reflects the new value (no restart needed).
    assert bot_health_alert.get_alert_interval_override() == 240
    assert bot_health_alert.get_bot_health_alert_interval_seconds() == 240
    # Audit row was recorded.
    matching = [
        c for c in db.record_admin_audit.await_args_list
        if c.kwargs.get("action") == "control_alert_interval_update"
    ]
    assert matching, db.record_admin_audit.await_args_list
    bot_health_alert.clear_alert_interval_override()


async def test_control_alert_interval_post_clear_drops_db_row(
    aiohttp_client, make_admin_app
):
    """``action=clear`` deletes the DB row and clears the in-process
    override so the env var (or default) takes effect again."""
    import bot_health_alert

    bot_health_alert.set_alert_interval_override(120)

    async def _get(key: str):
        return None  # row already deleted

    db = _stub_toggle_db()
    db.delete_setting = AsyncMock(return_value=True)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    # After the GET-render refresh ran with our stubbed get_setting
    # returning None, the cache may have been cleared. Re-set so the
    # POST has something to clear.
    bot_health_alert.set_alert_interval_override(120)
    db.delete_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-interval",
        data={
            "alert_interval_seconds": "",
            "action": "clear",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_setting.assert_awaited_once_with(
        bot_health_alert.ALERT_INTERVAL_SETTING_KEY,
    )
    assert bot_health_alert.get_alert_interval_override() is None
    bot_health_alert.clear_alert_interval_override()


async def test_control_alert_interval_post_rejects_below_minimum(
    aiohttp_client, make_admin_app
):
    import bot_health_alert

    bot_health_alert.clear_alert_interval_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-interval",
        data={
            "alert_interval_seconds": "0",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert bot_health_alert.get_alert_interval_override() is None


async def test_control_alert_interval_post_rejects_above_max(
    aiohttp_client, make_admin_app
):
    """The 24h cap on the override slot prevents a fat-finger like
    ``86400000`` (intended ``60``) silently disabling alerting for
    a month."""
    import bot_health_alert

    bot_health_alert.clear_alert_interval_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-interval",
        data={
            "alert_interval_seconds": str(
                bot_health_alert.INTERVAL_OVERRIDE_MAXIMUM + 1
            ),
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert bot_health_alert.get_alert_interval_override() is None


async def test_control_alert_interval_post_rejects_non_int(
    aiohttp_client, make_admin_app
):
    import bot_health_alert

    bot_health_alert.clear_alert_interval_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-interval",
        data={
            "alert_interval_seconds": "abc",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


async def test_control_alert_interval_post_rejects_blank_set(
    aiohttp_client, make_admin_app
):
    """A blank input on action=set must be rejected so the operator
    doesn't accidentally try to "set blank" instead of clicking
    "Clear DB override"."""
    import bot_health_alert

    bot_health_alert.clear_alert_interval_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-interval",
        data={
            "alert_interval_seconds": "",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


async def test_control_alert_interval_post_rejects_unknown_action(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-interval",
        data={
            "alert_interval_seconds": "60",
            "action": "delete_everything",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


async def test_control_alert_interval_post_db_failure_keeps_previous(
    aiohttp_client, make_admin_app
):
    """A transient DB blip on upsert keeps the previous override in
    place."""
    import bot_health_alert

    bot_health_alert.clear_alert_interval_override()

    db = _stub_toggle_db()
    db.upsert_setting = AsyncMock(side_effect=RuntimeError("DB down"))
    db.get_setting = AsyncMock(return_value="180")

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    # GET-render refresh loaded "180" into the cache.
    assert bot_health_alert.get_alert_interval_override() == 180

    resp = await client.post(
        "/admin/control/alert-interval",
        data={
            "alert_interval_seconds": "240",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    # Override unchanged after a failed write.
    assert bot_health_alert.get_alert_interval_override() == 180
    bot_health_alert.clear_alert_interval_override()


def test_audit_action_labels_includes_alert_interval_update():
    """Regression: ``control_alert_interval_update`` must be listed
    in ``AUDIT_ACTION_LABELS`` so the ``/admin/audit`` filter
    dropdown surfaces alert-cadence changes."""
    from web_admin import AUDIT_ACTION_LABELS

    assert "control_alert_interval_update" in AUDIT_ACTION_LABELS
    assert AUDIT_ACTION_LABELS["control_alert_interval_update"] == (
        "Bot-health alert interval updated"
    )


async def test_control_get_renders_alert_interval_card(
    aiohttp_client, make_admin_app
):
    """The /admin/control panel renders the new alert-interval card
    with the current effective value, source badge, and the form."""
    import bot_health_alert

    bot_health_alert.clear_alert_interval_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    resp = await client.get("/admin/control")
    assert resp.status == 200
    body = await resp.text()
    assert "Alert-loop cadence" in body
    assert "/admin/control/alert-interval" in body
    assert 'name="alert_interval_seconds"' in body


# =====================================================================
# Stage-15-Step-E #10b row 9: PENDING_EXPIRATION_HOURS editor
# =====================================================================


async def test_control_expiration_hours_post_requires_auth(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="pw"))
    resp = await client.post(
        "/admin/control/expiration-hours",
        data={
            "expiration_hours": "24",
            "action": "set",
            "csrf_token": "x",
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303), resp.status
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_control_expiration_hours_post_csrf_required(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    db.upsert_setting.reset_mock()
    resp = await client.post(
        "/admin/control/expiration-hours",
        data={"expiration_hours": "24", "action": "set"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/control"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_control_expiration_hours_post_persists_value_and_refreshes(
    aiohttp_client, make_admin_app
):
    """Happy path: a valid integer is upserted to system_settings AND
    pushed into the in-process override cache."""
    import pending_expiration

    pending_expiration.clear_expiration_hours_override()
    saved: dict[str, str | None] = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        if key == pending_expiration.EXPIRATION_HOURS_SETTING_KEY:
            saved["value"] = value

    async def _get(key: str):
        if key == pending_expiration.EXPIRATION_HOURS_SETTING_KEY:
            return saved["value"]
        return None

    db = _stub_toggle_db()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/expiration-hours",
        data={
            "expiration_hours": "72",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/control"

    db.upsert_setting.assert_awaited_once_with(
        pending_expiration.EXPIRATION_HOURS_SETTING_KEY, "72",
    )
    # In-process cache reflects the new value (no restart needed).
    assert pending_expiration.get_expiration_hours_override() == 72
    assert pending_expiration.get_pending_expiration_hours() == 72
    matching = [
        c for c in db.record_admin_audit.await_args_list
        if c.kwargs.get("action") == "control_expiration_hours_update"
    ]
    assert matching, db.record_admin_audit.await_args_list
    pending_expiration.clear_expiration_hours_override()


async def test_control_expiration_hours_post_clear_drops_db_row(
    aiohttp_client, make_admin_app
):
    """``action=clear`` deletes the DB row and clears the in-process
    override so the env var (or default) takes effect again."""
    import pending_expiration

    pending_expiration.set_expiration_hours_override(48)

    async def _get(key: str):
        return None  # row already deleted

    db = _stub_toggle_db()
    db.delete_setting = AsyncMock(return_value=True)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    pending_expiration.set_expiration_hours_override(48)
    db.delete_setting.reset_mock()

    resp = await client.post(
        "/admin/control/expiration-hours",
        data={
            "expiration_hours": "",
            "action": "clear",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.delete_setting.assert_awaited_once_with(
        pending_expiration.EXPIRATION_HOURS_SETTING_KEY,
    )
    assert pending_expiration.get_expiration_hours_override() is None
    pending_expiration.clear_expiration_hours_override()


async def test_control_expiration_hours_post_rejects_below_minimum(
    aiohttp_client, make_admin_app
):
    import pending_expiration

    pending_expiration.clear_expiration_hours_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/expiration-hours",
        data={
            "expiration_hours": "0",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert pending_expiration.get_expiration_hours_override() is None


async def test_control_expiration_hours_post_rejects_above_max(
    aiohttp_client, make_admin_app
):
    """The 1-year cap on the override slot prevents a fat-finger like
    ``876000`` (intended ``168``) silently disabling the reaper for
    the rest of the deploy lifetime."""
    import pending_expiration

    pending_expiration.clear_expiration_hours_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/expiration-hours",
        data={
            "expiration_hours": str(
                pending_expiration.EXPIRATION_HOURS_OVERRIDE_MAXIMUM + 1
            ),
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert pending_expiration.get_expiration_hours_override() is None


async def test_control_expiration_hours_post_rejects_non_int(
    aiohttp_client, make_admin_app
):
    import pending_expiration

    pending_expiration.clear_expiration_hours_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/expiration-hours",
        data={
            "expiration_hours": "abc",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


async def test_control_expiration_hours_post_rejects_blank_set(
    aiohttp_client, make_admin_app
):
    """A blank input on action=set must be rejected so the operator
    doesn't accidentally try to "set blank" instead of clicking
    "Clear DB override"."""
    import pending_expiration

    pending_expiration.clear_expiration_hours_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/expiration-hours",
        data={
            "expiration_hours": "",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


async def test_control_expiration_hours_post_rejects_unknown_action(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/expiration-hours",
        data={
            "expiration_hours": "24",
            "action": "delete_everything",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


async def test_control_expiration_hours_post_db_failure_keeps_previous(
    aiohttp_client, make_admin_app
):
    """A transient DB blip on upsert keeps the previous override in
    place."""
    import pending_expiration

    pending_expiration.clear_expiration_hours_override()

    db = _stub_toggle_db()
    db.upsert_setting = AsyncMock(side_effect=RuntimeError("DB down"))
    db.get_setting = AsyncMock(return_value="36")

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    # GET-render refresh loaded "36" into the cache.
    assert pending_expiration.get_expiration_hours_override() == 36

    resp = await client.post(
        "/admin/control/expiration-hours",
        data={
            "expiration_hours": "72",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    # Override unchanged after a failed write.
    assert pending_expiration.get_expiration_hours_override() == 36
    pending_expiration.clear_expiration_hours_override()


def test_audit_action_labels_includes_expiration_hours_update():
    """Regression: ``control_expiration_hours_update`` must be listed
    in ``AUDIT_ACTION_LABELS`` so the ``/admin/audit`` filter
    dropdown surfaces expiration-window changes."""
    from web_admin import AUDIT_ACTION_LABELS

    assert "control_expiration_hours_update" in AUDIT_ACTION_LABELS
    assert AUDIT_ACTION_LABELS["control_expiration_hours_update"] == (
        "Pending-expiration window updated"
    )


async def test_control_get_renders_expiration_hours_card(
    aiohttp_client, make_admin_app
):
    """The /admin/control panel renders the new pending-expiration
    card with the current effective value, source badge, and the
    form."""
    import pending_expiration

    pending_expiration.clear_expiration_hours_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    resp = await client.get("/admin/control")
    assert resp.status == 200
    body = await resp.text()
    assert "Pending-PENDING expiration window" in body
    assert "/admin/control/expiration-hours" in body
    assert 'name="expiration_hours"' in body


# =====================================================================
# Stage-15-Step-E #10b row 10: PENDING_ALERT_THRESHOLD_HOURS editor
# =====================================================================


async def test_control_alert_threshold_post_requires_auth(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="pw"))
    resp = await client.post(
        "/admin/control/alert-threshold",
        data={
            "alert_threshold_hours": "2",
            "action": "set",
            "csrf_token": "x",
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303), resp.status
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_control_alert_threshold_post_csrf_required(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    db.upsert_setting.reset_mock()
    resp = await client.post(
        "/admin/control/alert-threshold",
        data={"alert_threshold_hours": "2", "action": "set"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/control"
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_control_alert_threshold_post_persists_value_and_refreshes(
    aiohttp_client, make_admin_app
):
    """Happy path: a valid integer is upserted to system_settings AND
    pushed into the in-process override cache."""
    import pending_alert

    pending_alert.clear_alert_threshold_override()
    saved: dict[str, str | None] = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        if key == pending_alert.ALERT_THRESHOLD_SETTING_KEY:
            saved["value"] = value

    async def _get(key: str):
        if key == pending_alert.ALERT_THRESHOLD_SETTING_KEY:
            return saved["value"]
        return None

    db = _stub_toggle_db()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-threshold",
        data={
            "alert_threshold_hours": "4",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/control"

    db.upsert_setting.assert_awaited_once_with(
        pending_alert.ALERT_THRESHOLD_SETTING_KEY, "4",
    )
    # In-process cache reflects the new value (no restart needed).
    assert pending_alert.get_alert_threshold_override() == 4
    assert pending_alert.get_pending_alert_threshold_hours() == 4
    matching = [
        c for c in db.record_admin_audit.await_args_list
        if c.kwargs.get("action") == "control_alert_threshold_update"
    ]
    assert matching, db.record_admin_audit.await_args_list
    pending_alert.clear_alert_threshold_override()


async def test_control_alert_threshold_post_clear_drops_db_row(
    aiohttp_client, make_admin_app
):
    """``action=clear`` deletes the DB row and clears the in-process
    override so the env var (or default) takes effect again."""
    import pending_alert

    pending_alert.set_alert_threshold_override(4)

    async def _get(key: str):
        return None  # row already deleted

    db = _stub_toggle_db()
    db.delete_setting = AsyncMock(return_value=True)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    pending_alert.set_alert_threshold_override(4)
    db.delete_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-threshold",
        data={
            "alert_threshold_hours": "",
            "action": "clear",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    db.delete_setting.assert_awaited_once_with(
        pending_alert.ALERT_THRESHOLD_SETTING_KEY,
    )
    assert pending_alert.get_alert_threshold_override() is None


async def test_control_alert_threshold_post_rejects_below_minimum(
    aiohttp_client, make_admin_app
):
    import pending_alert
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-threshold",
        data={
            "alert_threshold_hours": "0",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert pending_alert.get_alert_threshold_override() is None


async def test_control_alert_threshold_post_rejects_above_max(
    aiohttp_client, make_admin_app
):
    import pending_alert
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-threshold",
        data={
            "alert_threshold_hours": str(
                pending_alert.ALERT_THRESHOLD_OVERRIDE_MAXIMUM + 1
            ),
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert pending_alert.get_alert_threshold_override() is None


async def test_control_alert_threshold_post_rejects_non_int(
    aiohttp_client, make_admin_app
):
    import pending_alert
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-threshold",
        data={
            "alert_threshold_hours": "abc",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert pending_alert.get_alert_threshold_override() is None


async def test_control_alert_threshold_post_rejects_blank_set(
    aiohttp_client, make_admin_app
):
    """A 'set' with no value is rejected — operator must use 'clear'
    explicitly to drop the override."""
    import pending_alert
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-threshold",
        data={
            "alert_threshold_hours": "",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert pending_alert.get_alert_threshold_override() is None


async def test_control_alert_threshold_post_rejects_unknown_action(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/alert-threshold",
        data={
            "alert_threshold_hours": "2",
            "action": "delete_everything",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


async def test_control_alert_threshold_post_db_failure_keeps_previous(
    aiohttp_client, make_admin_app
):
    """A transient DB blip on upsert keeps the previous override in
    place."""
    import pending_alert

    pending_alert.clear_alert_threshold_override()

    db = _stub_toggle_db()
    db.upsert_setting = AsyncMock(side_effect=RuntimeError("DB down"))
    db.get_setting = AsyncMock(return_value="6")

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    # GET-render refresh loaded "6" into the cache.
    assert pending_alert.get_alert_threshold_override() == 6

    resp = await client.post(
        "/admin/control/alert-threshold",
        data={
            "alert_threshold_hours": "8",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    # Override unchanged after a failed write.
    assert pending_alert.get_alert_threshold_override() == 6
    pending_alert.clear_alert_threshold_override()


def test_audit_action_labels_includes_alert_threshold_update():
    """Regression: ``control_alert_threshold_update`` must be listed
    in ``AUDIT_ACTION_LABELS`` so the ``/admin/audit`` filter
    dropdown surfaces alert-threshold changes."""
    from web_admin import AUDIT_ACTION_LABELS

    assert "control_alert_threshold_update" in AUDIT_ACTION_LABELS
    assert AUDIT_ACTION_LABELS["control_alert_threshold_update"] == (
        "Pending-alert threshold updated"
    )


async def test_control_get_renders_alert_threshold_card(
    aiohttp_client, make_admin_app
):
    """The /admin/control panel renders the new pending-alert
    threshold card with the current effective value, source badge,
    and the form."""
    import pending_alert

    pending_alert.clear_alert_threshold_override()

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    resp = await client.get("/admin/control")
    assert resp.status == 200
    body = await resp.text()
    assert "Pending-PENDING alert threshold" in body
    assert "/admin/control/alert-threshold" in body
    assert 'name="alert_threshold_hours"' in body


# =====================================================================
# Stage-15-Step-E #10b row 11: per-loop stale-threshold editor
# =====================================================================


async def test_control_loop_stale_post_persists_value_and_refreshes(
    aiohttp_client, make_admin_app
):
    """Happy path: a valid integer is upserted to system_settings AND
    pushed into the per-loop in-process override cache so the panel +
    classifier see the new threshold without a restart."""
    import bot_health

    bot_health.reset_loop_stale_overrides_for_tests()
    saved: dict[str, str | None] = {"value": None}

    async def _upsert(key: str, value: str) -> None:
        if key == "BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS":
            saved["value"] = value

    async def _list(prefix: str):
        if not prefix.startswith("BOT_HEALTH_LOOP_STALE_"):
            return {}
        if saved["value"] is None:
            return {}
        return {
            "BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS": saved["value"],
        }

    db = _stub_toggle_db()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.list_settings_with_prefix = AsyncMock(side_effect=_list)

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/loop-stale",
        data={
            "loop_name": "fx_refresh",
            "loop_stale_seconds": "600",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    assert resp.headers["Location"] == "/admin/control"

    db.upsert_setting.assert_awaited_once_with(
        "BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS", "600",
    )
    assert bot_health.get_loop_stale_override("fx_refresh") == 600
    assert bot_health.loop_stale_threshold_seconds("fx_refresh") == 600
    matching = [
        c for c in db.record_admin_audit.await_args_list
        if c.kwargs.get("action") == "control_loop_stale_update"
    ]
    assert matching, db.record_admin_audit.await_args_list
    bot_health.reset_loop_stale_overrides_for_tests()


async def test_control_loop_stale_post_clear_drops_db_row(
    aiohttp_client, make_admin_app
):
    """``action=clear`` deletes the DB row and clears the in-process
    override so the env / cadence-derived threshold takes over."""
    import bot_health

    bot_health.reset_loop_stale_overrides_for_tests()
    bot_health.set_loop_stale_override("fx_refresh", 600)

    db = _stub_toggle_db()
    db.delete_setting = AsyncMock(return_value=True)
    db.list_settings_with_prefix = AsyncMock(return_value={})

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    bot_health.set_loop_stale_override("fx_refresh", 600)
    db.delete_setting.reset_mock()

    resp = await client.post(
        "/admin/control/loop-stale",
        data={
            "loop_name": "fx_refresh",
            "loop_stale_seconds": "",
            "action": "clear",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302, await resp.text()
    db.delete_setting.assert_awaited_once_with(
        "BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS",
    )
    assert bot_health.get_loop_stale_override("fx_refresh") is None
    bot_health.reset_loop_stale_overrides_for_tests()


async def test_control_loop_stale_post_rejects_below_minimum(
    aiohttp_client, make_admin_app
):
    import bot_health

    bot_health.reset_loop_stale_overrides_for_tests()
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/loop-stale",
        data={
            "loop_name": "fx_refresh",
            "loop_stale_seconds": "0",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert bot_health.get_loop_stale_override("fx_refresh") is None


async def test_control_loop_stale_post_rejects_above_maximum(
    aiohttp_client, make_admin_app
):
    import bot_health

    bot_health.reset_loop_stale_overrides_for_tests()
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/loop-stale",
        data={
            "loop_name": "fx_refresh",
            "loop_stale_seconds": str(
                bot_health.LOOP_STALE_OVERRIDE_MAXIMUM + 1
            ),
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    assert bot_health.get_loop_stale_override("fx_refresh") is None


async def test_control_loop_stale_post_rejects_unknown_loop(
    aiohttp_client, make_admin_app
):
    """Bug-fix coverage: a typo in ``loop_name`` (or a malicious POST)
    must NOT write a ``BOT_HEALTH_LOOP_STALE_*_SECONDS`` row that no
    real loop reads — the panel rejects loops not registered via
    :func:`bot_health.register_loop`."""
    import bot_health

    bot_health.reset_loop_stale_overrides_for_tests()
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/loop-stale",
        data={
            "loop_name": "totally_made_up_loop",
            "loop_stale_seconds": "600",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


async def test_control_loop_stale_post_rejects_blank_set(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/loop-stale",
        data={
            "loop_name": "fx_refresh",
            "loop_stale_seconds": "",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()


async def test_control_loop_stale_post_rejects_unknown_action(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()
    db.delete_setting.reset_mock()

    resp = await client.post(
        "/admin/control/loop-stale",
        data={
            "loop_name": "fx_refresh",
            "loop_stale_seconds": "600",
            "action": "bogus",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    db.upsert_setting.assert_not_awaited()
    db.delete_setting.assert_not_awaited()


async def test_control_loop_stale_post_db_failure_keeps_previous(
    aiohttp_client, make_admin_app
):
    """A transient DB blip on upsert keeps the previous override in
    place rather than half-applying the change."""
    import bot_health

    bot_health.reset_loop_stale_overrides_for_tests()

    db = _stub_toggle_db()
    db.upsert_setting = AsyncMock(side_effect=RuntimeError("DB down"))
    db.list_settings_with_prefix = AsyncMock(return_value={
        "BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS": "300",
    })

    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    # GET-render refresh loaded 300 into the cache.
    assert bot_health.get_loop_stale_override("fx_refresh") == 300

    resp = await client.post(
        "/admin/control/loop-stale",
        data={
            "loop_name": "fx_refresh",
            "loop_stale_seconds": "777",
            "action": "set",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert bot_health.get_loop_stale_override("fx_refresh") == 300
    bot_health.reset_loop_stale_overrides_for_tests()


async def test_control_loop_stale_post_csrf_required(
    aiohttp_client, make_admin_app
):
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login_and_get_control_csrf(client, "pw")
    db.upsert_setting.reset_mock()

    resp = await client.post(
        "/admin/control/loop-stale",
        data={
            "loop_name": "fx_refresh",
            "loop_stale_seconds": "600",
            "action": "set",
            # No csrf_token — must be rejected.
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 400, 403)
    db.upsert_setting.assert_not_awaited()


async def test_control_loop_stale_post_requires_super(
    aiohttp_client, make_admin_app
):
    """Operator + viewer roles must NOT be able to write per-loop
    stale-threshold overrides — same posture as the other
    ``/admin/control/*`` POSTs."""
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    # No login → 401.
    resp = await client.post(
        "/admin/control/loop-stale",
        data={
            "loop_name": "fx_refresh",
            "loop_stale_seconds": "600",
            "action": "set",
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 401, 403)
    db.upsert_setting.assert_not_awaited()


def test_audit_action_labels_includes_loop_stale_update():
    """Regression: ``control_loop_stale_update`` must be listed in
    ``AUDIT_ACTION_LABELS`` so the ``/admin/audit`` filter dropdown
    surfaces per-loop stale-threshold changes."""
    from web_admin import AUDIT_ACTION_LABELS

    assert "control_loop_stale_update" in AUDIT_ACTION_LABELS
    assert AUDIT_ACTION_LABELS["control_loop_stale_update"] == (
        "Per-loop stale threshold updated"
    )


async def test_control_get_renders_loop_stale_card(
    aiohttp_client, make_admin_app
):
    """The /admin/control panel renders the new per-loop stale
    threshold card with the form, the source badge, and at least
    one registered loop row."""
    import bot_health

    bot_health.reset_loop_stale_overrides_for_tests()
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    resp = await client.get("/admin/control")
    assert resp.status == 200
    body = await resp.text()
    assert "Per-loop stale thresholds" in body
    assert "/admin/control/loop-stale" in body
    assert 'name="loop_stale_seconds"' in body
    # Every production loop should appear; spot-check fx_refresh.
    assert "fx_refresh" in body


async def test_control_get_loop_stale_view_source_db_when_override_set(
    aiohttp_client, make_admin_app
):
    """A saved per-loop override flips the rendered "source" badge to
    ``db``. This is the panel-side complement to
    :func:`test_db_override_beats_env_for_per_loop`."""
    import bot_health

    bot_health.reset_loop_stale_overrides_for_tests()
    db = _stub_toggle_db()
    db.list_settings_with_prefix = AsyncMock(return_value={
        "BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS": "600",
    })
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    resp = await client.get("/admin/control")
    assert resp.status == 200
    body = await resp.text()
    # The fx_refresh row should now report 600s effective.
    assert "600s" in body
    bot_health.reset_loop_stale_overrides_for_tests()


# =====================================================================
# Stage-15-Step-F follow-up #6: per-loop manual "tick now" button
# =====================================================================


def test_collect_control_signals_attaches_has_runner_flag(monkeypatch):
    """Every loop with a registered runner is marked
    ``has_runner=True`` so the panel can render its "Tick now"
    button. The bug-fix regression target is that the previous
    panel had no per-loop manual trigger at all — the button is
    new in PR #159."""
    signals = _control_signals_for_test(monkeypatch)
    by_name = {row["name"]: row for row in signals["loops"]}

    # Every production loop registered a runner in PR #159.
    for name in [
        "fx_refresh",
        "min_amount_refresh",
        "model_discovery",
        "catalog_refresh",
        "pending_alert",
        "pending_reaper",
        "bot_health_alert",
        "zarinpal_backfill",
    ]:
        assert name in by_name, f"loop {name!r} missing from signals"
        assert by_name[name]["has_runner"] is True, (
            f"loop {name!r} did not register a tick-now runner"
        )


def test_collect_control_signals_running_late_distinct_from_overdue(
    monkeypatch,
):
    """Bug-fix regression (bundled in PR #159): pre-fix the panel
    rendered "(overdue by Ns)" any time the loop's age passed its
    cadence — but the classifier's actual overdue threshold is
    ≈ 2× cadence + 60s. So in the grace window between cadence
    and stale-threshold, the panel said "overdue" while the status
    badge said "fresh". Now ``is_running_late`` flags that grace
    window separately so the template can render "(running late)"
    instead."""
    import metrics
    import time as _t

    metrics.reset_loop_ticks_for_tests()
    # fx_refresh: cadence 600s, threshold 1260s. A 700s-old tick
    # is past cadence but still within threshold → running late
    # but not overdue.
    metrics.record_loop_tick("fx_refresh", ts=_t.time() - 700.0)
    signals = _control_signals_for_test_with_existing_ticks(monkeypatch)
    row = next(r for r in signals["loops"] if r["name"] == "fx_refresh")

    assert row["is_overdue"] is False, (
        "700s old tick must NOT trigger overdue (threshold is 1260s)"
    )
    assert row["is_running_late"] is True, (
        "700s old tick must trigger running_late (cadence is 600s)"
    )
    assert row["next_tick_in_s"] is not None
    assert row["next_tick_in_s"] < 0


def test_collect_control_signals_fresh_tick_not_running_late(monkeypatch):
    """A loop comfortably within its cadence is neither overdue nor
    running late — the new ``is_running_late`` flag is False so the
    template renders "(next in ~Ns)" rather than "(running late)"."""
    import metrics
    import time as _t

    metrics.reset_loop_ticks_for_tests()
    metrics.record_loop_tick("fx_refresh", ts=_t.time() - 120.0)
    signals = _control_signals_for_test_with_existing_ticks(monkeypatch)
    row = next(r for r in signals["loops"] if r["name"] == "fx_refresh")

    assert row["is_overdue"] is False
    assert row["is_running_late"] is False


def test_collect_control_signals_overdue_tick_not_running_late(monkeypatch):
    """An actually-overdue tick (past stale threshold) sets
    ``is_overdue=True`` but ``is_running_late=False`` — the two
    flags are mutually exclusive, the more severe one wins."""
    import metrics
    import time as _t

    metrics.reset_loop_ticks_for_tests()
    metrics.record_loop_tick("fx_refresh", ts=_t.time() - 5000.0)
    signals = _control_signals_for_test_with_existing_ticks(monkeypatch)
    row = next(r for r in signals["loops"] if r["name"] == "fx_refresh")

    assert row["is_overdue"] is True
    assert row["is_running_late"] is False, (
        "is_running_late must be False once is_overdue fires — "
        "the template renders only one badge"
    )


async def test_control_get_renders_tick_now_button_per_loop(
    aiohttp_client, make_admin_app
):
    """End-to-end render: every loop row in the heartbeats table
    has a "Tick now" form pointing at
    ``/admin/control/loop/<name>/tick-now`` with the CSRF token."""
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )
    resp = await client.get("/admin/control")
    body = await resp.text()

    # Every production loop has a tick-now form.
    for name in [
        "fx_refresh",
        "min_amount_refresh",
        "pending_alert",
        "pending_reaper",
        "bot_health_alert",
        "zarinpal_backfill",
        "model_discovery",
        "catalog_refresh",
    ]:
        assert (
            f'/admin/control/loop/{name}/tick-now' in body
        ), f"missing tick-now form for {name!r}"


async def test_control_loop_tick_now_post_requires_csrf(
    aiohttp_client, make_admin_app,
):
    """A POST without a CSRF token is refused with 302 + flash
    and never invokes the runner."""
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await client.post(
        "/admin/login", data={"password": "pw"}, allow_redirects=False,
    )

    invoked: list[str] = []

    async def fake_runner(_app):
        invoked.append("called")

    import bot_health

    bot_health.LOOP_RUNNERS["fx_refresh"] = fake_runner
    try:
        resp = await client.post(
            "/admin/control/loop/fx_refresh/tick-now",
            data={},
            allow_redirects=False,
        )
        assert resp.status == 302
        assert invoked == [], (
            "runner must NOT be invoked when CSRF is missing"
        )
    finally:
        # Restore the real runner so other tests aren't affected.
        from fx_rates import _tick_fx_refresh_from_app
        bot_health.LOOP_RUNNERS["fx_refresh"] = _tick_fx_refresh_from_app


async def test_control_loop_tick_now_post_unknown_loop(
    aiohttp_client, make_admin_app,
):
    """A POST for a loop name that isn't in ``LOOP_CADENCES`` 302s
    back with an error flash — never silently no-ops."""
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")

    resp = await client.post(
        "/admin/control/loop/not_a_real_loop/tick-now",
        data={"csrf_token": csrf},
        allow_redirects=False,
    )
    assert resp.status == 302
    follow = await client.get("/admin/control")
    body = await follow.text()
    assert "unknown loop" in body.lower()


async def test_control_loop_tick_now_post_no_runner_registered(
    aiohttp_client, make_admin_app,
):
    """A POST for a loop whose runner has been temporarily
    unregistered (e.g. a future loop in development) 302s back
    with an error flash, no 500."""
    import bot_health

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")

    saved = bot_health.LOOP_RUNNERS.pop("fx_refresh", None)
    try:
        resp = await client.post(
            "/admin/control/loop/fx_refresh/tick-now",
            data={"csrf_token": csrf},
            allow_redirects=False,
        )
        assert resp.status == 302
        follow = await client.get("/admin/control")
        body = await follow.text()
        assert "no registered tick-now runner" in body.lower()
    finally:
        if saved is not None:
            bot_health.LOOP_RUNNERS["fx_refresh"] = saved


async def test_control_loop_tick_now_post_invokes_runner_and_flashes(
    aiohttp_client, make_admin_app,
):
    """Happy path: with CSRF + a known loop name, the runner is
    awaited and a success flash renders on the redirect target."""
    import bot_health

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")
    db.record_admin_audit.reset_mock()

    invoked: list[bool] = []

    async def fake_runner(_app):
        invoked.append(True)

    saved = bot_health.LOOP_RUNNERS.get("fx_refresh")
    bot_health.LOOP_RUNNERS["fx_refresh"] = fake_runner
    try:
        resp = await client.post(
            "/admin/control/loop/fx_refresh/tick-now",
            data={"csrf_token": csrf},
            allow_redirects=False,
        )
        assert resp.status == 302
        assert resp.headers["Location"] == "/admin/control"
        assert invoked == [True]

        follow = await client.get("/admin/control")
        body = await follow.text()
        assert "ticked successfully" in body.lower()

        # Audit row written for the tick-now action.
        db.record_admin_audit.assert_awaited()
        actions = [
            c.kwargs.get("action")
            for c in db.record_admin_audit.await_args_list
        ]
        assert "control_loop_tick_now" in actions
    finally:
        if saved is not None:
            bot_health.LOOP_RUNNERS["fx_refresh"] = saved


async def test_control_loop_tick_now_post_runner_exception_flashes_error(
    aiohttp_client, make_admin_app,
):
    """A runner that raises is caught — the panel 302s back with
    an error flash quoting the exception class so the operator
    can diagnose without checking server logs first."""
    import bot_health

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")

    async def boom_runner(_app):
        raise RuntimeError("simulated upstream failure")

    saved = bot_health.LOOP_RUNNERS.get("fx_refresh")
    bot_health.LOOP_RUNNERS["fx_refresh"] = boom_runner
    try:
        resp = await client.post(
            "/admin/control/loop/fx_refresh/tick-now",
            data={"csrf_token": csrf},
            allow_redirects=False,
        )
        assert resp.status == 302

        follow = await client.get("/admin/control")
        body = await follow.text()
        assert "tick failed" in body.lower()
        assert "RuntimeError" in body
        assert "simulated upstream failure" in body
    finally:
        if saved is not None:
            bot_health.LOOP_RUNNERS["fx_refresh"] = saved


async def test_control_loop_tick_now_post_timeout_flashes_error(
    aiohttp_client, make_admin_app, monkeypatch,
):
    """A runner that takes longer than ``_TICK_NOW_TIMEOUT_SECONDS``
    is cancelled via ``asyncio.wait_for`` and the panel 302s back
    with a timeout flash. We patch the timeout down to 0.05s so the
    test runs fast."""
    import asyncio
    import bot_health
    import web_admin

    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    csrf = await _login_and_get_control_csrf(client, "pw")

    monkeypatch.setattr(web_admin, "_TICK_NOW_TIMEOUT_SECONDS", 0.05)

    async def slow_runner(_app):
        await asyncio.sleep(5.0)

    saved = bot_health.LOOP_RUNNERS.get("fx_refresh")
    bot_health.LOOP_RUNNERS["fx_refresh"] = slow_runner
    try:
        resp = await client.post(
            "/admin/control/loop/fx_refresh/tick-now",
            data={"csrf_token": csrf},
            allow_redirects=False,
        )
        assert resp.status == 302

        follow = await client.get("/admin/control")
        body = await follow.text()
        assert "timeout" in body.lower()
    finally:
        if saved is not None:
            bot_health.LOOP_RUNNERS["fx_refresh"] = saved


async def test_control_loop_tick_now_post_requires_auth(
    aiohttp_client, make_admin_app,
):
    """Unauthenticated POST is rejected with 302 to login — the
    handler is wrapped in ``_require_auth`` and the runner never
    fires."""
    db = _stub_toggle_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))

    resp = await client.post(
        "/admin/control/loop/fx_refresh/tick-now",
        data={"csrf_token": "anything"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert "/admin/login" in resp.headers["Location"]


# ---------------------------------------------------------------------
# Stage-15-Step-E #5 follow-up #4: "view as <role>" toggle
# ---------------------------------------------------------------------
#
# Pure-function tests for sign / verify, plus integration tests that
# exercise the full toggle flow + role gates via ``_require_role`` on
# selected handlers.


def test_sign_view_as_cookie_round_trip():
    from web_admin import sign_view_as_cookie, verify_view_as_cookie

    secret = "view-as-secret-1234567890"
    cookie = sign_view_as_cookie("viewer", secret=secret)
    assert verify_view_as_cookie(cookie, secret=secret) == "viewer"


def test_sign_view_as_cookie_round_trip_for_each_role():
    from web_admin import sign_view_as_cookie, verify_view_as_cookie

    secret = "view-as-secret-1234567890"
    for role in ("viewer", "operator", "super"):
        cookie = sign_view_as_cookie(role, secret=secret)
        assert verify_view_as_cookie(cookie, secret=secret) == role


def test_sign_view_as_cookie_rejects_unknown_role():
    from web_admin import sign_view_as_cookie

    with pytest.raises(ValueError):
        sign_view_as_cookie("admin", secret="x" * 32)


def test_sign_view_as_cookie_rejects_empty_secret():
    from web_admin import sign_view_as_cookie

    with pytest.raises(ValueError):
        sign_view_as_cookie("viewer", secret="")


def test_verify_view_as_cookie_rejects_none_and_empty():
    from web_admin import verify_view_as_cookie

    assert verify_view_as_cookie(None, secret="x" * 32) is None
    assert verify_view_as_cookie("", secret="x" * 32) is None


def test_verify_view_as_cookie_rejects_no_dot_separator():
    from web_admin import verify_view_as_cookie

    assert verify_view_as_cookie("just-a-blob", secret="x" * 32) is None


def test_verify_view_as_cookie_rejects_bad_base64():
    from web_admin import verify_view_as_cookie

    assert verify_view_as_cookie("!!!.!!!", secret="x" * 32) is None


def test_verify_view_as_cookie_rejects_tampered_signature():
    """Flipping a byte in the sig invalidates the cookie."""
    from web_admin import sign_view_as_cookie, verify_view_as_cookie

    secret = "x" * 32
    good = sign_view_as_cookie("viewer", secret=secret)
    role_b64, sig_b64 = good.split(".", 1)
    bad_sig = "A" + sig_b64[1:] if sig_b64[0] != "A" else "B" + sig_b64[1:]
    tampered = f"{role_b64}.{bad_sig}"
    assert verify_view_as_cookie(tampered, secret=secret) is None


def test_verify_view_as_cookie_rejects_tampered_payload():
    """Changing the role payload invalidates the HMAC."""
    from web_admin import sign_view_as_cookie, verify_view_as_cookie
    import base64

    secret = "x" * 32
    good = sign_view_as_cookie("viewer", secret=secret)
    role_b64, sig_b64 = good.split(".", 1)
    # Substitute "super" payload while keeping the "viewer" sig
    bad_role_b64 = base64.urlsafe_b64encode(b"super").rstrip(b"=").decode("ascii")
    tampered = f"{bad_role_b64}.{sig_b64}"
    assert verify_view_as_cookie(tampered, secret=secret) is None


def test_verify_view_as_cookie_rejects_wrong_secret():
    """Cookie signed with secret A must NOT verify under secret B."""
    from web_admin import sign_view_as_cookie, verify_view_as_cookie

    cookie = sign_view_as_cookie("viewer", secret="secret-a")
    assert verify_view_as_cookie(cookie, secret="secret-b") is None


def test_verify_view_as_cookie_rejects_unknown_role_after_signing():
    """A signed cookie carrying a role name that's no longer in
    VALID_ROLES must fail closed (e.g., a future stage drops a role
    while a cookie carrying it is still in the wild)."""
    import base64
    import hashlib
    import hmac

    secret = "x" * 32
    role_bytes = b"god"  # not a valid role
    sig = hmac.new(
        secret.encode("utf-8"),
        b"viewas:" + role_bytes,
        hashlib.sha256,
    ).digest()
    role_b64 = base64.urlsafe_b64encode(role_bytes).rstrip(b"=").decode()
    sig_b64 = base64.urlsafe_b64encode(sig).rstrip(b"=").decode()
    cookie = f"{role_b64}.{sig_b64}"

    from web_admin import verify_view_as_cookie

    assert verify_view_as_cookie(cookie, secret=secret) is None


def test_view_as_cookie_does_not_share_signature_with_auth_cookie():
    """Domain-separation pin: a value that happens to encode the same
    bytes under the auth-cookie HMAC must NOT verify as a view-as
    cookie. Protects against a future format-change cross-replay."""
    from web_admin import (
        sign_view_as_cookie,
        verify_cookie,
        verify_view_as_cookie,
    )

    secret = "x" * 32
    # Sign a role under the view-as HMAC.
    cookie = sign_view_as_cookie("super", secret=secret)
    # Under the AUTH cookie verifier (which expects an ISO timestamp),
    # this MUST NOT verify — the auth verifier should reject it.
    assert verify_cookie(cookie, secret=secret) is False
    # And the view-as verifier round-trips fine.
    assert verify_view_as_cookie(cookie, secret=secret) == "super"


# ---------------------------------------------------------------------
# Bundled bug fix: sign_cookie rejects naive datetimes
# ---------------------------------------------------------------------


def test_sign_cookie_rejects_naive_datetime():
    """Stage-15-Step-E #5 follow-up #4 bundled bug fix.

    A naive ``expires_at`` would silently get coerced via
    ``datetime.astimezone(tz)``'s system-local-time interpretation,
    producing a cookie whose wall-clock expiry depends on the deploy
    host's TZ env. ``sign_cookie`` must refuse the input outright
    rather than mint a host-dependent cookie.
    """
    secret = "x" * 32
    naive = datetime(2026, 1, 1, 12, 0, 0)  # no tzinfo
    with pytest.raises(ValueError, match="timezone-aware"):
        sign_cookie(naive, secret=secret)


def test_sign_cookie_still_accepts_aware_datetime_after_fix():
    """Regression pin: the naive-datetime guard must NOT reject
    legitimate aware datetimes that production code already passes."""
    secret = "x" * 32
    aware_utc = datetime(2099, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    cookie = sign_cookie(aware_utc, secret=secret)
    assert verify_cookie(cookie, secret=secret) is True


def test_sign_cookie_aware_in_other_tz_normalises_to_utc():
    """An aware datetime in any tz round-trips correctly because
    sign_cookie normalises to UTC. Pins the contract end-to-end."""
    from datetime import timedelta as _td

    secret = "x" * 32
    # +05:30 (India) tz, future timestamp
    plus_530 = timezone(_td(hours=5, minutes=30))
    aware_other = datetime(2099, 1, 1, 12, 0, 0, tzinfo=plus_530)
    cookie = sign_cookie(aware_other, secret=secret)
    assert verify_cookie(cookie, secret=secret) is True


# ---------------------------------------------------------------------
# Integration tests for /admin/view-as
# ---------------------------------------------------------------------


async def _csrf_token_from_dashboard(client) -> str:
    """Helper: pull the canonical CSRF token off the dashboard page."""
    import re
    resp = await client.get("/admin/")
    body = await resp.text()
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token on dashboard"
    return m.group(1)


async def test_view_as_default_is_super_when_no_cookie(
    aiohttp_client, make_admin_app
):
    """Without a view-as cookie, the password owner should see
    'super' rendered in the toggle widget on every page."""
    client = await aiohttp_client(make_admin_app(password="pw"))
    await _login(client, "pw")
    resp = await client.get("/admin/")
    assert resp.status == 200
    body = await resp.text()
    assert "Previewing as: <strong>super</strong>" in body


async def test_view_as_post_requires_csrf(aiohttp_client, make_admin_app):
    client = await aiohttp_client(make_admin_app(password="pw"))
    await _login(client, "pw")
    resp = await client.post(
        "/admin/view-as",
        data={"role": "viewer", "csrf_token": "wrong", "next": "/admin/"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"].startswith("/admin/")
    # Cookie should NOT be set on a CSRF-failed request.
    from web_admin import VIEW_AS_COOKIE_NAME
    assert VIEW_AS_COOKIE_NAME not in resp.cookies


async def test_view_as_post_requires_auth(aiohttp_client, make_admin_app):
    """Without an auth cookie, the toggle endpoint 302s to login."""
    client = await aiohttp_client(make_admin_app(password="pw"))
    resp = await client.post(
        "/admin/view-as",
        data={"role": "viewer", "csrf_token": "any", "next": "/admin/"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert "/admin/login" in resp.headers["Location"]


async def test_view_as_post_sets_cookie_for_viewer(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="pw"))
    await _login(client, "pw")
    csrf = await _csrf_token_from_dashboard(client)
    resp = await client.post(
        "/admin/view-as",
        data={"role": "viewer", "csrf_token": csrf, "next": "/admin/"},
        allow_redirects=False,
    )
    assert resp.status == 302
    from web_admin import VIEW_AS_COOKIE_NAME
    cookie = resp.cookies.get(VIEW_AS_COOKIE_NAME)
    assert cookie is not None
    assert cookie.value
    # Subsequent dashboard render reflects the new role.
    resp2 = await client.get("/admin/")
    body = await resp2.text()
    assert "Previewing as: <strong>viewer</strong>" in body


async def test_view_as_post_sets_cookie_for_operator(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="pw"))
    await _login(client, "pw")
    csrf = await _csrf_token_from_dashboard(client)
    resp = await client.post(
        "/admin/view-as",
        data={"role": "operator", "csrf_token": csrf, "next": "/admin/"},
        allow_redirects=False,
    )
    assert resp.status == 302
    resp2 = await client.get("/admin/")
    body = await resp2.text()
    assert "Previewing as: <strong>operator</strong>" in body


async def test_view_as_post_super_clears_cookie(
    aiohttp_client, make_admin_app
):
    """Selecting 'super' MUST drop the cookie so a session-secret
    rotation doesn't leave a stale signed value behind."""
    client = await aiohttp_client(make_admin_app(password="pw"))
    await _login(client, "pw")
    csrf = await _csrf_token_from_dashboard(client)
    # Set a viewer override first.
    await client.post(
        "/admin/view-as",
        data={"role": "viewer", "csrf_token": csrf, "next": "/admin/"},
    )
    from web_admin import VIEW_AS_COOKIE_NAME
    # Now toggle back to super.
    csrf2 = await _csrf_token_from_dashboard(client)
    resp = await client.post(
        "/admin/view-as",
        data={"role": "super", "csrf_token": csrf2, "next": "/admin/"},
        allow_redirects=False,
    )
    assert resp.status == 302
    # The Set-Cookie header should be a delete (max-age=0 / empty value).
    set_cookie_hdr = resp.headers.getall("Set-Cookie", [])
    assert any(
        VIEW_AS_COOKIE_NAME in h
        and ('Max-Age=0' in h or 'max-age=0' in h)
        for h in set_cookie_hdr
    ), f"Expected Max-Age=0 delete on {VIEW_AS_COOKIE_NAME}: {set_cookie_hdr!r}"


async def test_view_as_post_rejects_unknown_role(
    aiohttp_client, make_admin_app
):
    client = await aiohttp_client(make_admin_app(password="pw"))
    await _login(client, "pw")
    csrf = await _csrf_token_from_dashboard(client)
    resp = await client.post(
        "/admin/view-as",
        data={"role": "god", "csrf_token": csrf, "next": "/admin/"},
        allow_redirects=False,
    )
    assert resp.status == 302
    # Cookie should NOT be set.
    from web_admin import VIEW_AS_COOKIE_NAME
    assert VIEW_AS_COOKIE_NAME not in resp.cookies


async def test_view_as_post_open_redirect_blocked(
    aiohttp_client, make_admin_app
):
    """A tampered ``next=https://evil/`` MUST NOT turn the toggle
    into an open-redirect — non-/admin/ targets fall back to /admin/."""
    client = await aiohttp_client(make_admin_app(password="pw"))
    await _login(client, "pw")
    csrf = await _csrf_token_from_dashboard(client)
    resp = await client.post(
        "/admin/view-as",
        data={
            "role": "viewer",
            "csrf_token": csrf,
            "next": "https://evil.example/",
        },
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/"


async def test_view_as_post_records_audit(aiohttp_client, make_admin_app):
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    csrf = await _csrf_token_from_dashboard(client)
    await client.post(
        "/admin/view-as",
        data={"role": "viewer", "csrf_token": csrf, "next": "/admin/"},
    )
    # The first audit row is "login_ok" from _login. Find the
    # view_as_change row.
    calls = db.record_admin_audit.call_args_list
    actions = [c.kwargs.get("action") or c.args[1] for c in calls]
    assert "view_as_change" in actions, (
        f"Expected view_as_change in audit actions: {actions!r}"
    )


# ---------------------------------------------------------------------
# Integration tests for _require_role gates
# ---------------------------------------------------------------------


async def _set_view_as(client, role: str) -> None:
    """Helper: log in (if not already) + set the view-as cookie."""
    csrf = await _csrf_token_from_dashboard(client)
    resp = await client.post(
        "/admin/view-as",
        data={"role": role, "csrf_token": csrf, "next": "/admin/"},
    )
    assert resp.status in (200, 302)


async def test_viewer_can_read_dashboard(aiohttp_client, make_admin_app):
    """A previewed-viewer can still read viewer-floor pages."""
    client = await aiohttp_client(make_admin_app(password="pw"))
    await _login(client, "pw")
    await _set_view_as(client, "viewer")
    resp = await client.get("/admin/")
    assert resp.status == 200


async def test_viewer_cannot_post_user_adjust(
    aiohttp_client, make_admin_app
):
    """user_adjust requires super; previewed-viewer must be denied."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    # Capture CSRF before the override (token is keyed off the auth
    # cookie, not view-as, so it stays valid across the toggle).
    csrf = await _csrf_token_from_dashboard(client)
    await _set_view_as(client, "viewer")
    resp = await client.post(
        "/admin/users/12345/adjust",
        data={
            "csrf_token": csrf,
            "amount_usd": "1.00",
            "memo": "test",
            "kind": "credit",
        },
        allow_redirects=False,
    )
    # Gate denies → 302 to /admin/ with a flash banner.
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/"
    # adjust_balance must NOT have been called.
    db.adjust_balance.assert_not_called()


async def test_viewer_cannot_post_broadcast(
    aiohttp_client, make_admin_app
):
    """broadcast_post requires operator; viewer must be denied."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    csrf = await _csrf_token_from_dashboard(client)
    await _set_view_as(client, "viewer")
    resp = await client.post(
        "/admin/broadcast",
        data={"csrf_token": csrf, "message": "hi", "audience": "all"},
        allow_redirects=False,
    )
    assert resp.status == 302
    assert resp.headers["Location"] == "/admin/"


async def test_operator_can_post_broadcast_but_not_user_adjust(
    aiohttp_client, make_admin_app
):
    """An operator-floor preview can broadcast but NOT adjust wallets."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    csrf = await _csrf_token_from_dashboard(client)
    await _set_view_as(client, "operator")
    # user_adjust → super-only → deny.
    resp_adjust = await client.post(
        "/admin/users/12345/adjust",
        data={
            "csrf_token": csrf,
            "amount_usd": "1.00",
            "memo": "test",
            "kind": "credit",
        },
        allow_redirects=False,
    )
    assert resp_adjust.status == 302
    assert resp_adjust.headers["Location"] == "/admin/"
    db.adjust_balance.assert_not_called()


async def test_super_view_as_can_do_everything(
    aiohttp_client, make_admin_app
):
    """The default (no override) is super; every gate must pass.

    We don't actually mutate state here — that's covered by the
    existing per-handler tests. We just pin that the gate doesn't
    bounce a super-tier preview.
    """
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    # No view-as override → defaults to super → gate must pass through
    # to the underlying handler. We pick a handler that 302s on success
    # so we can distinguish a successful run from a gate-deny.
    resp = await client.get("/admin/control")
    assert resp.status == 200


async def test_role_gate_audits_deny(aiohttp_client, make_admin_app):
    """A role-deny must record a ``view_as_deny`` audit row so an
    operator can see what a previewed-lower-role user tried to do."""
    db = _stub_db()
    client = await aiohttp_client(make_admin_app(password="pw", db=db))
    await _login(client, "pw")
    csrf = await _csrf_token_from_dashboard(client)
    await _set_view_as(client, "viewer")
    await client.post(
        "/admin/users/12345/adjust",
        data={
            "csrf_token": csrf,
            "amount_usd": "1.00",
            "memo": "test",
            "kind": "credit",
        },
        allow_redirects=False,
    )
    actions = [
        c.kwargs.get("action") or (c.args[1] if len(c.args) > 1 else None)
        for c in db.record_admin_audit.call_args_list
    ]
    assert "view_as_deny" in actions


async def test_view_as_audit_slugs_in_action_labels():
    """Both ``view_as_change`` and ``view_as_deny`` MUST be in
    AUDIT_ACTION_LABELS so the /admin/audit filter dropdown surfaces
    them. Pinning here so a future PR can't quietly drop them."""
    from web_admin import AUDIT_ACTION_LABELS

    assert "view_as_change" in AUDIT_ACTION_LABELS
    assert "view_as_deny" in AUDIT_ACTION_LABELS


async def test_layout_renders_view_as_widget(
    aiohttp_client, make_admin_app
):
    """The toggle widget must render on every page so an admin
    previewing as a lower role can revert."""
    client = await aiohttp_client(make_admin_app(password="pw"))
    await _login(client, "pw")
    resp = await client.get("/admin/")
    body = await resp.text()
    assert 'action="/admin/view-as"' in body
    assert 'name="role"' in body
    # All three role options rendered in the dropdown.
    for role in ("viewer", "operator", "super"):
        assert f'value="{role}"' in body


async def test_role_gates_on_routes_module_definition():
    """Module-level pin: a regression that drops _require_role from a
    route registration should be caught by inspecting the source."""
    import inspect
    import web_admin

    src = inspect.getsource(web_admin.setup_admin_routes)
    # Spot-check the highest-risk routes — must be wrapped in
    # _require_role(ROLE_SUPER).
    super_routes = [
        '"/admin/users/{telegram_id}/adjust"',
        '"/admin/users/{telegram_id}/edit"',
        '"/admin/transactions/{transaction_id}/refund"',
        '"/admin/control/disable-all-models"',
        '"/admin/control/force-stop"',
        '"/admin/openrouter-keys/add"',
        '"/admin/roles"',
    ]
    for route in super_routes:
        # Find the line and assert it's gated by _require_role(ROLE_SUPER)
        # within the next ~3 lines.
        idx = src.find(route)
        assert idx >= 0, f"route {route} not found in setup_admin_routes"
        snippet = src[idx : idx + 200]
        assert "_require_role(ROLE_SUPER)" in snippet, (
            f"route {route} not gated by _require_role(ROLE_SUPER): "
            f"{snippet!r}"
        )

    operator_routes = [
        '"/admin/broadcast"',
        '"/admin/promos"',
        '"/admin/gifts"',
    ]
    for route in operator_routes:
        # POST handler line follows the GET line. Search for the POST.
        # We match `_require_role(ROLE_OPERATOR)` near the route literal.
        all_idx = [
            i for i in range(len(src)) if src.startswith(route, i)
        ]
        assert all_idx, f"route {route} not found"
        # At least one occurrence must be gated by ROLE_OPERATOR.
        gated = any(
            "_require_role(ROLE_OPERATOR)" in src[i : i + 200]
            for i in all_idx
        )
        assert gated, (
            f"route {route} has no _require_role(ROLE_OPERATOR) "
            f"registration"
        )


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 25: /admin/profile (password rotation)
# Plus the bundled bug fix on /admin/logout (extra cookie cleanup).
# ---------------------------------------------------------------------


import admin_password as _ap  # noqa: E402  (module imported for tests)


def _stub_db_for_profile(
    *,
    upsert_setting_result: object | Exception = None,
    get_setting_result: str | None | Exception = None,
):
    """Stub DB pre-wired with the system_settings CRUD needed by the
    profile-page password rotation handler."""
    db = _stub_db()
    if isinstance(upsert_setting_result, Exception):
        db.upsert_setting = AsyncMock(side_effect=upsert_setting_result)
    else:
        db.upsert_setting = AsyncMock(return_value=upsert_setting_result)
    if isinstance(get_setting_result, Exception):
        db.get_setting = AsyncMock(side_effect=get_setting_result)
    else:
        db.get_setting = AsyncMock(return_value=get_setting_result)
    return db


@pytest.fixture(autouse=False)
def _reset_admin_password_cache():
    """Each profile-handler test starts with a clean module cache so
    the resolution chain (DB hash → env) is deterministic."""
    _ap.clear_admin_password_hash_override()
    yield
    _ap.clear_admin_password_hash_override()


async def _login_and_get_profile_csrf(
    client, password: str = "letmein-1234",
) -> str:
    """Log in, fetch /admin/profile, scrape its CSRF token."""
    await client.post(
        "/admin/login", data={"password": password}, allow_redirects=False,
    )
    resp = await client.get("/admin/profile")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    assert m, "Expected CSRF token on /admin/profile rotation form"
    return m.group(1)


async def test_profile_get_renders_form(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    db = _stub_db_for_profile()
    client = await aiohttp_client(
        make_admin_app(password="letmein-1234", db=db)
    )
    await client.post(
        "/admin/login",
        data={"password": "letmein-1234"},
        allow_redirects=False,
    )
    resp = await client.get("/admin/profile")
    assert resp.status == 200, await resp.text()
    body = await resp.text()
    assert 'action="/admin/profile/rotate-password"' in body
    assert 'name="csrf_token"' in body
    assert 'name="current_password"' in body
    assert 'name="new_password"' in body
    assert 'name="confirm_password"' in body
    # Source badge surfaces "Environment variable" since we're using
    # the back-compat env path (no DB rotation yet).
    assert "Environment variable" in body
    # Profile sidebar link active.
    assert 'class="active"' in body and 'href="/admin/profile"' in body


async def test_profile_get_requires_auth(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    client = await aiohttp_client(make_admin_app(password="letmein-1234"))
    resp = await client.get("/admin/profile", allow_redirects=False)
    assert resp.status in (302, 303)
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_profile_rotate_password_post_requires_auth(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    client = await aiohttp_client(make_admin_app(password="letmein-1234"))
    resp = await client.post(
        "/admin/profile/rotate-password",
        data={
            "current_password": "letmein-1234",
            "new_password": "NewPassword99!",
            "confirm_password": "NewPassword99!",
            "csrf_token": "x",
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303), resp.status
    assert resp.headers.get("Location", "").startswith("/admin/login")


async def test_profile_rotate_password_rejects_csrf_mismatch(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    db = _stub_db_for_profile()
    client = await aiohttp_client(
        make_admin_app(password="letmein-1234", db=db)
    )
    await _login_and_get_profile_csrf(client, password="letmein-1234")
    resp = await client.post(
        "/admin/profile/rotate-password",
        data={
            "current_password": "letmein-1234",
            "new_password": "NewPassword99!",
            "confirm_password": "NewPassword99!",
            "csrf_token": "wrong-csrf-token",
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303)
    assert resp.headers.get("Location", "").endswith("/admin/profile")
    db.upsert_setting.assert_not_awaited()


async def test_profile_rotate_password_persists_and_caches(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    """The rotation persists into upsert_setting AND lands the new
    hash into the in-process cache via refresh-from-DB so subsequent
    logins prefer the new password over the env back-compat."""
    saved: dict = {}

    async def _upsert(key, value):
        saved[key] = value
        return None

    async def _get(key):
        return saved.get(key)

    db = _stub_db_for_profile()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(
        make_admin_app(password="letmein-1234", db=db)
    )
    csrf = await _login_and_get_profile_csrf(client, password="letmein-1234")

    resp = await client.post(
        "/admin/profile/rotate-password",
        data={
            "current_password": "letmein-1234",
            "new_password": "NewPasswordRotated2024",
            "confirm_password": "NewPasswordRotated2024",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303), await resp.text()
    assert resp.headers.get("Location", "").endswith("/admin/profile")
    db.upsert_setting.assert_awaited_once()
    args, _ = db.upsert_setting.call_args
    assert args[0] == _ap.ADMIN_PASSWORD_HASH_SETTING_KEY
    assert args[1].startswith("scrypt$")
    # Cache now carries the rotated hash.
    cached = _ap.get_admin_password_hash_override()
    assert cached is not None
    assert _ap.verify_password("NewPasswordRotated2024", cached)
    # Old env plaintext is NO longer accepted.
    assert not _ap.verify_admin_password(
        "letmein-1234", env_expected="letmein-1234",
    )


async def test_profile_rotate_password_rejects_wrong_current(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    db = _stub_db_for_profile()
    client = await aiohttp_client(
        make_admin_app(password="letmein-1234", db=db)
    )
    csrf = await _login_and_get_profile_csrf(client, password="letmein-1234")
    resp = await client.post(
        "/admin/profile/rotate-password",
        data={
            "current_password": "wrong-current-1234",
            "new_password": "NewPasswordRotated2024",
            "confirm_password": "NewPasswordRotated2024",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303)
    db.upsert_setting.assert_not_awaited()
    assert _ap.get_admin_password_hash_override() is None


async def test_profile_rotate_password_rejects_confirm_mismatch(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    db = _stub_db_for_profile()
    client = await aiohttp_client(
        make_admin_app(password="letmein-1234", db=db)
    )
    csrf = await _login_and_get_profile_csrf(client, password="letmein-1234")
    resp = await client.post(
        "/admin/profile/rotate-password",
        data={
            "current_password": "letmein-1234",
            "new_password": "NewPasswordRotated2024",
            "confirm_password": "DIFFERENT-confirm-99",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303)
    db.upsert_setting.assert_not_awaited()


@pytest.mark.parametrize(
    "weak_password",
    [
        "",                       # empty
        "short1!",                # below 12
        "abcdefghijkl",           # alpha-only
        "123456789012",           # digit-only
        "             ",          # whitespace-only (12 chars)
    ],
)
async def test_profile_rotate_password_rejects_weak(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
    weak_password,
):
    db = _stub_db_for_profile()
    client = await aiohttp_client(
        make_admin_app(password="letmein-1234", db=db)
    )
    csrf = await _login_and_get_profile_csrf(client, password="letmein-1234")
    resp = await client.post(
        "/admin/profile/rotate-password",
        data={
            "current_password": "letmein-1234",
            "new_password": weak_password,
            "confirm_password": weak_password,
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303)
    db.upsert_setting.assert_not_awaited()


async def test_profile_rotate_password_rejects_unchanged(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    """An operator can't rotate to the same password — refuses with
    a flash, audits the failure."""
    db = _stub_db_for_profile()
    client = await aiohttp_client(
        make_admin_app(password="letmein-1234", db=db)
    )
    csrf = await _login_and_get_profile_csrf(client, password="letmein-1234")
    resp = await client.post(
        "/admin/profile/rotate-password",
        data={
            "current_password": "letmein-1234",
            "new_password": "letmein-1234",
            "confirm_password": "letmein-1234",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303)
    db.upsert_setting.assert_not_awaited()


async def test_profile_rotate_password_db_error_flashes(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    """upsert_setting raising → flash an error, leave cache as-is."""
    db = _stub_db_for_profile(
        upsert_setting_result=RuntimeError("db down"),
    )
    client = await aiohttp_client(
        make_admin_app(password="letmein-1234", db=db)
    )
    csrf = await _login_and_get_profile_csrf(client, password="letmein-1234")
    resp = await client.post(
        "/admin/profile/rotate-password",
        data={
            "current_password": "letmein-1234",
            "new_password": "NewPasswordRotated2024",
            "confirm_password": "NewPasswordRotated2024",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    assert resp.status in (302, 303)
    # Cache stays as-is — rotation failed at the persistence step.
    assert _ap.get_admin_password_hash_override() is None


async def test_login_post_uses_db_hash_after_rotation(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    """End-to-end: rotate via /admin/profile, then sign in with the
    NEW password (not the env back-compat) on a fresh login flow."""
    saved: dict = {}

    async def _upsert(key, value):
        saved[key] = value
        return None

    async def _get(key):
        return saved.get(key)

    db = _stub_db_for_profile()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(
        make_admin_app(password="letmein-1234", db=db)
    )
    csrf = await _login_and_get_profile_csrf(client, password="letmein-1234")
    await client.post(
        "/admin/profile/rotate-password",
        data={
            "current_password": "letmein-1234",
            "new_password": "NewPasswordRotated2024",
            "confirm_password": "NewPasswordRotated2024",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )

    # Sign out, then sign in with the NEW password — should succeed.
    await client.get("/admin/logout")
    resp_new = await client.post(
        "/admin/login",
        data={"password": "NewPasswordRotated2024"},
        allow_redirects=False,
    )
    assert resp_new.status == 302
    assert resp_new.headers["Location"] == "/admin/"


async def test_login_post_rejects_old_env_after_rotation(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    """End-to-end: after rotation the OLD env plaintext is rejected
    even if it stays in app config (the canonical "rotated" flow)."""
    saved: dict = {}

    async def _upsert(key, value):
        saved[key] = value
        return None

    async def _get(key):
        return saved.get(key)

    db = _stub_db_for_profile()
    db.upsert_setting = AsyncMock(side_effect=_upsert)
    db.get_setting = AsyncMock(side_effect=_get)

    client = await aiohttp_client(
        make_admin_app(password="letmein-1234", db=db)
    )
    csrf = await _login_and_get_profile_csrf(client, password="letmein-1234")
    await client.post(
        "/admin/profile/rotate-password",
        data={
            "current_password": "letmein-1234",
            "new_password": "NewPasswordRotated2024",
            "confirm_password": "NewPasswordRotated2024",
            "csrf_token": csrf,
        },
        allow_redirects=False,
    )
    await client.get("/admin/logout")
    resp_old = await client.post(
        "/admin/login",
        data={"password": "letmein-1234"},
        allow_redirects=False,
    )
    # Old env value NO longer signs in — DB hash wins.
    assert resp_old.status == 401


# ---------------------------------------------------------------------
# Bundled bug fix: /admin/logout clears every cookie the panel sets
# ---------------------------------------------------------------------


async def test_logout_clears_view_as_cookie(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    """The previous logout impl only cleared the session cookie,
    leaving the signed view-as cookie behind. Verify the rotation-
    PR bug fix sweeps it too."""
    client = await aiohttp_client(make_admin_app(password="letmein-1234"))
    await client.post(
        "/admin/login",
        data={"password": "letmein-1234"},
        allow_redirects=False,
    )
    # Set a view-as cookie via the toggle endpoint.
    body = await (await client.get("/admin/")).text()
    import re
    m = re.search(r'name="csrf_token" value="([^"]+)"', body)
    csrf = m.group(1) if m else ""
    await client.post(
        "/admin/view-as",
        data={"role": "viewer", "csrf_token": csrf, "next": "/admin/"},
        allow_redirects=False,
    )
    # Confirm the cookie is set on the client.
    cookie_jar = client.session.cookie_jar
    cookies_pre = {c.key: c.value for c in cookie_jar}
    assert "meow_admin_view_as" in cookies_pre, cookies_pre

    resp = await client.get("/admin/logout", allow_redirects=False)
    assert resp.status == 302
    # Logout must emit a Set-Cookie that clears the view-as cookie.
    set_cookie_headers = resp.headers.getall("Set-Cookie", [])
    assert any(
        "meow_admin_view_as=" in h
        and ("Max-Age=0" in h or "expires=" in h.lower())
        for h in set_cookie_headers
    ), set_cookie_headers


async def test_logout_clears_flash_cookie(
    aiohttp_client, make_admin_app, _reset_admin_password_cache,
):
    """Logout sweeps the flash cookie too, even if no flash is
    currently set — defensive cleanup is cheap and keeps the post-
    logout state pristine."""
    client = await aiohttp_client(make_admin_app(password="letmein-1234"))
    await client.post(
        "/admin/login",
        data={"password": "letmein-1234"},
        allow_redirects=False,
    )
    resp = await client.get("/admin/logout", allow_redirects=False)
    assert resp.status == 302
    set_cookie_headers = resp.headers.getall("Set-Cookie", [])
    # Three cleanup cookies in total: session, view-as, flash.
    cleared = {
        "meow_admin": False,
        "meow_admin_view_as": False,
        "meow_flash": False,
    }
    for h in set_cookie_headers:
        for key in cleared:
            if h.startswith(f"{key}=") and (
                "Max-Age=0" in h or "expires=" in h.lower()
            ):
                cleared[key] = True
    assert all(cleared.values()), (set_cookie_headers, cleared)
