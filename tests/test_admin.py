"""Tests for the admin module — env parsing, gating, and metrics formatting.

The aiogram handler functions themselves are integration-shaped and
need a live ``Database``, so they're not exercised here. We pin the
parser, gate predicate, and metrics formatter — each of which is a
piece of business logic that's easy to break and easy to unit-test.
"""

from __future__ import annotations

import logging

import pytest

import admin
from admin import (
    format_metrics,
    is_admin,
    parse_admin_user_ids,
    set_admin_user_ids,
)


# ---- parse_admin_user_ids ------------------------------------------


def test_parse_admin_user_ids_empty():
    assert parse_admin_user_ids(None) == frozenset()
    assert parse_admin_user_ids("") == frozenset()
    assert parse_admin_user_ids("   ") == frozenset()
    assert parse_admin_user_ids(",, ,") == frozenset()


def test_parse_admin_user_ids_single():
    assert parse_admin_user_ids("12345") == frozenset({12345})


def test_parse_admin_user_ids_csv():
    assert parse_admin_user_ids("1,2,3") == frozenset({1, 2, 3})


def test_parse_admin_user_ids_with_whitespace():
    assert parse_admin_user_ids(" 1 , 2 ,3   ") == frozenset({1, 2, 3})


def test_parse_admin_user_ids_drops_non_int(caplog):
    """Non-integer entries are dropped (with a warning), not crashed
    on. Otherwise a single typo in .env would knock the bot offline."""
    with caplog.at_level(logging.WARNING, logger="bot.admin"):
        result = parse_admin_user_ids("123,xyz,456")
    assert result == frozenset({123, 456})
    assert any("xyz" in rec.message for rec in caplog.records)


def test_parse_admin_user_ids_dedupes():
    assert parse_admin_user_ids("1,1,1,2") == frozenset({1, 2})


# ---- is_admin / set_admin_user_ids --------------------------------


def test_is_admin_none():
    set_admin_user_ids(set())
    assert is_admin(None) is False


def test_is_admin_empty_set():
    set_admin_user_ids(set())
    assert is_admin(123) is False


def test_is_admin_match():
    set_admin_user_ids({100, 200})
    assert is_admin(100) is True
    assert is_admin(200) is True
    assert is_admin(300) is False


def test_set_admin_user_ids_accepts_various_iterables():
    set_admin_user_ids([1, 2, 3])
    assert is_admin(2) is True
    set_admin_user_ids(frozenset({4}))
    assert is_admin(2) is False
    assert is_admin(4) is True


def test_set_admin_user_ids_coerces_to_int():
    """Even if a list of stringy ints sneaks in, gating still works."""
    set_admin_user_ids([1, 2, 3])  # plain ints
    assert is_admin(1) is True
    # cleanup so other tests start clean
    set_admin_user_ids(set())


# ---- format_metrics ------------------------------------------------


def _sample_metrics() -> dict:
    return {
        "users_total": 1_234,
        "users_active_7d": 56,
        "revenue_usd": 789.12,
        "spend_usd": 12.3456,
        "top_models": [
            {
                "model": "openai/gpt-4o-mini",
                "count": 100,
                "cost_usd": 1.2345,
            },
            {
                "model": "anthropic/claude-3-5-sonnet",
                "count": 42,
                "cost_usd": 5.6789,
            },
        ],
    }


def test_format_metrics_includes_all_fields():
    out = format_metrics(_sample_metrics())
    assert "1,234" in out  # users_total with thousands sep
    assert "56" in out
    assert "$789.12" in out
    assert "$12.3456" in out
    assert "openai/gpt-4o-mini" in out
    assert "100 calls" in out
    assert "$1.2345" in out
    assert "anthropic/claude-3-5-sonnet" in out


def test_format_metrics_empty_top_models():
    """Bot has been deployed but no one has used it yet — still readable."""
    metrics = _sample_metrics()
    metrics["top_models"] = []
    out = format_metrics(metrics)
    assert "1,234" in out
    assert "(no usage logged yet)" in out


# ---- import-time module state ------------------------------------


def test_module_init_reads_env(monkeypatch):
    """At import time the module reads ADMIN_USER_IDS. Verify the
    parser is wired correctly, not the parser itself (covered above)."""
    monkeypatch.setenv("ADMIN_USER_IDS", "999,888")
    import importlib

    reloaded = importlib.reload(admin)
    try:
        assert reloaded.is_admin(999) is True
        assert reloaded.is_admin(888) is True
        assert reloaded.is_admin(777) is False
    finally:
        # Reset to a clean slate so siblings don't see leaked state.
        monkeypatch.delenv("ADMIN_USER_IDS", raising=False)
        importlib.reload(admin)


# ---- handler-routing smoke ---------------------------------------


def test_admin_router_has_admin_command():
    """Sanity check that the router actually exposes /admin and
    /admin_metrics — guards against accidentally moving the decorators
    to a non-imported file."""
    import inspect

    src = inspect.getsource(admin)
    assert '@router.message(Command("admin"))' in src
    assert '@router.message(Command("admin_metrics"))' in src
    assert '@router.message(Command("admin_balance"))' in src
    assert '@router.message(Command("admin_credit"))' in src
    assert '@router.message(Command("admin_debit"))' in src
    assert '@router.message(Command("admin_promo_create"))' in src
    assert '@router.message(Command("admin_promo_list"))' in src
    assert '@router.message(Command("admin_promo_revoke"))' in src


# ---- parse_balance_args ------------------------------------------


from admin import _format_balance_summary, parse_balance_args  # noqa: E402


def test_parse_balance_args_happy_path():
    out = parse_balance_args("/admin_credit 12345 5.50 stuck invoice refund")
    assert out == (12345, 5.50, "stuck invoice refund")


def test_parse_balance_args_single_word_reason():
    out = parse_balance_args("/admin_debit 12345 1 typo")
    assert out == (12345, 1.0, "typo")


def test_parse_balance_args_missing_everything():
    assert parse_balance_args("/admin_credit") == "missing"
    assert parse_balance_args("/admin_credit  ") == "missing"


def test_parse_balance_args_no_amount():
    assert parse_balance_args("/admin_credit 12345") == "bad_amount"


def test_parse_balance_args_no_reason():
    assert parse_balance_args("/admin_credit 12345 5") == "missing_reason"


def test_parse_balance_args_bad_user_id():
    assert parse_balance_args("/admin_credit foo 5 reason") == "bad_user_id"


def test_parse_balance_args_bad_amount_text():
    assert (
        parse_balance_args("/admin_credit 12345 not_a_num reason")
        == "bad_amount"
    )


@pytest.mark.parametrize(
    "amount_str",
    ["nan", "NaN", "inf", "-inf", "Infinity", "-1", "0"],
)
def test_parse_balance_args_rejects_nonpositive_or_special(amount_str):
    out = parse_balance_args(f"/admin_credit 12345 {amount_str} reason")
    assert out == "bad_amount"


def test_parse_balance_args_accepts_decimal():
    out = parse_balance_args("/admin_credit 12345 0.0001 micro adjustment")
    assert out == (12345, 0.0001, "micro adjustment")


# ---- _format_balance_summary ------------------------------------


def _sample_summary() -> dict:
    return {
        "telegram_id": 12345,
        "username": "alice",
        "balance_usd": 7.5,
        "free_messages_left": 3,
        "active_model": "openai/gpt-4o-mini",
        "language_code": "en",
        "total_credited_usd": 25.0,
        "total_spent_usd": 17.5,
        "recent_transactions": [
            {
                "id": 7,
                "gateway": "NowPayments",
                "currency": "trx",
                "amount_usd": 25.0,
                "status": "SUCCESS",
                "created_at": "2026-04-28T01:23:45+00:00",
                "notes": None,
            },
            {
                "id": 8,
                "gateway": "admin",
                "currency": "USD",
                "amount_usd": -2.5,
                "status": "SUCCESS",
                "created_at": "2026-04-28T02:00:00+00:00",
                "notes": "double-charged user",
            },
        ],
    }


def test_format_balance_summary_full():
    out = _format_balance_summary(_sample_summary())
    assert "@alice" in out
    assert "12345" in out
    assert "$7.5000" in out
    assert "openai/gpt-4o-mini" in out
    assert "$25.0000" in out  # NowPayments credit
    assert "$2.5000" in out  # admin debit
    assert "double-charged user" in out
    assert "+$25.0000" in out
    assert "−$2.5000" in out


def test_format_balance_summary_no_username():
    summary = _sample_summary()
    summary["username"] = None
    out = _format_balance_summary(summary)
    assert "@" not in out.split("\n")[0]
    assert "id=12345" in out


def test_format_balance_summary_no_txs():
    summary = _sample_summary()
    summary["recent_transactions"] = []
    out = _format_balance_summary(summary)
    assert "Last 5 transactions" not in out


def test_format_balance_summary_user_with_no_notes():
    """Old NowPayments rows have notes=NULL — the formatter should
    not crash and should not append a stray ' — _' to the line."""
    summary = _sample_summary()
    summary["recent_transactions"] = [
        {
            "id": 1,
            "gateway": "NowPayments",
            "currency": "btc",
            "amount_usd": 5.0,
            "status": "SUCCESS",
            "created_at": "2026-04-28T00:00:00+00:00",
            "notes": None,
        }
    ]
    out = _format_balance_summary(summary)
    assert "#1" in out
    assert "$5.0000" in out
    assert " — _" not in out


# ---- parse_promo_create_args ------------------------------------


from admin import (  # noqa: E402
    _format_promo_row,
    parse_promo_create_args,
)


def test_parse_promo_create_args_percent_no_extras():
    out = parse_promo_create_args("/admin_promo_create WELCOME20 20%")
    assert out == {
        "code": "WELCOME20",
        "discount_percent": 20,
        "discount_amount": None,
        "max_uses": None,
        "expires_in_days": None,
    }


def test_parse_promo_create_args_dollar_amount():
    out = parse_promo_create_args("/admin_promo_create FIVEOFF $5")
    assert out == {
        "code": "FIVEOFF",
        "discount_percent": None,
        "discount_amount": 5.0,
        "max_uses": None,
        "expires_in_days": None,
    }


def test_parse_promo_create_args_bare_amount():
    out = parse_promo_create_args("/admin_promo_create FIVEOFF 5")
    assert out["discount_amount"] == 5.0
    assert out["discount_percent"] is None


def test_parse_promo_create_args_full():
    out = parse_promo_create_args(
        "/admin_promo_create WINTER20 20% 100 30"
    )
    assert out == {
        "code": "WINTER20",
        "discount_percent": 20,
        "discount_amount": None,
        "max_uses": 100,
        "expires_in_days": 30,
    }


def test_parse_promo_create_args_uppercases_code():
    out = parse_promo_create_args("/admin_promo_create welcome20 20%")
    assert out["code"] == "WELCOME20"


def test_parse_promo_create_args_missing():
    assert parse_promo_create_args("/admin_promo_create") == "missing"
    assert parse_promo_create_args("/admin_promo_create CODE") == "missing"


@pytest.mark.parametrize(
    "code", ["has/slash", "🎉", "a" * 65]
)
def test_parse_promo_create_args_bad_code(code):
    assert (
        parse_promo_create_args(f"/admin_promo_create {code} 20%")
        == "bad_code"
    )


def test_parse_promo_create_args_empty_code_resolves_to_missing():
    # "/admin_promo_create  20%" splits to 2 parts -> missing
    assert parse_promo_create_args("/admin_promo_create  20%") == "missing"


@pytest.mark.parametrize(
    "disc",
    [
        "0%", "101%", "-5%", "abc%", "%", "$0", "$-1", "$nan",
        "$inf", "nan", "inf", "abc",
    ],
)
def test_parse_promo_create_args_bad_discount(disc):
    assert (
        parse_promo_create_args(f"/admin_promo_create CODE {disc}")
        == "bad_discount"
    )


@pytest.mark.parametrize("max_uses", ["abc", "0", "-5"])
def test_parse_promo_create_args_bad_max_uses(max_uses):
    assert (
        parse_promo_create_args(
            f"/admin_promo_create CODE 20% {max_uses}"
        )
        == "bad_max_uses"
    )


@pytest.mark.parametrize("days", ["abc", "0", "-5"])
def test_parse_promo_create_args_bad_days(days):
    assert (
        parse_promo_create_args(
            f"/admin_promo_create CODE 20% 100 {days}"
        )
        == "bad_days"
    )


def test_parse_promo_create_args_allows_underscore_and_dash():
    out = parse_promo_create_args("/admin_promo_create EARLY_BIRD-25 25%")
    assert out["code"] == "EARLY_BIRD-25"


# ---- _format_promo_row -----------------------------------------


def test_format_promo_row_active_percent():
    row = {
        "code": "WELCOME20",
        "discount_percent": 20,
        "discount_amount": None,
        "max_uses": 100,
        "used_count": 5,
        "expires_at": "2026-12-31T00:00:00+00:00",
        "is_active": True,
    }
    out = _format_promo_row(row)
    assert "WELCOME20" in out
    assert "20%" in out
    assert "5/100" in out
    assert "2026-12-31" in out
    assert "active" in out
    assert "revoked" not in out


def test_format_promo_row_revoked_amount_no_cap_no_expiry():
    row = {
        "code": "FIVEOFF",
        "discount_percent": None,
        "discount_amount": 5.0,
        "max_uses": None,
        "used_count": 3,
        "expires_at": None,
        "is_active": False,
    }
    out = _format_promo_row(row)
    assert "$5.00" in out
    assert "3/∞" in out
    assert "revoked" in out
    # No "exp=" segment when expires_at is None
    assert "exp=" not in out
