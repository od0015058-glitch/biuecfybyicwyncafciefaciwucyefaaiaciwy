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


def test_format_metrics_omits_pending_line_when_zero():
    """Stage-9-Step-9: zero PENDING rows = no spend signal worth a
    line in the metrics digest. The dashboard tile still renders
    "0", but the Telegram-side digest stays terse."""
    metrics = _sample_metrics()
    metrics["pending_payments_count"] = 0
    metrics["pending_payments_oldest_age_hours"] = None
    out = format_metrics(metrics)
    assert "Pending payments" not in out


def test_format_metrics_includes_pending_line_when_non_zero():
    """Stage-9-Step-9: surface count + oldest age so the admin can
    distinguish "5 fresh invoices waiting for IPN" from "5 invoices
    stuck for 23h about to be reaped"."""
    metrics = _sample_metrics()
    metrics["pending_payments_count"] = 5
    metrics["pending_payments_oldest_age_hours"] = 12.4
    out = format_metrics(metrics)
    assert "Pending payments" in out
    assert "5" in out
    assert "12.4h" in out


def test_format_metrics_pending_without_age_renders_count_only():
    """Defensive: if a future caller passes count>0 with no age (a
    half-populated dict from an upgrade-in-flight), still render
    the count instead of crashing."""
    metrics = _sample_metrics()
    metrics["pending_payments_count"] = 2
    metrics["pending_payments_oldest_age_hours"] = None
    out = format_metrics(metrics)
    assert "Pending payments" in out
    assert "2" in out


def test_format_metrics_renders_over_threshold_subline():
    """Stage-15-Step-D #5 bundled fix: the
    ``pending_payments_over_threshold_count`` sub-line must surface
    on the Telegram-side ``/admin_metrics`` digest so it matches
    what ``dashboard.html`` already shows. Stage-12-Step-B added
    the field but only wired it into the web template — operators
    on Telegram saw "5 pending" with no signal that 3 were already
    past the proactive-DM threshold and triggering separate alerts.
    """
    metrics = _sample_metrics()
    metrics["pending_payments_count"] = 5
    metrics["pending_payments_oldest_age_hours"] = 6.2
    metrics["pending_payments_over_threshold_count"] = 3
    metrics["pending_alert_threshold_hours"] = 2
    out = format_metrics(metrics)
    assert "Pending payments" in out
    assert "5" in out
    # The sub-line must mention both the count and the threshold so
    # the operator can correlate with the proactive-DM threshold.
    assert "3 over 2h" in out


def test_format_metrics_omits_over_threshold_when_zero():
    """When zero rows are over the threshold, the sub-line is
    suppressed — keeps the digest terse on the happy path."""
    metrics = _sample_metrics()
    metrics["pending_payments_count"] = 4
    metrics["pending_payments_oldest_age_hours"] = 0.5
    metrics["pending_payments_over_threshold_count"] = 0
    metrics["pending_alert_threshold_hours"] = 2
    out = format_metrics(metrics)
    assert "Pending payments" in out
    assert "4" in out
    assert "over 2h" not in out


def test_format_metrics_skips_over_threshold_when_keys_missing():
    """Backwards compatibility: a caller passing a pre-Stage-12-B
    metrics dict (no over-threshold keys) must still get a clean
    rendering. Defensive ``rows.get(...)`` covers the gap."""
    metrics = _sample_metrics()
    metrics["pending_payments_count"] = 7
    metrics["pending_payments_oldest_age_hours"] = 3.0
    # Deliberately omit pending_payments_over_threshold_count
    # and pending_alert_threshold_hours.
    out = format_metrics(metrics)
    assert "Pending payments" in out
    assert "7" in out
    assert "over" not in out


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
    # Stage-9-Step-7 unified format_usd: minus is the ASCII
    # ``-`` placed BEFORE the dollar sign (matching accounting
    # convention).
    assert "-$2.5000" in out


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


@pytest.mark.parametrize(
    "code",
    [
        # Persian (Eastern Arabic) digit instead of ASCII '1'.
        "PROMO\u06f1",
        # Roman numeral V — visually identical to ASCII 'V' but a
        # distinct Unicode codepoint (U+2164).
        "CODE\u2164",
        # Cyrillic 'А' (U+0410) homoglyph of Latin 'A'.
        "PROM\u041e",
        # Superscript 2 — ``str.isalnum`` returns True for it.
        "GIFT\u00b2",
        # Pure non-ASCII alnum (Persian "PROMO1").
        "\u067e\u0631\u0648\u0645\u0648\u06f1",
    ],
)
def test_parse_promo_create_args_rejects_unicode_alnum(code):
    """ASCII-only guard: ``str.isalnum`` returns True for Persian
    digits / Roman numerals / Cyrillic homoglyphs / superscripts.
    Pre-fix these stored fine but no user typing on a standard
    keyboard could ever match the row, so the admin's promo
    silently never redeemed. Post-fix the parser rejects them at
    creation time so the admin sees the ``bad_code`` error and
    re-types the code in plain ASCII.
    """
    assert (
        parse_promo_create_args(f"/admin_promo_create {code} 20%")
        == "bad_code"
    )


def test_parse_promo_create_args_accepts_full_ascii_alnum():
    """Regression pin: every ASCII letter + digit + the two reserved
    punctuation characters must still pass through unchanged. Without
    this the new ``isascii()`` guard could regress to rejecting
    legitimate codes.
    """
    out = parse_promo_create_args(
        "/admin_promo_create ABCdef-123_XYZ 10%"
    )
    assert out["code"] == "ABCDEF-123_XYZ"


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


# ---- _escape_md (Markdown escaping) -----------------------------


from admin import _escape_md, parse_broadcast_args  # noqa: E402


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("", ""),
        (None, ""),
        ("plain text", "plain text"),
        ("stuck_invoice", "stuck\\_invoice"),
        ("the *real* one", "the \\*real\\* one"),
        ("`code` block", "\\`code\\` block"),
        ("[link](url)", "\\[link](url)"),
        ("a_b*c`d[e", "a\\_b\\*c\\`d\\[e"),
        # Persian text without reserved chars passes through.
        ("سلام دنیا", "سلام دنیا"),
        # Persian + an underscore
        ("بازپرداخت_فاکتور", "بازپرداخت\\_فاکتور"),
    ],
)
def test_escape_md(raw, expected):
    assert _escape_md(raw) == expected


def test_format_balance_summary_escapes_note():
    # Regression test for Devin Review finding on PR #50: a stored
    # note like "stuck_invoice" used to crash the admin reply.
    summary = {
        "telegram_id": 123,
        "username": "alice",
        "balance_usd": 5.0,
        "free_messages_left": 0,
        "active_model": "openai/gpt-4o",
        "language_code": "fa",
        "total_credited_usd": 10.0,
        "total_spent_usd": 5.0,
        "recent_transactions": [
            {
                "id": 1,
                "gateway": "admin",
                "amount_usd": 1.0,
                "status": "SUCCESS",
                "notes": "stuck_invoice *fix*",
            }
        ],
    }
    out = _format_balance_summary(summary)
    # Underscore + asterisks must be escaped, not stripped.
    assert "stuck\\_invoice \\*fix\\*" in out
    # Original raw form must NOT appear unescaped.
    assert " stuck_invoice " not in out


# ---- parse_broadcast_args ---------------------------------------


def test_parse_broadcast_args_simple():
    out = parse_broadcast_args("/admin_broadcast hello world")
    assert out == {"only_active_days": None, "text": "hello world"}


def test_parse_broadcast_args_with_active_filter():
    out = parse_broadcast_args(
        "/admin_broadcast --active=30 maintenance tonight"
    )
    assert out == {
        "only_active_days": 30,
        "text": "maintenance tonight",
    }


def test_parse_broadcast_args_preserves_newlines():
    out = parse_broadcast_args(
        "/admin_broadcast line1\nline2\nline3"
    )
    assert out == {
        "only_active_days": None,
        "text": "line1\nline2\nline3",
    }


def test_parse_broadcast_args_missing():
    assert parse_broadcast_args("/admin_broadcast") == "missing"
    assert parse_broadcast_args("/admin_broadcast    ") == "missing"
    # --active without text is still missing
    assert parse_broadcast_args("/admin_broadcast --active=7") == "missing"


@pytest.mark.parametrize("bad", ["abc", "0", "-5"])
def test_parse_broadcast_args_bad_active(bad):
    assert (
        parse_broadcast_args(f"/admin_broadcast --active={bad} hello")
        == "bad_active"
    )


def test_parse_broadcast_args_too_long():
    body = "x" * 5000
    assert (
        parse_broadcast_args(f"/admin_broadcast {body}") == "too_long"
    )


def test_parse_broadcast_args_unicode_persian():
    out = parse_broadcast_args("/admin_broadcast سلام به همه")
    assert out == {"only_active_days": None, "text": "سلام به همه"}


def test_parse_broadcast_args_active_too_large():
    """Stage-8-Part-6 guard: ``--active=9999999999`` would overflow
    PG's 32-bit interval column downstream. Reject up-front with a
    friendly error key instead of letting a generic "DB query failed"
    banner surface.
    """
    from admin import _BROADCAST_ACTIVE_DAYS_MAX

    # Exactly at the cap is allowed.
    out = parse_broadcast_args(
        f"/admin_broadcast --active={_BROADCAST_ACTIVE_DAYS_MAX} hello"
    )
    assert isinstance(out, dict)
    assert out["only_active_days"] == _BROADCAST_ACTIVE_DAYS_MAX

    # One past the cap is rejected.
    assert (
        parse_broadcast_args(
            f"/admin_broadcast --active={_BROADCAST_ACTIVE_DAYS_MAX + 1} hi"
        )
        == "active_too_large"
    )

    # A real admin typo ("let's use all of history") also rejected.
    assert (
        parse_broadcast_args("/admin_broadcast --active=9999999999 hi")
        == "active_too_large"
    )


def test_admin_broadcast_router_decorator_present():
    import inspect
    src = inspect.getsource(admin)
    assert '@router.message(Command("admin_broadcast"))' in src


# ---------------------------------------------------------------------
# Bug-fix sweep: ``max_uses`` / ``[days]`` overflow on
# ``/admin_promo_create``.
#
# Mirrors the equivalent fix on the web admin's ``parse_promo_form``.
# Pre-fix the parser had no upper bound on either ``max_uses`` or the
# ``[days]`` argument. PG INTEGER overflows at 2_147_483_647, and PG
# ``interval`` arithmetic overflows somewhere below that depending on
# the unit. Both crashes surfaced as the generic
# ``"DB write failed — see logs."`` reply — the admin couldn't tell
# the cause was a fat-finger.
# ---------------------------------------------------------------------
def test_parse_promo_create_args_max_uses_at_cap_accepted():
    from admin import _PROMO_MAX_USES_CAP
    out = parse_promo_create_args(
        f"/admin_promo_create FOO 10% {_PROMO_MAX_USES_CAP}"
    )
    assert isinstance(out, dict)
    assert out["max_uses"] == _PROMO_MAX_USES_CAP


def test_parse_promo_create_args_max_uses_above_cap_rejected():
    from admin import _PROMO_MAX_USES_CAP
    out = parse_promo_create_args(
        f"/admin_promo_create FOO 10% {_PROMO_MAX_USES_CAP + 1}"
    )
    assert out == "max_uses_too_large"


def test_parse_promo_create_args_max_uses_pg_int_overflow_rejected():
    """Direct repro of the original crash: 2_147_483_648 would
    overflow PG INTEGER on insert."""
    out = parse_promo_create_args(
        "/admin_promo_create FOO 10% 2147483648"
    )
    assert out == "max_uses_too_large"


def test_parse_promo_create_args_days_at_cap_accepted():
    from admin import _PROMO_EXPIRES_IN_DAYS_CAP
    out = parse_promo_create_args(
        f"/admin_promo_create FOO 10% 100 {_PROMO_EXPIRES_IN_DAYS_CAP}"
    )
    assert isinstance(out, dict)
    assert out["expires_in_days"] == _PROMO_EXPIRES_IN_DAYS_CAP


def test_parse_promo_create_args_days_above_cap_rejected():
    from admin import _PROMO_EXPIRES_IN_DAYS_CAP
    out = parse_promo_create_args(
        f"/admin_promo_create FOO 10% 100 {_PROMO_EXPIRES_IN_DAYS_CAP + 1}"
    )
    assert out == "days_too_large"


def test_promo_create_err_text_has_new_keys():
    """The dispatcher uses the error key as a dict lookup; a missing
    key would silently render an unrouted error. Pin both new keys
    have hand-written messages."""
    from admin import _PROMO_CREATE_ERR_TEXT
    assert "max_uses_too_large" in _PROMO_CREATE_ERR_TEXT
    assert "days_too_large" in _PROMO_CREATE_ERR_TEXT
    assert "1,000,000" in _PROMO_CREATE_ERR_TEXT["max_uses_too_large"]
    assert "36,500" in _PROMO_CREATE_ERR_TEXT["days_too_large"]


# ---------------------------------------------------------------------
# Stage-15-Step-E #5: admin role grant / revoke / list
# ---------------------------------------------------------------------


def test_admin_router_has_role_commands():
    """Same guard as the credit/promo handlers — a refactor that moves
    these decorators must not silently drop them from the router."""
    import inspect

    src = inspect.getsource(admin)
    assert '@router.message(Command("admin_role_grant"))' in src
    assert '@router.message(Command("admin_role_revoke"))' in src
    assert '@router.message(Command("admin_role_list"))' in src


def test_admin_hub_text_lists_role_commands():
    """The /admin hub message advertises every command the router
    exposes; out-of-sync entries make the surface invisible to a
    new admin reading the hub."""
    text = admin._ADMIN_HUB_TEXT
    assert "/admin_role_grant" in text
    assert "/admin_role_revoke" in text
    assert "/admin_role_list" in text


def test_format_role_row_renders_full_metadata():
    rendered = admin._format_role_row({
        "telegram_id": 777,
        "role": "operator",
        "granted_at": "2026-04-30T12:34:56",
        "granted_by": 1,
        "notes": "trusted",
    })
    assert "`777`" in rendered
    assert "*operator*" in rendered
    assert "2026-04-30 12:34:56" in rendered
    assert "by `1`" in rendered
    assert "_trusted_" in rendered


def test_format_role_row_omits_optional_fields_when_missing():
    rendered = admin._format_role_row({
        "telegram_id": 888,
        "role": "viewer",
        "granted_at": "2026-04-30T12:34:56",
        "granted_by": None,
        "notes": None,
    })
    assert "*viewer*" in rendered
    assert "by `" not in rendered
    assert "_" not in rendered.split("granted")[1]


def test_format_role_row_escapes_markdown_in_notes():
    """A free-form ``notes`` value containing reserved Markdown
    characters (``_ * ` [``) must round-trip through ``_escape_md``
    so the message render doesn't 400 on Telegram's parser."""
    rendered = admin._format_role_row({
        "telegram_id": 1,
        "role": "viewer",
        "granted_at": "2026-04-30T00:00:00",
        "granted_by": None,
        "notes": "stuck_invoice *VIP*",
    })
    assert r"stuck\_invoice" in rendered
    assert r"\*VIP\*" in rendered


# ---- handler smoke tests via mocked Message + db ------------------


class _FakeMessage:
    """Minimal aiogram-Message-lookalike covering the surface our
    new handlers actually use (``message.text``, ``message.from_user.id``,
    ``message.answer``)."""

    def __init__(self, text: str, user_id: int | None = 1):
        self.text = text
        self.from_user = type("_U", (), {"id": user_id})() if user_id is not None else None
        self.replies: list[tuple[str, dict]] = []

    async def answer(self, text, **kwargs):
        self.replies.append((text, kwargs))


@pytest.mark.asyncio
async def test_admin_role_grant_no_op_for_non_admins(monkeypatch):
    """Non-admins (env-list miss) get the same silent no-op every other
    admin command does — consistent surface, no leak of the admin
    namespace existence."""
    set_admin_user_ids(set())
    msg = _FakeMessage("/admin_role_grant 777 viewer", user_id=999)
    await admin.admin_role_grant(msg)
    assert msg.replies == []


@pytest.mark.asyncio
async def test_admin_role_grant_rejects_invalid_role(monkeypatch):
    set_admin_user_ids({1})
    monkeypatch.setattr(
        admin.db, "set_admin_role", _failing_assert_not_called(),
    )
    msg = _FakeMessage("/admin_role_grant 777 admin", user_id=1)
    await admin.admin_role_grant(msg)
    assert any("not a valid role" in r[0] for r in msg.replies)


@pytest.mark.asyncio
async def test_admin_role_grant_rejects_non_int_user_id(monkeypatch):
    set_admin_user_ids({1})
    monkeypatch.setattr(
        admin.db, "set_admin_role", _failing_assert_not_called(),
    )
    msg = _FakeMessage("/admin_role_grant abc viewer", user_id=1)
    await admin.admin_role_grant(msg)
    assert any("not a valid Telegram id" in r[0] for r in msg.replies)


@pytest.mark.asyncio
async def test_admin_role_grant_writes_through_to_db(monkeypatch):
    from unittest.mock import AsyncMock

    set_admin_user_ids({1})
    set_role = AsyncMock(return_value="viewer")
    record_audit = AsyncMock(return_value=1)
    monkeypatch.setattr(admin.db, "set_admin_role", set_role)
    monkeypatch.setattr(admin.db, "record_admin_audit", record_audit)

    msg = _FakeMessage(
        "/admin_role_grant 777 viewer trusted op",
        user_id=1,
    )
    await admin.admin_role_grant(msg)

    set_role.assert_awaited_once_with(
        777, "viewer", granted_by=1, notes="trusted op",
    )
    record_audit.assert_awaited()
    assert any("Granted role *viewer*" in r[0] for r in msg.replies)


@pytest.mark.asyncio
async def test_admin_role_grant_db_audit_failure_does_not_block_success(
    monkeypatch,
):
    """The audit insert is best-effort. A transient ``admin_audit_log``
    write failure must not regress the user-visible success message
    (otherwise the operator retries and double-grants)."""
    from unittest.mock import AsyncMock

    set_admin_user_ids({1})
    set_role = AsyncMock(return_value="viewer")
    record_audit = AsyncMock(side_effect=RuntimeError("audit blip"))
    monkeypatch.setattr(admin.db, "set_admin_role", set_role)
    monkeypatch.setattr(admin.db, "record_admin_audit", record_audit)

    msg = _FakeMessage("/admin_role_grant 777 viewer", user_id=1)
    await admin.admin_role_grant(msg)

    set_role.assert_awaited_once()
    assert any("Granted role *viewer*" in r[0] for r in msg.replies)


@pytest.mark.asyncio
async def test_admin_role_revoke_no_op_for_non_admins():
    set_admin_user_ids(set())
    msg = _FakeMessage("/admin_role_revoke 777", user_id=999)
    await admin.admin_role_revoke(msg)
    assert msg.replies == []


@pytest.mark.asyncio
async def test_admin_role_revoke_reports_no_row_when_not_found(monkeypatch):
    from unittest.mock import AsyncMock

    set_admin_user_ids({1})
    monkeypatch.setattr(
        admin.db, "delete_admin_role", AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        admin.db, "record_admin_audit", AsyncMock(return_value=1),
    )

    msg = _FakeMessage("/admin_role_revoke 777", user_id=1)
    await admin.admin_role_revoke(msg)
    assert any("nothing to revoke" in r[0] for r in msg.replies)


@pytest.mark.asyncio
async def test_admin_role_revoke_reports_success_when_deleted(monkeypatch):
    from unittest.mock import AsyncMock

    set_admin_user_ids({1})
    monkeypatch.setattr(
        admin.db, "delete_admin_role", AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        admin.db, "record_admin_audit", AsyncMock(return_value=1),
    )

    msg = _FakeMessage("/admin_role_revoke 777", user_id=1)
    await admin.admin_role_revoke(msg)
    assert any("Revoked DB-tracked role" in r[0] for r in msg.replies)


@pytest.mark.asyncio
async def test_admin_role_list_renders_rows(monkeypatch):
    from unittest.mock import AsyncMock

    set_admin_user_ids({1})
    monkeypatch.setattr(
        admin.db, "list_admin_roles",
        AsyncMock(return_value=[
            {
                "telegram_id": 777, "role": "operator",
                "granted_at": "2026-04-30T12:00:00",
                "granted_by": 1, "notes": "trusted",
            },
        ]),
    )

    msg = _FakeMessage("/admin_role_list", user_id=1)
    await admin.admin_role_list(msg)
    body = msg.replies[0][0]
    assert "Admin roles" in body
    assert "`777`" in body
    assert "*operator*" in body


@pytest.mark.asyncio
async def test_admin_role_list_handles_empty_table(monkeypatch):
    from unittest.mock import AsyncMock

    set_admin_user_ids({1})
    monkeypatch.setattr(
        admin.db, "list_admin_roles", AsyncMock(return_value=[]),
    )

    msg = _FakeMessage("/admin_role_list", user_id=1)
    await admin.admin_role_list(msg)
    assert any("No DB-tracked admin roles" in r[0] for r in msg.replies)


def _failing_assert_not_called():
    """Helper: an AsyncMock that fails the test if it gets awaited.

    Used to pin "validation rejects the input *before* hitting the DB"
    so a future regression that flips the order of checks fails this
    test loudly rather than silently writing a poisoned row."""
    from unittest.mock import AsyncMock

    mock = AsyncMock(
        side_effect=AssertionError("DB was called for an invalid input"),
    )
    return mock
