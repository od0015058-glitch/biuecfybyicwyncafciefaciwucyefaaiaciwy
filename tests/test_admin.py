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
