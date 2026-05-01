"""Tests for ``zarinpal_backfill`` (Stage-15-Step-E #8 follow-up #2).

Pins the browser-close backfill reaper:

* ``Database.list_pending_zarinpal_for_backfill`` SQL shape +
  return shape + age-window filtering + irr coercion edge cases.
* ``zarinpal_backfill.backfill_pending_once`` happy path,
  verify-rejected path, transport-error path, finalize-noop path,
  per-row crash isolation.
* Counter accounting for ops panels.
* ``record_loop_tick`` warns once on unknown loop names (bundled
  bug fix).
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

import database as database_module
import metrics
import zarinpal_backfill
from tests.test_database_queries import _PoolStub, _make_conn


# ---------------------------------------------------------------------
# Database.list_pending_zarinpal_for_backfill — SQL + return shape
# ---------------------------------------------------------------------


async def test_list_pending_zarinpal_for_backfill_pins_sql_shape():
    """The reaper's SELECT must:
      * scope by gateway='zarinpal' AND status='PENDING'
      * filter by min_age (lower bound) AND max_age (upper bound)
      * order oldest first
      * return the columns the reaper needs to verify+finalize
    """
    sample_row = {
        "transaction_id": 7,
        "gateway_invoice_id": "auth-abc",
        "telegram_id": 99,
        "amount_crypto_or_rial": 1234567,
        "amount_usd_credited": 1.50,
        "created_at": None,
    }
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[sample_row])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_pending_zarinpal_for_backfill(
        min_age_seconds=300, max_age_hours=23, limit=50,
    )
    assert rows == [
        {
            "transaction_id": 7,
            "gateway_invoice_id": "auth-abc",
            "telegram_id": 99,
            "locked_irr": 1234567,
            "locked_usd": 1.50,
            "created_at": None,
        }
    ]

    sql, *args = conn.fetch.await_args.args
    assert "FROM transactions" in sql
    assert "gateway = 'zarinpal'" in sql
    assert "status = 'PENDING'" in sql
    assert "NOW() - ($1 || ' seconds')::interval" in sql
    assert "NOW() - ($2 || ' hours')::interval" in sql
    assert "ORDER BY created_at" in sql
    assert "LIMIT $3" in sql
    assert args[0] == "300"
    assert args[1] == "23"
    assert args[2] == 50


async def test_list_pending_zarinpal_for_backfill_rejects_invalid_bounds():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError):
        await db.list_pending_zarinpal_for_backfill(
            min_age_seconds=0, max_age_hours=23,
        )
    with pytest.raises(ValueError):
        await db.list_pending_zarinpal_for_backfill(
            min_age_seconds=300, max_age_hours=0,
        )
    with pytest.raises(ValueError):
        await db.list_pending_zarinpal_for_backfill(
            min_age_seconds=300, max_age_hours=23, limit=0,
        )


async def test_list_pending_zarinpal_for_backfill_filters_invalid_irr():
    """A legacy row with NULL / non-finite / non-positive
    ``amount_crypto_or_rial`` must be filtered out — passing it to
    ``zarinpal.verify_payment`` would crash the reaper at runtime."""
    rows = [
        {
            "transaction_id": 1,
            "gateway_invoice_id": "ok",
            "telegram_id": 1,
            "amount_crypto_or_rial": 100000,
            "amount_usd_credited": 1.0,
            "created_at": None,
        },
        {
            "transaction_id": 2,
            "gateway_invoice_id": "null-irr",
            "telegram_id": 2,
            "amount_crypto_or_rial": None,
            "amount_usd_credited": 1.0,
            "created_at": None,
        },
        {
            "transaction_id": 3,
            "gateway_invoice_id": "zero-irr",
            "telegram_id": 3,
            "amount_crypto_or_rial": 0,
            "amount_usd_credited": 1.0,
            "created_at": None,
        },
        {
            "transaction_id": 4,
            "gateway_invoice_id": "neg-irr",
            "telegram_id": 4,
            "amount_crypto_or_rial": -100,
            "amount_usd_credited": 1.0,
            "created_at": None,
        },
    ]
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=rows)
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    result = await db.list_pending_zarinpal_for_backfill(
        min_age_seconds=300, max_age_hours=23,
    )
    assert [r["gateway_invoice_id"] for r in result] == ["ok"]


# ---------------------------------------------------------------------
# backfill_pending_once — happy path
# ---------------------------------------------------------------------


@pytest.fixture
def fake_bot():
    bot = MagicMock()
    bot.send_message = AsyncMock()
    return bot


@pytest.fixture(autouse=True)
def _reset_counters():
    zarinpal_backfill.reset_counters_for_tests()
    metrics.reset_loop_ticks_for_tests()
    yield
    zarinpal_backfill.reset_counters_for_tests()
    metrics.reset_loop_ticks_for_tests()


async def test_backfill_credits_settled_row(monkeypatch, fake_bot):
    """Happy path: list_pending returns one row, verify succeeds,
    finalize returns a credited dict, the user gets a DM, the
    counter ticks 'credited'."""
    rows = [
        {
            "transaction_id": 7,
            "gateway_invoice_id": "auth-7",
            "telegram_id": 99,
            "locked_irr": 1234560,
            "locked_usd": 5.0,
            "created_at": None,
        }
    ]
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "list_pending_zarinpal_for_backfill",
        AsyncMock(return_value=rows),
    )
    verify_mock = AsyncMock(return_value={"data": {"code": 100}})
    monkeypatch.setattr(
        zarinpal_backfill.zarinpal, "verify_payment", verify_mock,
    )
    finalize_mock = AsyncMock(return_value={
        "transaction_id": 7,
        "telegram_id": 99,
        "delta_credited": 5.0,
        "promo_bonus_credited": 0.0,
    })
    monkeypatch.setattr(
        zarinpal_backfill.db, "finalize_payment", finalize_mock,
    )
    monkeypatch.setattr(
        zarinpal_backfill.db, "record_admin_audit", AsyncMock(),
    )
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "get_user_language",
        AsyncMock(return_value="fa"),
    )

    n = await zarinpal_backfill.backfill_pending_once(
        fake_bot, min_age_seconds=300, max_age_hours=23,
    )
    assert n == 1
    verify_mock.assert_awaited_once_with("auth-7", 1234560)
    finalize_mock.assert_awaited_once_with("auth-7", 5.0)
    fake_bot.send_message.assert_awaited()
    counters = zarinpal_backfill.get_zarinpal_backfill_counters()
    assert counters["credited"] == 1
    assert counters["rows_examined"] == 1


async def test_backfill_skips_when_verify_rejects(monkeypatch, fake_bot):
    """Verify says 'no, this order is not settled' — DON'T call
    finalize, DON'T DM the user, increment 'verify_failed'."""
    rows = [
        {
            "transaction_id": 1,
            "gateway_invoice_id": "auth-1",
            "telegram_id": 7,
            "locked_irr": 100,
            "locked_usd": 1.0,
            "created_at": None,
        }
    ]
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "list_pending_zarinpal_for_backfill",
        AsyncMock(return_value=rows),
    )
    monkeypatch.setattr(
        zarinpal_backfill.zarinpal,
        "verify_payment",
        AsyncMock(
            side_effect=zarinpal_backfill.zarinpal.ZarinpalError(
                101, "not settled",
            )
        ),
    )
    finalize_mock = AsyncMock()
    monkeypatch.setattr(
        zarinpal_backfill.db, "finalize_payment", finalize_mock,
    )

    n = await zarinpal_backfill.backfill_pending_once(
        fake_bot, min_age_seconds=300, max_age_hours=23,
    )
    assert n == 0
    finalize_mock.assert_not_awaited()
    fake_bot.send_message.assert_not_awaited()
    counters = zarinpal_backfill.get_zarinpal_backfill_counters()
    assert counters["verify_failed"] == 1
    assert counters["credited"] == 0


async def test_backfill_handles_transport_error(monkeypatch, fake_bot):
    """Verify raises a transport error (asyncio.TimeoutError /
    aiohttp.ClientError / etc.) — don't crash, increment
    'transport_error', try again next tick."""
    import asyncio as _aio

    rows = [
        {
            "transaction_id": 1,
            "gateway_invoice_id": "auth-1",
            "telegram_id": 7,
            "locked_irr": 100,
            "locked_usd": 1.0,
            "created_at": None,
        }
    ]
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "list_pending_zarinpal_for_backfill",
        AsyncMock(return_value=rows),
    )
    monkeypatch.setattr(
        zarinpal_backfill.zarinpal,
        "verify_payment",
        AsyncMock(side_effect=_aio.TimeoutError()),
    )

    n = await zarinpal_backfill.backfill_pending_once(
        fake_bot, min_age_seconds=300, max_age_hours=23,
    )
    assert n == 0
    counters = zarinpal_backfill.get_zarinpal_backfill_counters()
    assert counters["transport_error"] == 1


async def test_backfill_finalize_noop_when_callback_raced(
    monkeypatch, fake_bot,
):
    """The user's redirect callback may race ahead of our reaper
    and finalize the row first. ``finalize_payment`` returns None
    in that case — we MUST NOT credit again or DM the user."""
    rows = [
        {
            "transaction_id": 1,
            "gateway_invoice_id": "auth-1",
            "telegram_id": 7,
            "locked_irr": 100,
            "locked_usd": 1.0,
            "created_at": None,
        }
    ]
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "list_pending_zarinpal_for_backfill",
        AsyncMock(return_value=rows),
    )
    monkeypatch.setattr(
        zarinpal_backfill.zarinpal,
        "verify_payment",
        AsyncMock(return_value={"data": {"code": 101}}),
    )
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "finalize_payment",
        AsyncMock(return_value=None),
    )

    n = await zarinpal_backfill.backfill_pending_once(
        fake_bot, min_age_seconds=300, max_age_hours=23,
    )
    assert n == 0
    fake_bot.send_message.assert_not_awaited()
    counters = zarinpal_backfill.get_zarinpal_backfill_counters()
    assert counters["finalize_noop"] == 1
    assert counters["credited"] == 0


async def test_backfill_continues_on_per_row_crash(
    monkeypatch, fake_bot,
):
    """A bug or bad data on row 1 must not abort processing of
    rows 2..N. The reaper's per-row safety net swallows the
    exception and continues."""
    rows = [
        {
            "transaction_id": 1,
            "gateway_invoice_id": "auth-bad",
            "telegram_id": 7,
            "locked_irr": 100,
            "locked_usd": 1.0,
            "created_at": None,
        },
        {
            "transaction_id": 2,
            "gateway_invoice_id": "auth-good",
            "telegram_id": 8,
            "locked_irr": 200,
            "locked_usd": 2.0,
            "created_at": None,
        },
    ]
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "list_pending_zarinpal_for_backfill",
        AsyncMock(return_value=rows),
    )

    async def selective_verify(authority, irr):
        if authority == "auth-bad":
            raise RuntimeError("simulated bad row")
        return {"data": {"code": 100}}

    monkeypatch.setattr(
        zarinpal_backfill.zarinpal,
        "verify_payment",
        AsyncMock(side_effect=selective_verify),
    )
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "finalize_payment",
        AsyncMock(return_value={
            "transaction_id": 2,
            "telegram_id": 8,
            "delta_credited": 2.0,
            "promo_bonus_credited": 0.0,
        }),
    )
    monkeypatch.setattr(
        zarinpal_backfill.db, "record_admin_audit", AsyncMock(),
    )
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "get_user_language",
        AsyncMock(return_value="fa"),
    )

    n = await zarinpal_backfill.backfill_pending_once(
        fake_bot, min_age_seconds=300, max_age_hours=23,
    )
    # Row 1 crashed and was bucketed as transport_error;
    # row 2 was credited successfully.
    assert n == 1
    counters = zarinpal_backfill.get_zarinpal_backfill_counters()
    assert counters["credited"] == 1
    assert counters["transport_error"] == 1


async def test_backfill_returns_zero_on_empty_list(monkeypatch, fake_bot):
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "list_pending_zarinpal_for_backfill",
        AsyncMock(return_value=[]),
    )
    n = await zarinpal_backfill.backfill_pending_once(
        fake_bot, min_age_seconds=300, max_age_hours=23,
    )
    assert n == 0


async def test_backfill_returns_zero_on_db_query_error(
    monkeypatch, fake_bot,
):
    """If the LIST query itself crashes (DB hiccup, bad SQL), the
    reaper logs and returns 0 — does NOT propagate so the loop
    keeps ticking."""
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "list_pending_zarinpal_for_backfill",
        AsyncMock(side_effect=RuntimeError("pool closed")),
    )
    n = await zarinpal_backfill.backfill_pending_once(
        fake_bot, min_age_seconds=300, max_age_hours=23,
    )
    assert n == 0


async def test_backfill_dm_failure_does_not_block_credit(
    monkeypatch, fake_bot,
):
    """The user's wallet must be credited even if Telegram is
    unreachable. A failed DM logs a warning; the credit still
    counts."""
    from aiogram.exceptions import TelegramForbiddenError

    rows = [
        {
            "transaction_id": 1,
            "gateway_invoice_id": "auth-1",
            "telegram_id": 7,
            "locked_irr": 100,
            "locked_usd": 1.0,
            "created_at": None,
        }
    ]
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "list_pending_zarinpal_for_backfill",
        AsyncMock(return_value=rows),
    )
    monkeypatch.setattr(
        zarinpal_backfill.zarinpal,
        "verify_payment",
        AsyncMock(return_value={"data": {"code": 100}}),
    )
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "finalize_payment",
        AsyncMock(return_value={
            "transaction_id": 1,
            "telegram_id": 7,
            "delta_credited": 1.0,
            "promo_bonus_credited": 0.0,
        }),
    )
    monkeypatch.setattr(
        zarinpal_backfill.db, "record_admin_audit", AsyncMock(),
    )
    monkeypatch.setattr(
        zarinpal_backfill.db,
        "get_user_language",
        AsyncMock(return_value="fa"),
    )
    fake_bot.send_message = AsyncMock(
        side_effect=TelegramForbiddenError(method=None, message="blocked"),
    )

    n = await zarinpal_backfill.backfill_pending_once(
        fake_bot, min_age_seconds=300, max_age_hours=23,
    )
    assert n == 1
    counters = zarinpal_backfill.get_zarinpal_backfill_counters()
    assert counters["credited"] == 1


# ---------------------------------------------------------------------
# _read_int_env defensive parsing
# ---------------------------------------------------------------------


def test_read_int_env_uses_default_for_unset(monkeypatch):
    monkeypatch.delenv("ZARINPAL_BACKFILL_INTERVAL_MIN", raising=False)
    assert zarinpal_backfill._read_int_env(
        "ZARINPAL_BACKFILL_INTERVAL_MIN", 5,
    ) == 5


def test_read_int_env_falls_back_for_garbage(monkeypatch, caplog):
    monkeypatch.setenv("ZARINPAL_BACKFILL_INTERVAL_MIN", "abc")
    with caplog.at_level("ERROR", logger="bot.zarinpal_backfill"):
        assert zarinpal_backfill._read_int_env(
            "ZARINPAL_BACKFILL_INTERVAL_MIN", 5,
        ) == 5


def test_read_int_env_clamps_below_minimum(monkeypatch, caplog):
    monkeypatch.setenv("ZARINPAL_BACKFILL_INTERVAL_MIN", "0")
    with caplog.at_level("ERROR", logger="bot.zarinpal_backfill"):
        assert zarinpal_backfill._read_int_env(
            "ZARINPAL_BACKFILL_INTERVAL_MIN", 5, minimum=1,
        ) == 1


# ---------------------------------------------------------------------
# Bundled bug fix: record_loop_tick warns once for unknown loop name
# ---------------------------------------------------------------------


def test_record_loop_tick_warns_once_for_unknown_name(caplog):
    """A typo'd loop name silently dropped the gauge from /metrics
    pre-fix. Now we log a WARN once per process so the typo is
    discoverable."""
    metrics.reset_loop_ticks_for_tests()
    with caplog.at_level("WARNING", logger="bot.metrics"):
        metrics.record_loop_tick("totally_typoed_loop_name")
        metrics.record_loop_tick("totally_typoed_loop_name")
        metrics.record_loop_tick("totally_typoed_loop_name")
    warn_messages = [
        r.message for r in caplog.records
        if r.levelno == logging.WARNING and "totally_typoed_loop_name" in r.message
    ]
    # Exactly one warning per unique unknown name.
    assert len(warn_messages) == 1
    assert "totally_typoed_loop_name" in warn_messages[0]
    # The tick was still stored — get_loop_last_tick must work for
    # ad-hoc inspection even when the gauge is dropped.
    assert metrics.get_loop_last_tick("totally_typoed_loop_name") is not None


def test_record_loop_tick_does_not_warn_for_known_name(caplog):
    """Registered loop names must not trigger the warning."""
    metrics.reset_loop_ticks_for_tests()
    with caplog.at_level("WARNING", logger="bot.metrics"):
        metrics.record_loop_tick("zarinpal_backfill")
        metrics.record_loop_tick("pending_reaper")
    warn_messages = [
        r.message for r in caplog.records
        if r.levelno == logging.WARNING
        and "_LOOP_METRIC_NAMES" in r.message
    ]
    assert warn_messages == []


def test_record_loop_tick_warns_once_per_distinct_unknown_name(caplog):
    """Two different typo'd names should produce two warnings."""
    metrics.reset_loop_ticks_for_tests()
    with caplog.at_level("WARNING", logger="bot.metrics"):
        metrics.record_loop_tick("typo_one")
        metrics.record_loop_tick("typo_two")
        metrics.record_loop_tick("typo_one")  # duplicate
    warn_messages = [
        r.message for r in caplog.records
        if r.levelno == logging.WARNING
        and "_LOOP_METRIC_NAMES" in r.message
    ]
    assert len(warn_messages) == 2


def test_zarinpal_backfill_is_in_loop_metric_names():
    """Pin: the new reaper's heartbeat must be exposed via the
    ``meowassist_zarinpal_backfill_last_run_epoch`` gauge."""
    assert "zarinpal_backfill" in metrics._LOOP_METRIC_NAMES


def test_reset_loop_ticks_for_tests_clears_warned_set():
    """``reset_loop_ticks_for_tests`` must clear the warned-name
    set so each test starts fresh — otherwise the second test in
    a run that calls record_loop_tick with the same unknown name
    would not see a warning."""
    metrics.reset_loop_ticks_for_tests()
    metrics.record_loop_tick("a_typo_name_xyz")
    assert "a_typo_name_xyz" in metrics._LOOP_TICK_UNKNOWN_NAMES_WARNED
    metrics.reset_loop_ticks_for_tests()
    assert "a_typo_name_xyz" not in metrics._LOOP_TICK_UNKNOWN_NAMES_WARNED
