"""Stage-9-Step-5 tests: background reaper for stuck PENDING transactions
+ ``mark_transaction_terminal`` FSM tightening."""

from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import database as database_module
import pending_expiration
from tests.test_database_queries import _PoolStub, _make_conn


# ---------------------------------------------------------------------
# Database.expire_stale_pending — SQL shape + return shape
# ---------------------------------------------------------------------


async def test_expire_stale_pending_pins_sql_shape():
    """The reaper's UPDATE must:
      * scope by status='PENDING' (NOT PARTIAL — those rows have a
        partial credit and may still upgrade to SUCCESS)
      * filter by NOW() - interval threshold so a fresh PENDING isn't
        wiped on the first tick after a deploy
      * use FOR UPDATE SKIP LOCKED so two reaper replicas don't double
        process the same row
      * RETURN the columns the caller needs to fire user notifications
    """
    sample_row = {
        "transaction_id": 7,
        "telegram_id": 99,
        "currency_used": "usdttrc20",
        "amount_usd_credited": 0.0,
        "gateway_invoice_id": "abc-99",
        "created_at": None,
    }
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[sample_row])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.expire_stale_pending(threshold_hours=24, limit=500)
    assert rows == [
        {
            "transaction_id": 7,
            "telegram_id": 99,
            "currency_used": "usdttrc20",
            "amount_usd_credited": 0.0,
            "gateway_invoice_id": "abc-99",
            "created_at": None,
        }
    ]

    sql, *args = conn.fetch.await_args.args
    assert "UPDATE transactions" in sql
    assert "SET status = 'EXPIRED'" in sql
    assert "WHERE status = 'PENDING'" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "RETURNING" in sql
    assert "NOW() - ($1 || ' hours')::interval" in sql
    assert args[0] == "24"
    assert args[1] == 500


async def test_expire_stale_pending_rejects_zero_threshold():
    """A zero or negative threshold would EXPIRE every PENDING row in
    the table on the first tick. ``ValueError`` at the API surface."""
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError):
        await db.expire_stale_pending(threshold_hours=0)
    with pytest.raises(ValueError):
        await db.expire_stale_pending(threshold_hours=-1)
    with pytest.raises(ValueError):
        await db.expire_stale_pending(threshold_hours=24, limit=0)


async def test_expire_stale_pending_returns_empty_on_no_rows():
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    assert await db.expire_stale_pending() == []


# ---------------------------------------------------------------------
# Bug-fix bundle: mark_transaction_terminal tightened FSM
# ---------------------------------------------------------------------


async def test_mark_transaction_terminal_rejects_non_terminal_status():
    """Pre-fix, ``mark_transaction_terminal("...", "PENDING")`` would
    fall through to a no-op UPDATE that still bumped completed_at.
    Now it raises at the API surface."""
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError, match="EXPIRED"):
        await db.mark_transaction_terminal("inv-1", "PENDING")
    with pytest.raises(ValueError):
        await db.mark_transaction_terminal("inv-1", "PARTIAL")
    with pytest.raises(ValueError):
        await db.mark_transaction_terminal("inv-1", "SUCCESS")


async def test_mark_transaction_terminal_update_guards_on_status_change():
    """The UPDATE WHERE clause now also checks ``status != $2`` —
    belt-and-suspenders against a same-status no-op slipping past the
    entry guard. Pin the SQL so a future refactor can't silently
    reintroduce the bug."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(
        return_value={
            "telegram_id": 7,
            "status": "PENDING",
            "currency_used": "usdttrc20",
            "amount_usd_credited": 0.0,
        }
    )
    conn.execute = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.mark_transaction_terminal("inv-1", "EXPIRED")
    assert result is not None
    assert result["previous_status"] == "PENDING"

    sql, *args = conn.execute.await_args.args
    assert "UPDATE transactions" in sql
    assert "AND status != $2" in sql


async def test_mark_transaction_terminal_returns_none_on_already_terminal():
    """Idempotence: a row already in EXPIRED must not be touched again."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(
        return_value={
            "telegram_id": 7,
            "status": "EXPIRED",
            "currency_used": "usdttrc20",
            "amount_usd_credited": 0.0,
        }
    )
    conn.execute = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.mark_transaction_terminal("inv-1", "EXPIRED")
    assert result is None
    conn.execute.assert_not_awaited()


def test_terminal_failure_statuses_constant():
    """The constant is the canonical allow-list — pin its membership
    so a future refactor doesn't accidentally let SUCCESS or PENDING
    in.

    Stage-12-Step-A: REFUNDED is no longer in this set. It moved to
    its own ``REFUND_STATUSES`` set with two dedicated entry points
    (:meth:`refund_transaction` for admin debits,
    :meth:`mark_payment_refunded_via_ipn` for gateway-side refunds)
    so the type system can't confuse the wallet-debit path with the
    no-debit path. Pin the new shape AND the new sibling constant
    to lock in the split.
    """
    assert database_module.Database.TERMINAL_FAILURE_STATUSES == frozenset(
        {"EXPIRED", "FAILED"}
    )
    assert database_module.Database.REFUND_STATUSES == frozenset({"REFUNDED"})
    # Sanity: the two sets are disjoint — a status string is *either*
    # a terminal-failure status *or* a refund status, never both.
    assert (
        database_module.Database.TERMINAL_FAILURE_STATUSES
        & database_module.Database.REFUND_STATUSES
    ) == frozenset()


async def test_mark_transaction_terminal_rejects_refunded_post_split():
    """Stage-12-Step-A bug-fix: passing ``REFUNDED`` to
    ``mark_transaction_terminal`` used to silently flip a row to
    REFUNDED *without* debiting the wallet — money-mint hazard for
    any future caller using this helper instead of
    :meth:`refund_transaction`. The split puts REFUNDED out of
    reach of this entry point entirely.
    """
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError, match="EXPIRED"):
        await db.mark_transaction_terminal("inv-1", "REFUNDED")


# ---------------------------------------------------------------------
# pending_expiration helpers
# ---------------------------------------------------------------------


def test_read_int_env_uses_default_on_unset(monkeypatch):
    monkeypatch.delenv("PENDING_TEST_VAR", raising=False)
    assert pending_expiration._read_int_env("PENDING_TEST_VAR", 15) == 15


def test_read_int_env_clamps_below_minimum(monkeypatch):
    monkeypatch.setenv("PENDING_TEST_VAR", "0")
    assert pending_expiration._read_int_env(
        "PENDING_TEST_VAR", 15, minimum=1
    ) == 1


def test_read_int_env_falls_back_on_garbage(monkeypatch):
    monkeypatch.setenv("PENDING_TEST_VAR", "not-an-int")
    assert pending_expiration._read_int_env("PENDING_TEST_VAR", 15) == 15


def test_read_int_env_parses_value(monkeypatch):
    monkeypatch.setenv("PENDING_TEST_VAR", "42")
    assert pending_expiration._read_int_env("PENDING_TEST_VAR", 15) == 42


# ---------------------------------------------------------------------
# expire_pending_once: full reap pass with mocked DB + bot
# ---------------------------------------------------------------------


async def test_expire_pending_once_no_rows_returns_zero(monkeypatch):
    """A clean ledger (no stuck PENDING) should be a clean no-op —
    no audit writes, no Telegram sends."""
    fake_db = MagicMock()
    fake_db.expire_stale_pending = AsyncMock(return_value=[])
    fake_db.record_admin_audit = AsyncMock(return_value=1)
    fake_db.get_user_language = AsyncMock(return_value="en")
    monkeypatch.setattr(pending_expiration, "db", fake_db)

    bot = MagicMock()
    bot.send_message = AsyncMock(return_value=None)

    n = await pending_expiration.expire_pending_once(
        bot, threshold_hours=24, batch_limit=10
    )
    assert n == 0
    fake_db.record_admin_audit.assert_not_awaited()
    bot.send_message.assert_not_awaited()


async def test_expire_pending_once_audits_and_notifies(monkeypatch):
    """Each expired row must produce one audit entry + one user ping."""
    expired = [
        {
            "transaction_id": 1,
            "telegram_id": 11,
            "currency_used": "usdttrc20",
            "amount_usd_credited": 0.0,
            "gateway_invoice_id": "inv-1",
            "created_at": "2026-01-01T00:00:00+00:00",
        },
        {
            "transaction_id": 2,
            "telegram_id": 22,
            "currency_used": "btc",
            "amount_usd_credited": 0.0,
            "gateway_invoice_id": "inv-2",
            "created_at": "2026-01-01T00:00:00+00:00",
        },
    ]
    fake_db = MagicMock()
    fake_db.expire_stale_pending = AsyncMock(return_value=expired)
    fake_db.record_admin_audit = AsyncMock(return_value=1)
    fake_db.get_user_language = AsyncMock(return_value="en")
    monkeypatch.setattr(pending_expiration, "db", fake_db)

    bot = MagicMock()
    bot.send_message = AsyncMock(return_value=None)

    n = await pending_expiration.expire_pending_once(
        bot, threshold_hours=24, batch_limit=10
    )
    assert n == 2
    assert fake_db.record_admin_audit.await_count == 2
    audit_calls = fake_db.record_admin_audit.await_args_list
    assert {c.args[1] for c in audit_calls} == {"payment_expired"}
    assert {c.args[0] for c in audit_calls} == {"reaper"}
    assert bot.send_message.await_count == 2


async def test_expire_pending_once_audit_failure_does_not_block_loop(
    monkeypatch, caplog
):
    """An audit-write blowup must NOT block the next row's notification —
    the EXPIRED commit already happened, the audit is only a logging
    gap."""
    expired = [
        {
            "transaction_id": 1,
            "telegram_id": 11,
            "currency_used": "usdttrc20",
            "amount_usd_credited": 0.0,
            "gateway_invoice_id": "inv-1",
            "created_at": None,
        },
    ]
    fake_db = MagicMock()
    fake_db.expire_stale_pending = AsyncMock(return_value=expired)
    fake_db.record_admin_audit = AsyncMock(side_effect=RuntimeError("audit DB blip"))
    fake_db.get_user_language = AsyncMock(return_value="en")
    monkeypatch.setattr(pending_expiration, "db", fake_db)

    bot = MagicMock()
    bot.send_message = AsyncMock(return_value=None)

    with caplog.at_level("ERROR", logger="bot.pending_expiration"):
        n = await pending_expiration.expire_pending_once(
            bot, threshold_hours=24, batch_limit=10
        )
    assert n == 1
    bot.send_message.assert_awaited_once()


async def test_expire_pending_once_telegram_block_swallowed(monkeypatch):
    """A user blocking the bot is exactly the user whose abandoned
    invoice we're closing. Swallow the exception and continue."""
    from aiogram.exceptions import TelegramForbiddenError

    expired = [
        {
            "transaction_id": 1,
            "telegram_id": 11,
            "currency_used": "usdttrc20",
            "amount_usd_credited": 0.0,
            "gateway_invoice_id": "inv-1",
            "created_at": None,
        },
        {
            "transaction_id": 2,
            "telegram_id": 22,
            "currency_used": "btc",
            "amount_usd_credited": 0.0,
            "gateway_invoice_id": "inv-2",
            "created_at": None,
        },
    ]
    fake_db = MagicMock()
    fake_db.expire_stale_pending = AsyncMock(return_value=expired)
    fake_db.record_admin_audit = AsyncMock(return_value=1)
    fake_db.get_user_language = AsyncMock(return_value="en")
    monkeypatch.setattr(pending_expiration, "db", fake_db)

    bot = MagicMock()
    bot.send_message = AsyncMock(
        side_effect=[
            TelegramForbiddenError(method=MagicMock(), message="bot was blocked"),
            None,
        ]
    )

    n = await pending_expiration.expire_pending_once(
        bot, threshold_hours=24, batch_limit=10
    )
    # Both rows expire; the first user's send fails silently, the second
    # still gets notified.
    assert n == 2
    assert bot.send_message.await_count == 2


async def test_expire_pending_once_db_failure_returns_zero(monkeypatch):
    """``expire_stale_pending`` blowing up must not propagate — the
    reaper retries on the next tick."""
    fake_db = MagicMock()
    fake_db.expire_stale_pending = AsyncMock(side_effect=RuntimeError("pool blip"))
    fake_db.record_admin_audit = AsyncMock(return_value=1)
    fake_db.get_user_language = AsyncMock(return_value="en")
    monkeypatch.setattr(pending_expiration, "db", fake_db)

    bot = MagicMock()
    bot.send_message = AsyncMock(return_value=None)

    n = await pending_expiration.expire_pending_once(
        bot, threshold_hours=24, batch_limit=10
    )
    assert n == 0
    bot.send_message.assert_not_awaited()


# ---------------------------------------------------------------------
# start_pending_expiration_task: cancellation must be clean
# ---------------------------------------------------------------------


async def test_reaper_task_cancellation_propagates_cleanly(monkeypatch):
    """The reaper exits cleanly on .cancel() so the main shutdown
    path's ``await expiration_task`` doesn't hang."""
    fake_db = MagicMock()
    fake_db.expire_stale_pending = AsyncMock(return_value=[])
    fake_db.record_admin_audit = AsyncMock(return_value=1)
    fake_db.get_user_language = AsyncMock(return_value="en")
    monkeypatch.setattr(pending_expiration, "db", fake_db)
    # Make the reaper wake every 100ms so the test runs fast.
    monkeypatch.setenv("PENDING_EXPIRATION_INTERVAL_MIN", "1")

    bot = MagicMock()

    task = pending_expiration.start_pending_expiration_task(bot)
    await asyncio.sleep(0)  # let it start
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()
