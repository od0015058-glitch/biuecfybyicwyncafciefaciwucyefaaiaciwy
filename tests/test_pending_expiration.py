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


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 9 — DB-backed PENDING_EXPIRATION_HOURS
# override.
# ---------------------------------------------------------------------


import logging  # noqa: E402  (kept local to the new section)


@pytest.fixture(autouse=True)
def _reset_expiration_hours_override():
    """Each test starts from a clean override slot.

    Mirrors :func:`test_bot_health_alert._reset_module_state`: a test
    that monkeypatches env should not see a leaked override from a
    previous test, and a leaked override should not survive into
    later tests in the same module.
    """
    pending_expiration.reset_expiration_hours_override_for_tests()
    yield
    pending_expiration.reset_expiration_hours_override_for_tests()


class _StubDB:
    """Minimal DB stub mirroring ``test_bot_health_alert._StubDB``.

    Only exposes ``get_setting`` / ``upsert_setting`` /
    ``delete_setting`` plus optional ``raise_on_*`` switches so the
    fail-soft branches of
    :func:`pending_expiration.refresh_expiration_hours_override_from_db`
    can be exercised without spinning up Postgres.
    """

    def __init__(
        self,
        initial=None,
        *,
        raise_on_get=None,
        raise_on_upsert=None,
        raise_on_delete=None,
    ):
        self.rows = dict(initial or {})
        self.raise_on_get = raise_on_get
        self.raise_on_upsert = raise_on_upsert
        self.raise_on_delete = raise_on_delete
        self.upserts = []
        self.deletes = []

    async def get_setting(self, key):
        if self.raise_on_get is not None:
            raise self.raise_on_get
        return self.rows.get(key)

    async def upsert_setting(self, key, value):
        if self.raise_on_upsert is not None:
            raise self.raise_on_upsert
        self.upserts.append((key, value))
        self.rows[key] = value

    async def delete_setting(self, key):
        if self.raise_on_delete is not None:
            raise self.raise_on_delete
        self.deletes.append(key)
        return self.rows.pop(key, None) is not None


def test_coerce_expiration_hours_accepts_int():
    assert pending_expiration._coerce_expiration_hours(24) == 24


def test_coerce_expiration_hours_accepts_string():
    assert pending_expiration._coerce_expiration_hours("48") == 48


def test_coerce_expiration_hours_strips_string():
    assert pending_expiration._coerce_expiration_hours("  72  ") == 72


def test_coerce_expiration_hours_rejects_bool():
    # True is an int subclass — must be rejected explicitly so a
    # malformed DB row of "true"/"True" can't shrink the window to
    # 1 hour and EXPIRE most of the legit-but-slow PENDING invoices.
    assert pending_expiration._coerce_expiration_hours(True) is None
    assert pending_expiration._coerce_expiration_hours(False) is None


def test_coerce_expiration_hours_rejects_zero():
    assert pending_expiration._coerce_expiration_hours(0) is None


def test_coerce_expiration_hours_rejects_negative():
    assert pending_expiration._coerce_expiration_hours(-5) is None


def test_coerce_expiration_hours_rejects_above_max():
    assert (
        pending_expiration._coerce_expiration_hours(
            pending_expiration.EXPIRATION_HOURS_OVERRIDE_MAXIMUM + 1
        )
        is None
    )


def test_coerce_expiration_hours_accepts_minimum():
    assert (
        pending_expiration._coerce_expiration_hours(
            pending_expiration.EXPIRATION_HOURS_MINIMUM
        )
        == pending_expiration.EXPIRATION_HOURS_MINIMUM
    )


def test_coerce_expiration_hours_accepts_max():
    assert (
        pending_expiration._coerce_expiration_hours(
            pending_expiration.EXPIRATION_HOURS_OVERRIDE_MAXIMUM
        )
        == pending_expiration.EXPIRATION_HOURS_OVERRIDE_MAXIMUM
    )


def test_coerce_expiration_hours_rejects_non_numeric():
    assert pending_expiration._coerce_expiration_hours("notanint") is None


def test_coerce_expiration_hours_rejects_float_string():
    assert pending_expiration._coerce_expiration_hours("24.5") is None


def test_coerce_expiration_hours_rejects_blank():
    assert pending_expiration._coerce_expiration_hours("") is None
    assert pending_expiration._coerce_expiration_hours("   ") is None


def test_coerce_expiration_hours_rejects_other_types():
    assert pending_expiration._coerce_expiration_hours(None) is None
    assert pending_expiration._coerce_expiration_hours(24.0) is None
    assert pending_expiration._coerce_expiration_hours([24]) is None
    assert pending_expiration._coerce_expiration_hours({"v": 24}) is None


def test_set_expiration_hours_override_applies():
    pending_expiration.set_expiration_hours_override(48)
    assert pending_expiration.get_expiration_hours_override() == 48


def test_set_expiration_hours_override_idempotent():
    pending_expiration.set_expiration_hours_override(48)
    pending_expiration.set_expiration_hours_override(48)
    assert pending_expiration.get_expiration_hours_override() == 48


def test_set_expiration_hours_override_rejects_zero():
    with pytest.raises(ValueError):
        pending_expiration.set_expiration_hours_override(0)


def test_set_expiration_hours_override_rejects_negative():
    with pytest.raises(ValueError):
        pending_expiration.set_expiration_hours_override(-1)


def test_set_expiration_hours_override_rejects_above_max():
    with pytest.raises(ValueError):
        pending_expiration.set_expiration_hours_override(
            pending_expiration.EXPIRATION_HOURS_OVERRIDE_MAXIMUM + 1
        )


def test_set_expiration_hours_override_rejects_bool():
    with pytest.raises(ValueError):
        pending_expiration.set_expiration_hours_override(True)  # type: ignore[arg-type]


def test_clear_expiration_hours_override_returns_true_when_set():
    pending_expiration.set_expiration_hours_override(48)
    assert pending_expiration.clear_expiration_hours_override() is True
    assert pending_expiration.get_expiration_hours_override() is None


def test_clear_expiration_hours_override_returns_false_when_unset():
    assert pending_expiration.clear_expiration_hours_override() is False


def test_get_expiration_hours_override_returns_none_by_default():
    assert pending_expiration.get_expiration_hours_override() is None


def test_get_pending_expiration_hours_prefers_override(monkeypatch):
    """Override beats env beats default."""
    monkeypatch.setenv("PENDING_EXPIRATION_HOURS", "12")
    pending_expiration.set_expiration_hours_override(72)
    assert pending_expiration.get_pending_expiration_hours() == 72


def test_get_pending_expiration_hours_falls_through_to_env_when_cleared(
    monkeypatch,
):
    monkeypatch.setenv("PENDING_EXPIRATION_HOURS", "12")
    pending_expiration.set_expiration_hours_override(72)
    pending_expiration.clear_expiration_hours_override()
    assert pending_expiration.get_pending_expiration_hours() == 12


def test_get_pending_expiration_hours_falls_through_to_default(monkeypatch):
    monkeypatch.delenv("PENDING_EXPIRATION_HOURS", raising=False)
    assert (
        pending_expiration.get_pending_expiration_hours()
        == pending_expiration.EXPIRATION_HOURS_DEFAULT
    )


def test_get_source_returns_db_when_override_set():
    pending_expiration.set_expiration_hours_override(48)
    assert pending_expiration.get_pending_expiration_hours_source() == "db"


def test_get_source_returns_env_when_only_env_set(monkeypatch):
    monkeypatch.setenv("PENDING_EXPIRATION_HOURS", "12")
    assert pending_expiration.get_pending_expiration_hours_source() == "env"


def test_get_source_returns_default_with_blank_env(monkeypatch):
    monkeypatch.delenv("PENDING_EXPIRATION_HOURS", raising=False)
    assert pending_expiration.get_pending_expiration_hours_source() == "default"


def test_get_source_returns_default_with_invalid_env(monkeypatch):
    monkeypatch.setenv("PENDING_EXPIRATION_HOURS", "notanint")
    # ``_read_int_env`` falls back to compile-time default for non-
    # numeric env values, so the badge should reflect "default".
    assert pending_expiration.get_pending_expiration_hours_source() == "default"


async def test_refresh_from_db_with_no_row_clears_override():
    pending_expiration.set_expiration_hours_override(48)
    db = _StubDB()  # no rows
    result = await pending_expiration.refresh_expiration_hours_override_from_db(db)
    assert result is None
    assert pending_expiration.get_expiration_hours_override() is None


async def test_refresh_from_db_loads_valid_row():
    db = _StubDB(
        {pending_expiration.EXPIRATION_HOURS_SETTING_KEY: "168"}
    )
    result = await pending_expiration.refresh_expiration_hours_override_from_db(db)
    assert result == 168
    assert pending_expiration.get_expiration_hours_override() == 168


async def test_refresh_from_db_keeps_cache_on_get_error(caplog):
    pending_expiration.set_expiration_hours_override(48)
    db = _StubDB(raise_on_get=RuntimeError("pool blip"))
    with caplog.at_level(logging.ERROR, logger="bot.pending_expiration"):
        result = await pending_expiration.refresh_expiration_hours_override_from_db(db)
    assert result == 48
    # Cache must not have been wiped by the transient error.
    assert pending_expiration.get_expiration_hours_override() == 48
    assert any("get_setting" in r.message for r in caplog.records)


async def test_refresh_from_db_clears_on_malformed_value(caplog):
    pending_expiration.set_expiration_hours_override(48)
    db = _StubDB(
        {pending_expiration.EXPIRATION_HOURS_SETTING_KEY: "notanint"}
    )
    with caplog.at_level(logging.WARNING, logger="bot.pending_expiration"):
        result = await pending_expiration.refresh_expiration_hours_override_from_db(db)
    assert result is None
    assert pending_expiration.get_expiration_hours_override() is None
    assert any("rejected stored" in r.message for r in caplog.records)


async def test_refresh_from_db_clears_on_above_max(caplog):
    pending_expiration.set_expiration_hours_override(48)
    db = _StubDB({
        pending_expiration.EXPIRATION_HOURS_SETTING_KEY: str(
            pending_expiration.EXPIRATION_HOURS_OVERRIDE_MAXIMUM + 1
        )
    })
    with caplog.at_level(logging.WARNING, logger="bot.pending_expiration"):
        result = await pending_expiration.refresh_expiration_hours_override_from_db(db)
    assert result is None
    assert pending_expiration.get_expiration_hours_override() is None


async def test_refresh_from_db_returns_cache_when_db_is_none():
    pending_expiration.set_expiration_hours_override(48)
    result = await pending_expiration.refresh_expiration_hours_override_from_db(None)
    assert result == 48
    assert pending_expiration.get_expiration_hours_override() == 48


# ---------------------------------------------------------------------
# Bundled bug fix: audit meta now carries threshold_hours_used.
# ---------------------------------------------------------------------


async def test_audit_records_threshold_hours_used(monkeypatch):
    """Stage-15-Step-E #10b row 9 bundled bug fix: every payment_expired
    audit row carries the resolved ``threshold_hours_used`` so an
    investigator can tell whether an EXPIRED row was reaped under
    the default 24h or a custom override.
    """
    expired = [{
        "transaction_id": 7,
        "telegram_id": 11,
        "currency_used": "usdttrc20",
        "amount_usd_credited": 0.0,
        "gateway_invoice_id": "inv-7",
        "created_at": "2026-01-01T00:00:00+00:00",
    }]
    fake_db = MagicMock()
    fake_db.expire_stale_pending = AsyncMock(return_value=expired)
    fake_db.record_admin_audit = AsyncMock(return_value=1)
    fake_db.get_user_language = AsyncMock(return_value="en")
    monkeypatch.setattr(pending_expiration, "db", fake_db)

    bot = MagicMock()
    bot.send_message = AsyncMock(return_value=None)

    await pending_expiration.expire_pending_once(
        bot, threshold_hours=72, batch_limit=10
    )

    fake_db.record_admin_audit.assert_awaited_once()
    call = fake_db.record_admin_audit.await_args
    meta = call.kwargs["meta"]
    assert meta["threshold_hours_used"] == 72
    # Sanity-check the other audit fields didn't regress.
    assert meta["gateway_invoice_id"] == "inv-7"
    assert meta["telegram_id"] == 11


async def test_audit_records_override_threshold_when_set(monkeypatch):
    """When an override is active, the loop's resolved threshold (not
    the env default) gets recorded into the audit row so forensics
    can reconcile against the operator's audit trail."""
    expired = [{
        "transaction_id": 8,
        "telegram_id": 22,
        "currency_used": "btc",
        "amount_usd_credited": 0.0,
        "gateway_invoice_id": "inv-8",
        "created_at": None,
    }]
    fake_db = MagicMock()
    fake_db.expire_stale_pending = AsyncMock(return_value=expired)
    fake_db.record_admin_audit = AsyncMock(return_value=1)
    fake_db.get_user_language = AsyncMock(return_value="en")
    monkeypatch.setattr(pending_expiration, "db", fake_db)

    bot = MagicMock()
    bot.send_message = AsyncMock(return_value=None)

    pending_expiration.set_expiration_hours_override(168)
    threshold = pending_expiration.get_pending_expiration_hours()
    assert threshold == 168
    await pending_expiration.expire_pending_once(
        bot, threshold_hours=threshold, batch_limit=10
    )

    fake_db.record_admin_audit.assert_awaited_once()
    meta = fake_db.record_admin_audit.await_args.kwargs["meta"]
    assert meta["threshold_hours_used"] == 168


# ---------------------------------------------------------------------
# Manual ``Tick now`` button respects DB override (bug fix).
# ---------------------------------------------------------------------


async def test_tick_pending_reaper_from_app_uses_override(monkeypatch):
    """Pre-fix the manual ``Tick now`` button on /admin/control read
    ``PENDING_EXPIRATION_HOURS`` directly from env, silently
    bypassing any DB override the operator had set. Now it routes
    through :func:`get_pending_expiration_hours` like the loop and
    Database.expire_stale_pending."""

    captured = {}

    async def fake_expire_pending_once(_bot, *, threshold_hours, batch_limit):
        captured["threshold_hours"] = threshold_hours
        captured["batch_limit"] = batch_limit
        return 0

    monkeypatch.setattr(
        pending_expiration, "expire_pending_once", fake_expire_pending_once
    )
    monkeypatch.setenv("PENDING_EXPIRATION_HOURS", "24")
    pending_expiration.set_expiration_hours_override(96)

    fake_app = {}
    # Inject a stub bot via APP_KEY_BOT.
    from web_admin import APP_KEY_BOT
    fake_app[APP_KEY_BOT] = MagicMock()

    class _AppDict(dict):
        def get(self, key, default=None):
            return super().get(key, default)

    app = _AppDict(fake_app)
    await pending_expiration._tick_pending_reaper_from_app(app)
    assert captured["threshold_hours"] == 96


async def test_tick_pending_reaper_from_app_raises_when_bot_missing():
    """If the bot was never added to the app state the manual tick
    must raise loud rather than silently skipping."""

    class _AppDict(dict):
        def get(self, key, default=None):
            return super().get(key, default)

    app = _AppDict()
    with pytest.raises(RuntimeError, match="bot not in app state"):
        await pending_expiration._tick_pending_reaper_from_app(app)
