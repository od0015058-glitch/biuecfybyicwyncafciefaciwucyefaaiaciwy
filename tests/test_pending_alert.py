"""Stage-12-Step-B: tests for the proactive stuck-pending admin alert
loop (:mod:`pending_alert`).

Covers the four contracts the loop must keep:

1. The DM body renders correctly for one-row, multi-row, and
   overflow cases, with the threshold echoed in the head.
2. Per-admin fault isolation: ``TelegramForbiddenError`` on admin A
   does not stop admin B's notification, and ``TelegramAPIError``
   is logged + swallowed.
3. Per-row hour-bucket dedupe: a row stuck for 2.4 h alerts at hour
   2, then again at hour 3 (after age crosses the integer boundary),
   never twice in the same hour-bucket.
4. ``run_pending_alert_pass`` swallows DB errors and logs them so a
   transient blip can't take the loop off the air.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

import pending_alert


# ---------------------------------------------------------------------
# env-parse helpers
# ---------------------------------------------------------------------


def test_get_pending_alert_threshold_hours_default_is_2():
    """Default mirrors the documented value in HANDOFF + .env.example."""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("PENDING_ALERT_THRESHOLD_HOURS", None)
        assert pending_alert.get_pending_alert_threshold_hours() == 2


def test_get_pending_alert_threshold_hours_clamps_to_minimum():
    """A deploy-time typo (`PENDING_ALERT_THRESHOLD_HOURS=0`) must
    clamp to the 1-hour floor — sub-hour alerts would stutter on
    rows that just hit the line."""
    with patch.dict(os.environ, {"PENDING_ALERT_THRESHOLD_HOURS": "0"}):
        assert pending_alert.get_pending_alert_threshold_hours() == 1


def test_get_pending_alert_threshold_hours_typo_falls_back():
    """Non-integer value logs and falls back to default rather than
    crashing the boot sequence."""
    with patch.dict(os.environ, {"PENDING_ALERT_THRESHOLD_HOURS": "abc"}):
        assert pending_alert.get_pending_alert_threshold_hours() == 2


def test_get_pending_alert_interval_seconds_default_is_30min():
    """30 min is the documented default; expressed in seconds for the
    asyncio.sleep call site."""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("PENDING_ALERT_INTERVAL_MIN", None)
        assert pending_alert.get_pending_alert_interval_seconds() == 30 * 60


def test_get_pending_alert_row_limit_default_is_500():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("PENDING_ALERT_LIMIT", None)
        assert pending_alert.get_pending_alert_row_limit() == 500


# ---------------------------------------------------------------------
# alert_key (per-row dedupe)
# ---------------------------------------------------------------------


def test_alert_key_uses_floor_of_age_hours():
    """A row stuck for 2.4 h alerts at hour 2; the same row at hour
    3.1 alerts at hour 3 — distinct keys, distinct alerts."""
    assert pending_alert._alert_key(
        {"transaction_id": 42, "age_hours": 2.4}
    ) == (42, 2)
    assert pending_alert._alert_key(
        {"transaction_id": 42, "age_hours": 3.1}
    ) == (42, 3)


def test_alert_key_handles_missing_age_hours():
    """A defensive default for rows that somehow arrive without
    ``age_hours`` populated (shouldn't happen with the canonical SQL,
    but the loop doesn't crash if it does)."""
    assert pending_alert._alert_key(
        {"transaction_id": 42}
    ) == (42, 0)


# ---------------------------------------------------------------------
# _format_alert_body
# ---------------------------------------------------------------------


def _make_row(**overrides) -> dict:
    base = {
        "transaction_id": 1,
        "telegram_id": 99,
        "gateway": "nowpayments",
        "currency_used": "usdttrc20",
        "amount_usd_credited": 12.34,
        "gateway_invoice_id": "inv-1",
        "created_at": "2024-01-01T00:00:00",
        "age_hours": 2.5,
    }
    base.update(overrides)
    return base


def test_format_alert_body_single_row_includes_threshold_in_head():
    body = pending_alert._format_alert_body([_make_row()], threshold_hours=2)
    assert "stuck over 2h" in body
    assert "tx#1" in body
    assert "nowpayments" in body
    assert "$12.34" in body
    assert "inv-1" in body
    assert "2.5h" in body


def test_format_alert_body_multiple_rows_one_line_each():
    rows = [
        _make_row(transaction_id=1, age_hours=2.1),
        _make_row(transaction_id=2, age_hours=4.2),
        _make_row(transaction_id=3, age_hours=6.3),
    ]
    body = pending_alert._format_alert_body(rows, threshold_hours=2)
    assert "3 pending payment" in body
    assert "tx#1" in body and "tx#2" in body and "tx#3" in body
    # No overflow footer needed at 3 rows.
    assert "more" not in body


def test_format_alert_body_truncates_to_max_rows_with_footer():
    """11 rows over a 10-row body cap renders 10 + an overflow footer
    naming the residual count, plus a pointer to the admin panel."""
    rows = [
        _make_row(transaction_id=i, age_hours=2.0 + i * 0.1)
        for i in range(15)
    ]
    body = pending_alert._format_alert_body(rows, threshold_hours=2)
    assert "15 pending payment" in body
    assert "…and 5 more" in body
    assert "/admin/transactions?status=PENDING" in body


def test_format_alert_body_handles_null_fields_gracefully():
    """A row with NULL gateway / amount / invoice still renders with
    placeholders rather than crashing the loop."""
    body = pending_alert._format_alert_body(
        [{"transaction_id": 99, "age_hours": 3.0}], threshold_hours=2
    )
    assert "tx#99" in body
    assert "?" in body  # placeholder for missing gateway / invoice
    assert "$0.00" in body  # NULL amount renders as 0


# ---------------------------------------------------------------------
# notify_admins_of_stuck_pending: per-admin fault isolation
# ---------------------------------------------------------------------


async def test_notify_admins_no_op_on_empty_rows():
    """Empty row list short-circuits before any get_admin_user_ids /
    bot calls — callers can ``await`` unconditionally."""
    bot = MagicMock()
    bot.send_message = AsyncMock()
    sent = await pending_alert.notify_admins_of_stuck_pending(
        bot, [], threshold_hours=2
    )
    assert sent == 0
    bot.send_message.assert_not_awaited()


async def test_notify_admins_warns_when_admin_ids_empty():
    """ADMIN_USER_IDS unset is a config bug, not a row-data bug. We
    log loudly and return 0 — no DM sent because nobody to DM."""
    bot = MagicMock()
    bot.send_message = AsyncMock()
    with patch.object(
        pending_alert, "get_admin_user_ids", return_value=frozenset()
    ):
        sent = await pending_alert.notify_admins_of_stuck_pending(
            bot, [_make_row()], threshold_hours=2
        )
    assert sent == 0
    bot.send_message.assert_not_awaited()


async def test_notify_admins_isolates_blocked_admin():
    """Admin A blocked the bot; admin B still gets the DM."""
    bot = MagicMock()
    bot.send_message = AsyncMock(
        side_effect=[TelegramForbiddenError(method="x", message="b"), None]
    )
    with patch.object(
        pending_alert, "get_admin_user_ids", return_value=frozenset({1, 2})
    ):
        sent = await pending_alert.notify_admins_of_stuck_pending(
            bot, [_make_row()], threshold_hours=2
        )
    assert sent == 1
    assert bot.send_message.await_count == 2


async def test_notify_admins_isolates_telegram_api_error():
    """A 5xx / network blip on admin A is logged and swallowed; the
    next admin still gets DM'd."""
    bot = MagicMock()
    bot.send_message = AsyncMock(
        side_effect=[TelegramAPIError(method="x", message="5xx"), None]
    )
    with patch.object(
        pending_alert, "get_admin_user_ids", return_value=frozenset({1, 2})
    ):
        sent = await pending_alert.notify_admins_of_stuck_pending(
            bot, [_make_row()], threshold_hours=2
        )
    assert sent == 1


# ---------------------------------------------------------------------
# run_pending_alert_pass: dedupe + DB-error fault isolation
# ---------------------------------------------------------------------


async def test_run_pass_returns_0_when_no_rows():
    bot = MagicMock()
    bot.send_message = AsyncMock()
    with patch.object(
        pending_alert.db,
        "list_pending_payments_over_threshold",
        AsyncMock(return_value=[]),
    ):
        sent = await pending_alert.run_pending_alert_pass(
            bot,
            threshold_hours=2,
            state=set(),
            row_limit=500,
        )
    assert sent == 0


async def test_run_pass_deduplicates_within_hour_bucket():
    """First pass alerts on the row; second pass at the same hour-
    bucket reads the same row from the DB, finds its key already in
    state, and emits no DM. Only when age crosses the integer-hour
    boundary does the next alert fire."""
    bot = MagicMock()
    bot.send_message = AsyncMock()

    state: set = set()

    row_at_2h = _make_row(transaction_id=1, age_hours=2.4)
    row_at_2h_again = _make_row(transaction_id=1, age_hours=2.7)
    row_at_3h = _make_row(transaction_id=1, age_hours=3.1)

    with patch.object(
        pending_alert,
        "get_admin_user_ids",
        return_value=frozenset({99}),
    ):
        # Pass 1: row is fresh, alert fires.
        with patch.object(
            pending_alert.db,
            "list_pending_payments_over_threshold",
            AsyncMock(return_value=[row_at_2h]),
        ):
            sent1 = await pending_alert.run_pending_alert_pass(
                bot, threshold_hours=2, state=state, row_limit=500,
            )
        assert sent1 == 1

        # Pass 2: same hour-bucket — silent.
        with patch.object(
            pending_alert.db,
            "list_pending_payments_over_threshold",
            AsyncMock(return_value=[row_at_2h_again]),
        ):
            sent2 = await pending_alert.run_pending_alert_pass(
                bot, threshold_hours=2, state=state, row_limit=500,
            )
        assert sent2 == 0

        # Pass 3: integer-hour boundary crossed — alert fires again.
        with patch.object(
            pending_alert.db,
            "list_pending_payments_over_threshold",
            AsyncMock(return_value=[row_at_3h]),
        ):
            sent3 = await pending_alert.run_pending_alert_pass(
                bot, threshold_hours=2, state=state, row_limit=500,
            )
        assert sent3 == 1


async def test_run_pass_swallows_db_errors():
    """A transient DB blip is logged and the pass returns 0 — the
    enclosing forever-loop must not die."""
    bot = MagicMock()
    bot.send_message = AsyncMock()
    with patch.object(
        pending_alert.db,
        "list_pending_payments_over_threshold",
        AsyncMock(side_effect=RuntimeError("connection lost")),
    ):
        sent = await pending_alert.run_pending_alert_pass(
            bot,
            threshold_hours=2,
            state=set(),
            row_limit=500,
        )
    assert sent == 0
    bot.send_message.assert_not_awaited()


async def test_run_pass_registers_keys_even_when_dm_fails():
    """If the DM dispatch fails (all admins blocked), we still record
    the alert key so we don't pound the same row every 30 min. The
    hour-bucket roll naturally re-fires the alert at the next
    integer-hour boundary."""
    bot = MagicMock()
    bot.send_message = AsyncMock(
        side_effect=TelegramForbiddenError(method="x", message="b")
    )

    state: set = set()
    row = _make_row(transaction_id=42, age_hours=2.5)
    with patch.object(
        pending_alert,
        "get_admin_user_ids",
        return_value=frozenset({1}),
    ), patch.object(
        pending_alert.db,
        "list_pending_payments_over_threshold",
        AsyncMock(return_value=[row]),
    ):
        sent = await pending_alert.run_pending_alert_pass(
            bot, threshold_hours=2, state=state, row_limit=500,
        )
    assert sent == 0
    # State still recorded — the next pass at the same hour-bucket
    # is silent.
    assert (42, 2) in state


# ---------------------------------------------------------------------
# Database.list_pending_payments_over_threshold
# ---------------------------------------------------------------------


async def test_list_pending_validates_threshold_hours():
    """Zero / negative threshold is a deploy bug, not a query — fail
    loudly before reaching the pool."""
    import database as database_module
    db = database_module.Database()
    with pytest.raises(ValueError, match="threshold_hours"):
        await db.list_pending_payments_over_threshold(threshold_hours=0)
    with pytest.raises(ValueError, match="threshold_hours"):
        await db.list_pending_payments_over_threshold(threshold_hours=-1)


async def test_list_pending_validates_limit():
    import database as database_module
    db = database_module.Database()
    with pytest.raises(ValueError, match="limit"):
        await db.list_pending_payments_over_threshold(
            threshold_hours=2, limit=0
        )


async def test_list_pending_returns_normalized_rows():
    """The DB cursor returns asyncpg Records; the method maps them to
    plain dicts with float ``age_hours`` so the alert loop can
    bucket without re-typing."""
    from tests.test_database_queries import _PoolStub, _make_conn
    import database as database_module

    conn = _make_conn()
    conn.fetch = AsyncMock(
        return_value=[
            {
                "transaction_id": 7,
                "telegram_id": 99,
                "gateway": "nowpayments",
                "currency_used": "usdttrc20",
                "amount_usd_credited": 12.34,
                "gateway_invoice_id": "abc",
                "created_at": None,
                "age_hours": 3.5,
            }
        ]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    rows = await db.list_pending_payments_over_threshold(
        threshold_hours=2, limit=500
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["transaction_id"] == 7
    assert r["telegram_id"] == 99
    assert r["gateway"] == "nowpayments"
    assert r["amount_usd_credited"] == 12.34
    assert r["age_hours"] == 3.5
    # Pin the SQL: must filter by status='PENDING' and use the
    # threshold interval form. A leak to SUCCESS / PARTIAL / EXPIRED
    # would alert on rows that aren't actually stuck.
    sql = conn.fetch.await_args.args[0]
    normalized = " ".join(sql.split())
    assert "WHERE status = 'PENDING'" in normalized
    assert "NOW() - ($1 || ' hours')::interval" in normalized
    assert "ORDER BY created_at" in normalized


async def test_list_pending_passes_threshold_and_limit():
    """The two query bind values are the threshold (as a string,
    matched up with ``($1 || ' hours')::interval``) and the limit
    (an integer ``$2``)."""
    from tests.test_database_queries import _PoolStub, _make_conn
    import database as database_module

    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    await db.list_pending_payments_over_threshold(
        threshold_hours=4, limit=42
    )
    args = conn.fetch.await_args.args
    assert args[1] == "4"
    assert args[2] == 42


# ---------------------------------------------------------------------
# Bug-fix-bundle pin: get_system_metrics + the alert loop must use
# the same threshold so the dashboard tile matches the DM body.
# ---------------------------------------------------------------------


async def test_get_system_metrics_threshold_matches_alert_loop_default():
    """The dashboard handler reads the threshold via
    :func:`pending_alert.get_pending_alert_threshold_hours` and
    forwards it into :meth:`Database.get_system_metrics`. Pin that
    the default flows end-to-end so an admin viewing the dashboard
    at 15:00 and a DM body at 15:30 cite the same number."""
    from tests.test_database_queries import _PoolStub, _make_conn
    import database as database_module

    conn = _make_conn()
    conn.fetchval = AsyncMock(side_effect=lambda *a, **k: 0)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value={
        "count": 5,
        "oldest_age_hours": 4.0,
        "over_threshold_count": 3,
    })
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    threshold = pending_alert.get_pending_alert_threshold_hours()
    result = await db.get_system_metrics(
        pending_alert_threshold_hours=threshold,
    )
    assert result["pending_alert_threshold_hours"] == threshold
    assert result["pending_payments_over_threshold_count"] == 3
    # The threshold made it into the SQL bind.
    assert conn.fetchrow.await_args.args[1] == str(threshold)
