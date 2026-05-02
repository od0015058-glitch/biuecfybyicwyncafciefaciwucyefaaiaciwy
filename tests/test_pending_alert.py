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


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 10 — DB-backed PENDING_ALERT_THRESHOLD_HOURS
# override.
# ---------------------------------------------------------------------


import logging  # noqa: E402  (kept local to the new section)


@pytest.fixture(autouse=True)
def _reset_alert_threshold_override():
    """Each test starts from a clean override slot.

    Mirrors :func:`test_pending_expiration._reset_expiration_hours_override`
    so a test that monkeypatches env doesn't see a leaked override
    from a previous test, and a leaked override doesn't survive
    into later tests in the same module.
    """
    pending_alert.reset_alert_threshold_override_for_tests()
    yield
    pending_alert.reset_alert_threshold_override_for_tests()


class _StubDB:
    """Minimal DB stub mirroring ``test_pending_expiration._StubDB``."""

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


# ---- _coerce_alert_threshold_hours ----------------------------------


def test_coerce_alert_threshold_hours_accepts_int():
    assert pending_alert._coerce_alert_threshold_hours(2) == 2


def test_coerce_alert_threshold_hours_accepts_string():
    assert pending_alert._coerce_alert_threshold_hours("4") == 4


def test_coerce_alert_threshold_hours_strips_string():
    assert pending_alert._coerce_alert_threshold_hours("  6  ") == 6


def test_coerce_alert_threshold_hours_rejects_bool():
    """A stored ``"true"`` row would coerce to ``1`` and shrink the
    threshold to "anything PENDING for an hour is suspicious", which
    would page admins constantly. Reject explicitly."""
    assert pending_alert._coerce_alert_threshold_hours(True) is None
    assert pending_alert._coerce_alert_threshold_hours(False) is None


def test_coerce_alert_threshold_hours_rejects_zero():
    assert pending_alert._coerce_alert_threshold_hours(0) is None


def test_coerce_alert_threshold_hours_rejects_negative():
    assert pending_alert._coerce_alert_threshold_hours(-1) is None


def test_coerce_alert_threshold_hours_rejects_above_maximum():
    above_max = pending_alert.ALERT_THRESHOLD_OVERRIDE_MAXIMUM + 1
    assert pending_alert._coerce_alert_threshold_hours(above_max) is None


def test_coerce_alert_threshold_hours_accepts_at_minimum():
    assert pending_alert._coerce_alert_threshold_hours(
        pending_alert.ALERT_THRESHOLD_MINIMUM
    ) == pending_alert.ALERT_THRESHOLD_MINIMUM


def test_coerce_alert_threshold_hours_accepts_at_maximum():
    assert pending_alert._coerce_alert_threshold_hours(
        pending_alert.ALERT_THRESHOLD_OVERRIDE_MAXIMUM
    ) == pending_alert.ALERT_THRESHOLD_OVERRIDE_MAXIMUM


def test_coerce_alert_threshold_hours_rejects_non_numeric_string():
    assert pending_alert._coerce_alert_threshold_hours("notanint") is None


def test_coerce_alert_threshold_hours_rejects_blank_string():
    assert pending_alert._coerce_alert_threshold_hours("") is None
    assert pending_alert._coerce_alert_threshold_hours("   ") is None


def test_coerce_alert_threshold_hours_rejects_other_types():
    assert pending_alert._coerce_alert_threshold_hours(None) is None
    assert pending_alert._coerce_alert_threshold_hours([2]) is None
    assert pending_alert._coerce_alert_threshold_hours({"hours": 2}) is None
    assert pending_alert._coerce_alert_threshold_hours(2.5) is None


# ---- set / clear / get override -------------------------------------


def test_set_alert_threshold_override_persists():
    pending_alert.set_alert_threshold_override(4)
    assert pending_alert.get_alert_threshold_override() == 4


def test_set_alert_threshold_override_revalidates():
    """Defence-in-depth: the public setter re-runs the coercer so a
    bad value rejected by the coercer is also rejected here."""
    with pytest.raises(ValueError):
        pending_alert.set_alert_threshold_override(0)
    with pytest.raises(ValueError):
        pending_alert.set_alert_threshold_override(True)
    with pytest.raises(ValueError):
        pending_alert.set_alert_threshold_override(
            pending_alert.ALERT_THRESHOLD_OVERRIDE_MAXIMUM + 1
        )


def test_clear_alert_threshold_override_returns_true_when_active():
    pending_alert.set_alert_threshold_override(4)
    assert pending_alert.clear_alert_threshold_override() is True
    assert pending_alert.get_alert_threshold_override() is None


def test_clear_alert_threshold_override_returns_false_when_clean():
    assert pending_alert.clear_alert_threshold_override() is False


def test_get_pending_alert_threshold_hours_returns_override():
    """Override beats env beats default."""
    pending_alert.set_alert_threshold_override(7)
    with patch.dict(os.environ, {"PENDING_ALERT_THRESHOLD_HOURS": "3"}):
        assert pending_alert.get_pending_alert_threshold_hours() == 7


def test_get_pending_alert_threshold_hours_env_when_no_override():
    with patch.dict(os.environ, {"PENDING_ALERT_THRESHOLD_HOURS": "5"}):
        assert pending_alert.get_pending_alert_threshold_hours() == 5


def test_get_pending_alert_threshold_hours_default_when_no_override_or_env():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("PENDING_ALERT_THRESHOLD_HOURS", None)
        assert pending_alert.get_pending_alert_threshold_hours() == \
            pending_alert.ALERT_THRESHOLD_DEFAULT


# ---- get_pending_alert_threshold_source -----------------------------


def test_get_pending_alert_threshold_source_db_when_override():
    pending_alert.set_alert_threshold_override(4)
    assert pending_alert.get_pending_alert_threshold_source() == "db"


def test_get_pending_alert_threshold_source_env_when_env_set():
    with patch.dict(os.environ, {"PENDING_ALERT_THRESHOLD_HOURS": "3"}):
        assert pending_alert.get_pending_alert_threshold_source() == "env"


def test_get_pending_alert_threshold_source_default_when_clean():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("PENDING_ALERT_THRESHOLD_HOURS", None)
        assert pending_alert.get_pending_alert_threshold_source() == "default"


def test_get_pending_alert_threshold_source_default_when_env_garbage():
    """Non-numeric env var falls back to the compile-time default;
    surface that as ``default`` so the panel doesn't lie."""
    with patch.dict(os.environ, {"PENDING_ALERT_THRESHOLD_HOURS": "abc"}):
        assert pending_alert.get_pending_alert_threshold_source() == "default"


def test_get_pending_alert_threshold_source_db_overrides_env():
    pending_alert.set_alert_threshold_override(7)
    with patch.dict(os.environ, {"PENDING_ALERT_THRESHOLD_HOURS": "3"}):
        assert pending_alert.get_pending_alert_threshold_source() == "db"


# ---- refresh_alert_threshold_override_from_db -----------------------


async def test_refresh_alert_threshold_from_db_with_no_row_clears_override():
    pending_alert.set_alert_threshold_override(7)
    db = _StubDB()  # no rows
    result = await pending_alert.refresh_alert_threshold_override_from_db(db)
    assert result is None
    assert pending_alert.get_alert_threshold_override() is None


async def test_refresh_alert_threshold_from_db_loads_valid_row():
    db = _StubDB({pending_alert.ALERT_THRESHOLD_SETTING_KEY: "4"})
    result = await pending_alert.refresh_alert_threshold_override_from_db(db)
    assert result == 4
    assert pending_alert.get_alert_threshold_override() == 4


async def test_refresh_alert_threshold_from_db_keeps_cache_on_get_error(caplog):
    pending_alert.set_alert_threshold_override(7)
    db = _StubDB(raise_on_get=RuntimeError("pool blip"))
    with caplog.at_level(logging.ERROR, logger="bot.pending_alert"):
        result = await pending_alert.refresh_alert_threshold_override_from_db(db)
    assert result == 7
    assert pending_alert.get_alert_threshold_override() == 7
    assert any(
        "refresh_alert_threshold_override_from_db: get_setting failed" in r.message
        for r in caplog.records
    )


async def test_refresh_alert_threshold_from_db_clears_on_malformed_value(caplog):
    pending_alert.set_alert_threshold_override(7)
    db = _StubDB({pending_alert.ALERT_THRESHOLD_SETTING_KEY: "notanint"})
    with caplog.at_level(logging.WARNING, logger="bot.pending_alert"):
        result = await pending_alert.refresh_alert_threshold_override_from_db(db)
    assert result is None
    assert pending_alert.get_alert_threshold_override() is None


async def test_refresh_alert_threshold_from_db_clears_on_above_max(caplog):
    pending_alert.set_alert_threshold_override(7)
    db = _StubDB({
        pending_alert.ALERT_THRESHOLD_SETTING_KEY: str(
            pending_alert.ALERT_THRESHOLD_OVERRIDE_MAXIMUM + 1
        )
    })
    with caplog.at_level(logging.WARNING, logger="bot.pending_alert"):
        result = await pending_alert.refresh_alert_threshold_override_from_db(db)
    assert result is None
    assert pending_alert.get_alert_threshold_override() is None


async def test_refresh_alert_threshold_from_db_returns_cache_when_db_none():
    pending_alert.set_alert_threshold_override(7)
    result = await pending_alert.refresh_alert_threshold_override_from_db(None)
    assert result == 7
    assert pending_alert.get_alert_threshold_override() == 7


# ---- _alert_loop iteration-time re-read -----------------------------


async def test_alert_loop_rereads_threshold_each_iteration():
    """The loop's threshold kwarg is the bootstrap value for the very
    first iteration; subsequent iterations re-read via
    :func:`get_pending_alert_threshold_hours`. Pin this so a saved DB
    override is live within at most one tick — no restart required.

    Drives two iterations, asserts that iteration 2 uses the
    override, not the bootstrap kwarg."""
    import asyncio

    bot = MagicMock()
    seen_thresholds: list[int] = []

    async def _fake_pass(b, *, threshold_hours, state, row_limit):
        seen_thresholds.append(threshold_hours)
        # After the first iteration completes, install an override
        # so iteration 2 sees the new value.
        if len(seen_thresholds) == 1:
            pending_alert.set_alert_threshold_override(8)
        if len(seen_thresholds) >= 2:
            raise asyncio.CancelledError()
        return 0

    with patch(
        "pending_alert.run_pending_alert_pass",
        side_effect=_fake_pass,
    ):
        # Tiny sleep so the test doesn't actually sleep 30 min.
        with patch("asyncio.sleep", new=AsyncMock()):
            with pytest.raises(asyncio.CancelledError):
                await pending_alert._alert_loop(
                    bot,
                    interval_seconds=1,
                    threshold_hours=2,  # bootstrap
                    row_limit=500,
                )

    # First iteration: bootstrap. Second iteration: override.
    assert seen_thresholds[0] == 2
    assert seen_thresholds[1] == 8


async def test_alert_loop_keeps_previous_threshold_when_resolver_raises(caplog):
    """If :func:`get_pending_alert_threshold_hours` raises during the
    iteration-time re-read, the loop falls back to the previous
    threshold (logged loudly) rather than crashing or alerting on
    age=0 rows.

    Drives two iterations: the resolver raises during the re-read
    after iteration 1, and iteration 2 should still see the bootstrap
    threshold."""
    import asyncio

    bot = MagicMock()
    seen_thresholds: list[int] = []

    async def _fake_pass(b, *, threshold_hours, state, row_limit):
        seen_thresholds.append(threshold_hours)
        if len(seen_thresholds) >= 2:
            raise asyncio.CancelledError()
        return 0

    with patch(
        "pending_alert.run_pending_alert_pass",
        side_effect=_fake_pass,
    ):
        with patch("asyncio.sleep", new=AsyncMock()):
            with patch(
                "pending_alert.get_pending_alert_threshold_hours",
                side_effect=RuntimeError("resolver blip"),
            ):
                with caplog.at_level(
                    logging.ERROR, logger="bot.pending_alert"
                ):
                    with pytest.raises(asyncio.CancelledError):
                        await pending_alert._alert_loop(
                            bot,
                            interval_seconds=1,
                            threshold_hours=3,  # bootstrap
                            row_limit=500,
                        )

    # Iteration 1 (bootstrap = 3), then resolver raises during re-read,
    # iteration 2 falls back to the previous value (still 3).
    assert seen_thresholds == [3, 3]
    assert any(
        "get_pending_alert_threshold_hours raised" in r.message
        for r in caplog.records
    )


# ---- start_pending_alert_task bootstrap respects override ----------


async def test_start_pending_alert_task_bootstrap_respects_override():
    """The loop's bootstrap threshold (passed to ``_alert_loop`` as a
    kwarg) routes through the override-aware resolver, so a warmed
    override is live for the very first iteration. Defence-in-depth
    against ordering bugs in :mod:`main`."""
    import asyncio

    pending_alert.set_alert_threshold_override(8)
    bot = MagicMock()

    captured = {}

    async def _fake_loop(b, *, interval_seconds, threshold_hours, row_limit):
        captured["threshold_hours"] = threshold_hours

    with patch("pending_alert._alert_loop", side_effect=_fake_loop):
        task = pending_alert.start_pending_alert_task(bot)
        await task

    assert captured["threshold_hours"] == 8


# ---- _tick_pending_alert_from_app respects override ---------------


async def test_tick_pending_alert_from_app_respects_override():
    """The manual 'Tick now' button path uses
    :func:`get_pending_alert_threshold_hours` so it picks up DB
    overrides without ceremony. Pin against a regression that would
    silently re-introduce the env-only read.
    """
    pending_alert.set_alert_threshold_override(8)

    app = MagicMock()
    bot = MagicMock()
    from web_admin import APP_KEY_BOT
    app.get = MagicMock(return_value=bot)
    seen_thresholds: list[int] = []

    async def _fake_pass(b, *, threshold_hours, state, row_limit):
        seen_thresholds.append(threshold_hours)
        return 0

    with patch(
        "pending_alert.run_pending_alert_pass",
        side_effect=_fake_pass,
    ):
        await pending_alert._tick_pending_alert_from_app(app)

    app.get.assert_called_with(APP_KEY_BOT)
    assert seen_thresholds == [8]


async def test_tick_pending_alert_from_app_raises_when_bot_missing():
    """No ``bot`` in app state → manual tick raises a clear error
    instead of silently doing nothing."""
    app = MagicMock()
    app.get = MagicMock(return_value=None)
    with pytest.raises(RuntimeError, match="bot not in app state"):
        await pending_alert._tick_pending_alert_from_app(app)
