"""Unit tests for the pure-SQL-shape bits of ``database.py``.

We don't spin up Postgres in CI — the schema is exercised by the
alembic upgrade/downgrade job. These tests pin the query text for the
methods whose correctness depends on WHERE-clause composition, so a
future refactor can't silently break an invariant (e.g. "revenue
excludes internal gateways").
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

import database as database_module


class _PoolStub:
    """Minimal ``asyncpg.pool``-lookalike that hands out a single
    connection stub (``MagicMock`` with AsyncMock ``fetch`` / ``fetchval``
    / ``fetchrow``).

    Records the SQL passed to each call on ``self.queries`` so tests
    can assert on it.
    """

    def __init__(self, connection):
        self.connection = connection

    def acquire(self):  # noqa: D401 — matches asyncpg signature
        outer = self

        class _Ctx:
            async def __aenter__(self_inner):
                return outer.connection

            async def __aexit__(self_inner, exc_type, exc, tb):
                return False

        return _Ctx()


def _make_conn():
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=0)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    return conn


# ---------------------------------------------------------------------
# get_system_metrics: revenue filter
# ---------------------------------------------------------------------


async def test_get_system_metrics_revenue_excludes_admin_and_gift_gateways():
    """Regression: PR #56 shipped gateway='gift' transaction rows for
    gift redemptions. Those are free-money-from-nothing credits and
    must NOT count as gateway revenue (which is real paid top-ups
    from NowPayments). The revenue query originally only excluded
    gateway='admin'; Stage-8-Part-4 widened the filter.
    """
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_system_metrics()

    sqls = [call.args[0] for call in conn.fetchval.await_args_list]
    revenue_sql = next(
        (s for s in sqls if "amount_usd_credited" in s and "SUCCESS" in s),
        None,
    )
    assert revenue_sql is not None, (
        "revenue_usd query not found in fetchval calls"
    )
    normalized = " ".join(revenue_sql.split())
    assert "gateway NOT IN ('admin', 'gift')" in normalized, (
        f"revenue filter doesn't exclude both admin and gift gateways "
        f"(got: {normalized!r})"
    )


async def test_get_system_metrics_returns_shape():
    """Sanity: the returned dict still has the exact shape the
    dashboard template and ``admin.format_metrics`` consume.
    """
    conn = _make_conn()
    # Pretend: 10 users, 3 active, $100 revenue, $4.20 spend, no models.
    fetchval_vals = iter([10, 3, 100.0, 4.20])
    conn.fetchval = AsyncMock(side_effect=lambda *a, **k: next(fetchval_vals))
    conn.fetch = AsyncMock(return_value=[])
    # Stage-9-Step-9: pending_payments tile reads via fetchrow.
    conn.fetchrow = AsyncMock(return_value={
        "count": 0, "oldest_age_hours": None,
    })

    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_system_metrics()

    assert set(result.keys()) == {
        "users_total", "users_active_7d", "revenue_usd",
        "spend_usd", "top_models",
        "pending_payments_count", "pending_payments_oldest_age_hours",
    }
    assert result["users_total"] == 10
    assert result["users_active_7d"] == 3
    assert result["revenue_usd"] == 100.0
    assert result["spend_usd"] == 4.20
    assert result["top_models"] == []
    assert result["pending_payments_count"] == 0
    assert result["pending_payments_oldest_age_hours"] is None


# ---------------------------------------------------------------------
# search_users
# ---------------------------------------------------------------------


async def test_search_users_empty_query_returns_empty_list_without_db():
    """Whitespace-only query must short-circuit before hitting the pool
    — the web handler uses that to distinguish "no search yet" from
    "searched and got nothing".
    """
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    assert await db.search_users("") == []
    assert await db.search_users("   ") == []
    # @-only input is stripped to empty → same short-circuit.
    assert await db.search_users("@") == []
    conn.fetch.assert_not_awaited()


async def test_search_users_integer_query_hits_telegram_id():
    conn = _make_conn()
    conn.fetch = AsyncMock(
        return_value=[
            {
                "telegram_id": 12345,
                "username": "bob",
                "balance_usd": 1.23,
                "free_messages_left": 4,
                "language_code": "fa",
            }
        ]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.search_users("12345")
    assert len(rows) == 1
    assert rows[0]["telegram_id"] == 12345
    sql = conn.fetch.await_args.args[0]
    assert "WHERE telegram_id = $1" in sql
    # First positional bind is the integer id, not a LIKE pattern.
    assert conn.fetch.await_args.args[1] == 12345


async def test_search_users_username_query_uses_escaped_ilike():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.search_users("bob_")  # underscore is a LIKE metacharacter
    sql = conn.fetch.await_args.args[0]
    assert "ILIKE" in sql
    assert "ESCAPE '\\'" in sql
    # Second arg is the LIKE pattern — the underscore must be escaped.
    pattern = conn.fetch.await_args.args[1]
    assert pattern == "%bob\\_%"


async def test_search_users_strips_at_prefix():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    await db.search_users("@alice")
    pattern = conn.fetch.await_args.args[1]
    assert pattern == "%alice%"


async def test_search_users_clamps_limit():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    await db.search_users("bob", limit=99999)
    limit_arg = conn.fetch.await_args.args[2]
    assert limit_arg == 100
    await db.search_users("bob", limit=-5)
    limit_arg = conn.fetch.await_args.args[2]
    assert limit_arg == 1


# ---------------------------------------------------------------------
# get_user_admin_summary recent-tx limit
# ---------------------------------------------------------------------


async def test_get_user_admin_summary_default_limit_is_5():
    """Telegram-side /admin_balance still shows 5 recent transactions
    by default; the web detail page overrides to 20.
    """
    conn = _make_conn()
    conn.fetchrow = AsyncMock(
        return_value={
            "telegram_id": 1,
            "username": "u",
            "balance_usd": 0.0,
            "free_messages_left": 0,
            "active_model": "m",
            "language_code": "en",
            "memory_enabled": False,
        }
    )
    conn.fetchval = AsyncMock(return_value=0)
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_user_admin_summary(1)
    # Transactions query is the only ``fetch`` call.
    limit_arg = conn.fetch.await_args.args[2]
    assert limit_arg == 5


async def test_get_user_admin_summary_custom_limit_clamps():
    conn = _make_conn()
    conn.fetchrow = AsyncMock(
        return_value={
            "telegram_id": 1,
            "username": "u",
            "balance_usd": 0.0,
            "free_messages_left": 0,
            "active_model": "m",
            "language_code": "en",
            "memory_enabled": False,
        }
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_user_admin_summary(1, recent_tx_limit=20)
    assert conn.fetch.await_args.args[2] == 20

    await db.get_user_admin_summary(1, recent_tx_limit=9999)
    assert conn.fetch.await_args.args[2] == 200

    await db.get_user_admin_summary(1, recent_tx_limit=0)
    assert conn.fetch.await_args.args[2] == 1


# ---------------------------------------------------------------------
# iter_broadcast_recipients — active-days bound (Stage-8-Part-6 fix)
# ---------------------------------------------------------------------


async def test_iter_broadcast_recipients_rejects_non_positive_active_days():
    """Defense-in-depth: the parsers already refuse this upstream but
    a direct caller (e.g. a REPL invocation) must also blow up early
    rather than format a bogus interval string.
    """
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    for bad in (0, -1, -365):
        with pytest.raises(ValueError, match="out of range"):
            await db.iter_broadcast_recipients(only_active_days=bad)
    conn.fetch.assert_not_awaited()


async def test_iter_broadcast_recipients_rejects_days_above_cap():
    """An admin typing ``--active=9999999999`` would otherwise
    overflow PG's 32-bit-int interval column and surface as a
    generic "DB query failed" banner. Guard runs before the pool
    is touched so the test doesn't need a real connection.
    """
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    cap = db.BROADCAST_ACTIVE_DAYS_MAX
    # Boundary: exactly at the cap passes. One past the cap blows.
    conn.fetch = AsyncMock(return_value=[])
    await db.iter_broadcast_recipients(only_active_days=cap)
    conn.fetch.assert_awaited_once()
    conn.fetch.reset_mock()

    with pytest.raises(ValueError, match="out of range"):
        await db.iter_broadcast_recipients(only_active_days=cap + 1)
    conn.fetch.assert_not_awaited()


async def test_iter_broadcast_recipients_none_returns_all_users():
    """Sanity: the no-filter branch still hits the "every user"
    query, not the active-users join.
    """
    conn = _make_conn()
    conn.fetch = AsyncMock(
        return_value=[{"telegram_id": 1}, {"telegram_id": 2}]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    out = await db.iter_broadcast_recipients(only_active_days=None)
    assert out == [1, 2]
    sql = conn.fetch.await_args.args[0]
    assert "usage_logs" not in sql  # would be the active-users join


# ---------------------------------------------------------------------
# list_transactions — paginated browser query (Stage-8-Part-6)
# ---------------------------------------------------------------------


async def test_list_transactions_no_filters_paginates_by_id_desc():
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=3)
    conn.fetch = AsyncMock(
        return_value=[
            {
                "transaction_id": 9,
                "telegram_id": 123,
                "gateway": "nowpayments",
                "currency_used": "USDT",
                "amount_crypto_or_rial": 1.234,
                "amount_usd_credited": 9.99,
                "status": "SUCCESS",
                "gateway_invoice_id": "inv-1",
                "created_at": None,
                "completed_at": None,
                "notes": None,
            }
        ]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    out = await db.list_transactions(page=1, per_page=50)

    # Count query: no WHERE clause when no filters supplied.
    count_sql = conn.fetchval.await_args.args[0]
    assert "COUNT(*)" in count_sql
    assert "WHERE" not in count_sql

    # List query orders desc and has LIMIT/OFFSET as the two
    # trailing positional binds.
    list_sql = conn.fetch.await_args.args[0]
    assert "ORDER BY transaction_id DESC" in list_sql
    binds = conn.fetch.await_args.args[1:]
    assert binds == (50, 0)  # page=1, per_page=50 → offset 0

    assert out["total"] == 3
    assert out["total_pages"] == 1
    assert out["page"] == 1
    assert out["per_page"] == 50
    assert len(out["rows"]) == 1
    assert out["rows"][0]["id"] == 9
    assert out["rows"][0]["gateway"] == "nowpayments"
    assert out["rows"][0]["amount_usd"] == 9.99


async def test_list_transactions_filters_compose_positional_binds():
    """Gateway + status + telegram_id all supplied — WHERE clause
    must use ``$1``/``$2``/``$3`` in insertion order and the final
    two binds must be LIMIT/OFFSET on ``$4``/``$5``.
    """
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=0)
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_transactions(
        gateway="nowpayments",
        status="SUCCESS",
        telegram_id=42,
        page=2,
        per_page=10,
    )

    count_sql = conn.fetchval.await_args.args[0]
    assert "WHERE gateway = $1 AND status = $2 AND telegram_id = $3" in count_sql
    # COUNT query gets only the 3 filter params, no LIMIT/OFFSET.
    count_binds = conn.fetchval.await_args.args[1:]
    assert count_binds == ("nowpayments", "SUCCESS", 42)

    list_sql = conn.fetch.await_args.args[0]
    assert "LIMIT $4 OFFSET $5" in list_sql
    list_binds = conn.fetch.await_args.args[1:]
    # page=2, per_page=10 → offset=10
    assert list_binds == ("nowpayments", "SUCCESS", 42, 10, 10)


async def test_list_transactions_rejects_unknown_gateway_and_status():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError, match="gateway"):
        await db.list_transactions(gateway="bogus")
    with pytest.raises(ValueError, match="status"):
        await db.list_transactions(status="not_a_real_state")


async def test_list_transactions_clamps_page_and_per_page():
    """``page<1`` clamps to 1, ``per_page`` is clamped to
    ``[1, TRANSACTIONS_MAX_PER_PAGE]``.
    """
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=0)
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_transactions(page=-10, per_page=9999)
    binds = conn.fetch.await_args.args[1:]
    # per_page clamped to 200, page clamped to 1 → offset 0
    assert binds == (db.TRANSACTIONS_MAX_PER_PAGE, 0)

    conn.fetch.reset_mock()
    await db.list_transactions(page=5, per_page=0)
    binds = conn.fetch.await_args.args[1:]
    # per_page clamped to 1, page=5 → offset (5-1)*1 = 4
    assert binds == (1, 4)


async def test_list_transactions_total_pages_ceiling():
    """total_pages = ceil(total / per_page); 0 when total is 0."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    # 0 → 0 pages
    conn.fetchval = AsyncMock(return_value=0)
    out = await db.list_transactions(per_page=10)
    assert out["total_pages"] == 0

    # 1 → 1 page (one row, per_page=10)
    conn.fetchval = AsyncMock(return_value=1)
    out = await db.list_transactions(per_page=10)
    assert out["total_pages"] == 1

    # 10 → 1 page
    conn.fetchval = AsyncMock(return_value=10)
    out = await db.list_transactions(per_page=10)
    assert out["total_pages"] == 1

    # 11 → 2 pages
    conn.fetchval = AsyncMock(return_value=11)
    out = await db.list_transactions(per_page=10)
    assert out["total_pages"] == 2

    # 250 with per_page=100 → 3 pages
    conn.fetchval = AsyncMock(return_value=250)
    out = await db.list_transactions(per_page=100)
    assert out["total_pages"] == 3


async def test_list_transactions_null_fields_pass_through():
    """Optional columns (``telegram_id``, ``amount_crypto_or_rial``,
    timestamps, ``notes``) may all be NULL in real rows — the
    mapper must render them as ``None`` not crash.
    """
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=1)
    conn.fetch = AsyncMock(
        return_value=[
            {
                "transaction_id": 7,
                "telegram_id": None,
                "gateway": "admin",
                "currency_used": "USD",
                "amount_crypto_or_rial": None,
                "amount_usd_credited": -2.5,
                "status": "SUCCESS",
                "gateway_invoice_id": None,
                "created_at": None,
                "completed_at": None,
                "notes": None,
            }
        ]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    out = await db.list_transactions()
    r = out["rows"][0]
    assert r["telegram_id"] is None
    assert r["amount_crypto_or_rial"] is None
    assert r["gateway_invoice_id"] is None
    assert r["created_at"] is None
    assert r["completed_at"] is None
    assert r["notes"] is None
    # Debit rows stay negative so the template can colourise.
    assert r["amount_usd"] == -2.5


# ---------------------------------------------------------------------
# Stage-9-Step-2: admin_audit_log + user-field editor + admin_telegram_id col
# ---------------------------------------------------------------------


async def test_record_admin_audit_inserts_row_and_returns_id():
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=12345)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    new_id = await db.record_admin_audit(
        actor="web",
        action="user_adjust",
        target="user:777",
        ip="203.0.113.10",
        outcome="ok",
        meta={"delta_usd": 5.0},
    )
    assert new_id == 12345
    sql = conn.fetchval.await_args.args[0]
    assert "INSERT INTO admin_audit_log" in sql
    args = conn.fetchval.await_args.args[1:]
    assert args[0] == "web"
    assert args[1] == "user_adjust"
    assert args[2] == "user:777"
    assert args[3] == "203.0.113.10"
    assert args[4] == "ok"
    # meta is JSON-serialized to text before being cast to ::jsonb in the SQL.
    import json
    assert json.loads(args[5]) == {"delta_usd": 5.0}


async def test_record_admin_audit_swallows_db_error_and_returns_none():
    """A failed audit-log write must NOT propagate — callers wrap
    in ``_record_audit_safe`` already, but defense in depth."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(side_effect=RuntimeError("pool gone"))
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.record_admin_audit(
        actor="web", action="login_ok",
    )
    assert result is None


async def test_record_admin_audit_omits_meta_when_none():
    """A row with no meta should pass NULL, not the JSON string 'null'."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=1)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.record_admin_audit(actor="web", action="login_ok")
    args = conn.fetchval.await_args.args[1:]
    assert args[5] is None  # meta param


async def test_list_admin_audit_log_default_no_filters():
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_admin_audit_log()
    sql = conn.fetch.await_args.args[0]
    assert "FROM admin_audit_log" in sql
    assert "WHERE" not in sql.split("ORDER BY")[0]
    assert conn.fetch.await_args.args[1] == 200  # default limit


async def test_list_admin_audit_log_filters_by_action_and_actor():
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_admin_audit_log(
        limit=50, action="user_adjust", actor="web",
    )
    sql = conn.fetch.await_args.args[0]
    args = conn.fetch.await_args.args[1:]
    assert "action = $1" in sql
    assert "actor = $2" in sql
    assert args == ("user_adjust", "web", 50)


async def test_update_user_admin_fields_rejects_unknown_column():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError, match="balance_usd"):
        await db.update_user_admin_fields(
            777, fields={"balance_usd": 999.99},
        )


async def test_update_user_admin_fields_rejects_empty_fields():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError, match="non-empty"):
        await db.update_user_admin_fields(777, fields={})


async def test_update_user_admin_fields_returns_none_for_missing_user():
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.update_user_admin_fields(
        777, fields={"language_code": "fa"},
    )
    assert result is None


async def test_update_user_admin_fields_builds_update_sql_for_multiple_columns():
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=777)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.update_user_admin_fields(
        777,
        fields={
            "language_code": "fa",
            "memory_enabled": True,
        },
    )
    assert result == {"changed": {"language_code": "fa", "memory_enabled": True}}
    sql = conn.fetchval.await_args.args[0]
    assert "UPDATE users SET" in sql
    assert "language_code = $1" in sql
    assert "memory_enabled = $2" in sql
    assert "WHERE telegram_id = $3" in sql
    assert conn.fetchval.await_args.args[1:] == ("fa", True, 777)


async def test_admin_adjust_balance_populates_admin_telegram_id_column():
    """Stage-9-Step-2 fix: the admin id now lives in a real column,
    not buried inside ``gateway_invoice_id``. Forensics queries do
    ``WHERE admin_telegram_id IS NOT NULL`` instead of substring matching.
    """
    conn = _make_conn()
    # Two fetchval calls: balance lookup, then the INSERT RETURNING.
    # An asyncpg fetchrow then fetchval pattern.
    conn.fetchrow = AsyncMock(return_value={"balance_usd": 10.0})
    conn.execute = AsyncMock(return_value="UPDATE 1")
    conn.fetchval = AsyncMock(return_value=999)
    # Mock transaction context
    class _TxCtx:
        async def __aenter__(self_inner): return None
        async def __aexit__(self_inner, *a): return False
    conn.transaction = MagicMock(return_value=_TxCtx())

    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.admin_adjust_balance(
        telegram_id=777,
        delta_usd=5.0,
        reason="manual top-up",
        admin_telegram_id=42,
    )
    assert result["transaction_id"] == 999

    # Find the INSERT call (the second fetchval after the row lock check).
    sql_calls = [c.args[0] for c in conn.fetchval.await_args_list]
    insert_calls = [s for s in sql_calls if "INSERT INTO transactions" in s]
    assert insert_calls, "expected an INSERT INTO transactions"
    insert_sql = insert_calls[0]
    assert "admin_telegram_id" in insert_sql

    # Find the args of that fetchval call.
    insert_args = next(
        c.args for c in conn.fetchval.await_args_list
        if "INSERT INTO transactions" in c.args[0]
    )
    # Last positional arg should be the admin id (42).
    assert insert_args[-1] == 42


# ---------------------------------------------------------------------
# Stage-9-Step-8: list_user_usage_logs + get_user_usage_aggregates
# ---------------------------------------------------------------------


async def test_list_user_usage_logs_filters_by_telegram_id():
    """The WHERE clause MUST scope to the requested user — leakage
    here would expose another user's usage history."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=0)
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_user_usage_logs(telegram_id=42, page=1, per_page=50)

    count_sql, count_bind = conn.fetchval.await_args.args
    assert "WHERE telegram_id = $1" in count_sql
    assert count_bind == 42

    list_sql, *list_binds = conn.fetch.await_args.args
    assert "WHERE telegram_id = $1" in list_sql
    assert "ORDER BY log_id DESC" in list_sql
    assert tuple(list_binds) == (42, 50, 0)


async def test_list_user_usage_logs_clamps_per_page():
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=0)
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    # Above max → clamps to USAGE_LOGS_MAX_PER_PAGE.
    await db.list_user_usage_logs(telegram_id=1, per_page=9999)
    binds = conn.fetch.await_args.args[1:]
    assert binds[1] == db.USAGE_LOGS_MAX_PER_PAGE

    # Below 1 → clamps to 1.
    conn.fetch.reset_mock()
    await db.list_user_usage_logs(telegram_id=1, per_page=0)
    binds = conn.fetch.await_args.args[1:]
    assert binds[1] == 1


async def test_list_user_usage_logs_maps_rows_with_total_tokens():
    """The mapper must compute ``total_tokens`` (prompt + completion)
    server-side so the template can render it without needing the
    raw cols."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=1)
    conn.fetch = AsyncMock(
        return_value=[
            {
                "log_id": 99,
                "model_used": "openai/gpt-4o",
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "cost_deducted_usd": 0.0042,
                "created_at": None,
            }
        ]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    out = await db.list_user_usage_logs(telegram_id=7)
    row = out["rows"][0]
    assert row["id"] == 99
    assert row["model"] == "openai/gpt-4o"
    assert row["total_tokens"] == 30
    assert row["cost_usd"] == pytest.approx(0.0042)
    assert row["created_at"] is None


async def test_get_user_usage_aggregates_returns_zeros_when_no_rows():
    """``COALESCE(..., 0)`` must absorb the empty-result case."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(
        return_value={"calls": 0, "tokens": 0, "cost": 0}
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    out = await db.get_user_usage_aggregates(telegram_id=999)
    assert out == {
        "total_calls": 0, "total_tokens": 0, "total_cost_usd": 0.0,
    }
    sql, bind = conn.fetchrow.await_args.args
    assert "WHERE telegram_id = $1" in sql
    assert bind == 999


async def test_get_user_usage_aggregates_handles_none_row():
    """Defensive: if ``fetchrow`` returns ``None`` (shouldn't with
    SUM/COUNT but belt-and-suspenders) we still return the zero
    shape rather than a TypeError on ``row[...]`` indexing."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    out = await db.get_user_usage_aggregates(telegram_id=1)
    assert out == {
        "total_calls": 0, "total_tokens": 0, "total_cost_usd": 0.0,
    }


# ---------------------------------------------------------------------
# Bug-fix sweep: defense-in-depth NaN / Infinity guards on the four
# money-handling DB methods.
#
# PR #75 closed the same hole at the IPN webhook layer; this PR adds
# the matching belt-and-suspenders at the DB layer so any future
# caller that bypasses the IPN path (a new internal call site, a
# refactor, a test stub) still can't quietly INSERT ``NaN`` into the
# wallet — which PostgreSQL silently accepts (it's a valid IEEE-754
# value) and which then bricks every subsequent balance comparison
# (``balance_usd >= $1`` is always ``False`` for ``NaN``).
# ---------------------------------------------------------------------


def test_is_finite_amount_helper_basic():
    """The pure helper used by all four guarded methods."""
    f = database_module._is_finite_amount
    assert f(0) is True
    assert f(0.0) is True
    assert f(1.5) is True
    assert f(-1.5) is True
    assert f(1_000_000) is True
    assert f(float("nan")) is False
    assert f(float("inf")) is False
    assert f(float("-inf")) is False
    assert f("nan") is False
    assert f("inf") is False
    assert f("not a number") is False
    assert f(None) is False


async def test_deduct_balance_refuses_nan_cost():
    """``balance_usd >= $1`` would silently return no rows for NaN
    (the WHERE comparison is always False), masking the bug. We refuse
    BEFORE issuing the SQL so the log line points at the bad caller."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.deduct_balance(telegram_id=777, cost_usd=float("nan"))
    assert result is False
    conn.fetchval.assert_not_awaited()


async def test_deduct_balance_refuses_negative_infinity_cost():
    """Negative infinity is the more dangerous case: it would *match*
    the WHERE clause for any finite balance and then write
    ``balance_usd - (-inf) = inf`` into the row, bricking the wallet.
    """
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.deduct_balance(telegram_id=777, cost_usd=float("-inf"))
    assert result is False
    conn.fetchval.assert_not_awaited()


async def test_deduct_balance_refuses_positive_infinity_cost():
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.deduct_balance(telegram_id=777, cost_usd=float("inf"))
    assert result is False
    conn.fetchval.assert_not_awaited()


async def test_deduct_balance_finite_zero_cost_still_runs_sql():
    """Regression pin: a $0 cost (free message that still settles
    through the paid path) is still a valid call — only NaN / Infinity
    short-circuit. The WHERE clause naturally accepts 0 deductions."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=10.0)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.deduct_balance(telegram_id=777, cost_usd=0.0)
    assert result is True
    conn.fetchval.assert_awaited_once()


async def test_deduct_balance_finite_positive_cost_still_runs_sql():
    """Regression pin: the happy path still issues the UPDATE."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=4.5)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.deduct_balance(telegram_id=777, cost_usd=0.5)
    assert result is True
    sql, cost, tg = conn.fetchval.await_args.args
    assert "UPDATE users" in sql
    assert "balance_usd >= $1" in sql
    assert cost == 0.5
    assert tg == 777


async def test_deduct_balance_refuses_finite_negative_cost():
    """A finite negative ``cost_usd`` would flip the SQL
    ``SET balance_usd = balance_usd - $1`` into a *credit* (the WHERE
    clause ``balance_usd >= -N`` is True for every solvent wallet) and
    write the credit without a ``transactions`` ledger row — bypassing
    the audit trail. ``pricing._apply_markup`` clamps cost to ``[0,
    ∞)`` upstream today, but that clamp lives one module away; defend
    in depth here so a future caller bypassing it can't credit users
    silently. We refuse BEFORE issuing the SQL so the log line points
    at the bad caller."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.deduct_balance(telegram_id=777, cost_usd=-5.0)
    assert result is False
    conn.fetchval.assert_not_awaited()


async def test_deduct_balance_refuses_negative_cost_smallest_magnitude():
    """A near-zero negative cost (``-0.0001``) is just as much an
    audit-trail regression as a large one — refuse anything strictly
    below zero. ``-0.0`` is treated as zero (Python ``-0.0 < 0`` is
    ``False``) so the "free message via paid path" call site that
    routes a $0 settlement still goes through unchanged."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.deduct_balance(telegram_id=777, cost_usd=-0.0001)
    assert result is False
    conn.fetchval.assert_not_awaited()


async def test_deduct_balance_negative_zero_treated_as_zero():
    """``-0.0`` is an IEEE-754 oddity but ``-0.0 < 0`` is ``False`` in
    Python, so the negative guard MUST NOT short-circuit it — the
    free-message-via-paid-path settlement (cost=0) still has to issue
    the UPDATE so the test in ``test_ai_engine.py`` that pins
    ``log_usage(cost=0)`` against ``deduct_balance`` returning True
    keeps holding."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=10.0)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.deduct_balance(telegram_id=777, cost_usd=-0.0)
    assert result is True
    conn.fetchval.assert_awaited_once()


async def test_admin_adjust_balance_raises_on_nan_delta():
    """``new_balance < 0`` is False for NaN, so the existing guard
    would let the NaN slip through and write into the wallet. The new
    guard upgrades this to a ValueError BEFORE the FOR UPDATE lock so
    the caller's error path runs immediately."""
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with pytest.raises(ValueError, match="finite"):
        await db.admin_adjust_balance(
            telegram_id=777,
            delta_usd=float("nan"),
            reason="manual",
            admin_telegram_id=42,
        )
    conn.fetchrow.assert_not_awaited()


async def test_admin_adjust_balance_raises_on_positive_infinity_delta():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with pytest.raises(ValueError, match="finite"):
        await db.admin_adjust_balance(
            telegram_id=777,
            delta_usd=float("inf"),
            reason="manual",
            admin_telegram_id=42,
        )
    conn.fetchrow.assert_not_awaited()


async def test_admin_adjust_balance_raises_on_negative_infinity_delta():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with pytest.raises(ValueError, match="finite"):
        await db.admin_adjust_balance(
            telegram_id=777,
            delta_usd=float("-inf"),
            reason="manual",
            admin_telegram_id=42,
        )
    conn.fetchrow.assert_not_awaited()


async def test_admin_adjust_balance_still_raises_on_zero_delta():
    """Regression pin: the existing zero-delta guard is preserved
    (independent of the new finite check) — the error message is
    distinct."""
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with pytest.raises(ValueError, match="non-zero"):
        await db.admin_adjust_balance(
            telegram_id=777,
            delta_usd=0,
            reason="manual",
            admin_telegram_id=42,
        )


async def test_finalize_payment_refuses_nan_full_price():
    """Defense-in-depth: PR #75 already validates at the IPN layer,
    but the DB function refuses too so a future internal caller can't
    silently brick a wallet."""
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.finalize_payment(
        gateway_invoice_id="np-1", full_price_usd=float("nan")
    )
    assert result is None
    conn.fetchrow.assert_not_awaited()


async def test_finalize_payment_refuses_infinity_full_price():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.finalize_payment(
        gateway_invoice_id="np-1", full_price_usd=float("inf")
    )
    assert result is None
    conn.fetchrow.assert_not_awaited()


async def test_finalize_payment_refuses_negative_full_price():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.finalize_payment(
        gateway_invoice_id="np-1", full_price_usd=-5.0
    )
    assert result is None
    conn.fetchrow.assert_not_awaited()


async def test_finalize_payment_refuses_zero_full_price():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.finalize_payment(
        gateway_invoice_id="np-1", full_price_usd=0.0
    )
    assert result is None
    conn.fetchrow.assert_not_awaited()


async def test_finalize_partial_payment_refuses_nan_actually_paid():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.finalize_partial_payment(
        gateway_invoice_id="np-1", actually_paid_usd=float("nan")
    )
    assert result is None
    conn.fetchrow.assert_not_awaited()


async def test_finalize_partial_payment_refuses_infinity_actually_paid():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.finalize_partial_payment(
        gateway_invoice_id="np-1", actually_paid_usd=float("inf")
    )
    assert result is None
    conn.fetchrow.assert_not_awaited()


async def test_finalize_partial_payment_refuses_zero_actually_paid():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.finalize_partial_payment(
        gateway_invoice_id="np-1", actually_paid_usd=0.0
    )
    assert result is None
    conn.fetchrow.assert_not_awaited()


async def test_finalize_partial_payment_refuses_negative_actually_paid():
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.finalize_partial_payment(
        gateway_invoice_id="np-1", actually_paid_usd=-1.0
    )
    assert result is None
    conn.fetchrow.assert_not_awaited()


# ---------------------------------------------------------------------
# redeem_gift_code: read-side non-finite guard (this PR)
# ---------------------------------------------------------------------


def _make_redeem_conn(amount_usd):
    """Connection stub that walks ``redeem_gift_code`` to the credit
    UPDATE — every eligibility branch passes — so the only thing we're
    verifying is what happens when the row's stored ``amount_usd`` is
    ``amount_usd``.
    """
    conn = MagicMock()
    # gift_codes row → user_exists lookup → tx_id INSERT RETURNING →
    # new_balance UPDATE RETURNING. ``amount_usd`` is the first read.
    conn.fetchrow = AsyncMock(return_value={
        "amount_usd": amount_usd,
        "max_uses": None,
        "used_count": 0,
        "expires_at": None,
        "is_active": True,
    })
    # 1) already_used probe → None (no prior redemption)
    # 2) user_exists probe → 1
    # 3) tx_id INSERT RETURNING → 12345
    # 4) new_balance UPDATE RETURNING → 99.99 (only reached if guard fails)
    conn.fetchval = AsyncMock(side_effect=[None, 1, 12345, 99.99])
    conn.execute = AsyncMock(return_value="UPDATE 1")

    class _TxCtx:
        async def __aenter__(self_inner):
            return None

        async def __aexit__(self_inner, *a):
            return False

    conn.transaction = MagicMock(return_value=_TxCtx())
    return conn


async def test_redeem_gift_code_refuses_nan_amount_in_row():
    """Defense-in-depth: a corrupted ``gift_codes.amount_usd``
    holding ``'NaN'::numeric`` must NOT credit the user. Every
    upstream comparison on NaN returns ``False`` (``NaN <= 0`` is
    ``False``, etc.) and PostgreSQL stores NaN happily, so a row
    that predates the create-side guard (PR #86) — or one inserted
    by a manual SQL fix, a future migration mishap, or any other
    path bypassing ``create_gift_code`` — would feed NaN into
    ``UPDATE users SET balance_usd = balance_usd + NaN`` and brick
    the wallet exactly the way PR #75 / #77 prevented at the IPN
    layer. This test pins the read-side guard so a future refactor
    can't quietly drop it.
    """
    conn = _make_redeem_conn(float("nan"))
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with pytest.raises(ValueError, match="finite"):
        await db.redeem_gift_code("GIFT5", 777)
    # The wallet UPDATE must NOT have been issued.
    update_calls = [
        c for c in conn.fetchval.await_args_list
        if "UPDATE users" in c.args[0]
    ]
    assert update_calls == [], (
        "wallet UPDATE issued despite non-finite gift amount "
        "(transaction should have been rolled back before credit)"
    )


async def test_redeem_gift_code_refuses_positive_infinity_amount_in_row():
    """``+Infinity`` would land in the ledger as
    ``amount_usd_credited = Inf`` and propagate through
    ``balance_usd + Inf`` the same way NaN does. Reject at the
    read-side for the same reason — the create-side guard catches
    new rows; this catches legacy / manually-edited rows.
    """
    conn = _make_redeem_conn(float("inf"))
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with pytest.raises(ValueError, match="finite"):
        await db.redeem_gift_code("GIFT5", 777)


async def test_redeem_gift_code_refuses_negative_infinity_amount_in_row():
    """``-Infinity`` is the dual of ``+Infinity``. Catch it for
    completeness so a future refactor that changes the sign-handling
    upstream doesn't quietly re-open the hole.
    """
    conn = _make_redeem_conn(float("-inf"))
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with pytest.raises(ValueError, match="finite"):
        await db.redeem_gift_code("GIFT5", 777)


async def test_redeem_gift_code_finite_amount_proceeds_to_credit():
    """Sanity-check the happy path: a finite ``amount_usd`` does
    NOT raise — the guard only fires on NaN / ±Infinity. Pins the
    contract so a typo in the new check (e.g. inverted ``not``)
    can't silently break every redemption.
    """
    conn = _make_redeem_conn(5.0)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.redeem_gift_code("GIFT5", 777)
    assert result["status"] == "ok"
    assert result["amount_usd"] == 5.0
    assert result["transaction_id"] == 12345


# ---------------------------------------------------------------------
# log_usage: defense-in-depth non-finite / negative guard (this PR)
# ---------------------------------------------------------------------


async def test_log_usage_skips_insert_for_nan_cost(caplog):
    """Defense-in-depth: a NaN ``cost`` would land in
    ``usage_logs.cost_deducted_usd`` and poison every aggregate
    (dashboard ``spend_usd`` tile, ``top_models`` per-model totals,
    ``get_user_usage_aggregates``). PG ``NUMERIC`` accepts
    ``'NaN'::numeric`` happily and there's no CHECK constraint.
    Refuse at the DB layer with the same shape as ``deduct_balance``
    — log error + skip the INSERT. The user's reply is preserved
    (``log_usage`` is fire-and-forget from ``chat_with_model``);
    we just don't poison the table.
    """
    import logging

    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with caplog.at_level(logging.ERROR, logger="bot.database"):
        await db.log_usage(
            telegram_id=42, model="openai/gpt-4o-mini",
            prompt_tokens=10, completion_tokens=20,
            cost=float("nan"),
        )
    conn.execute.assert_not_awaited()
    assert any("log_usage refused" in rec.message for rec in caplog.records)


async def test_log_usage_skips_insert_for_positive_infinity_cost():
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.log_usage(
        telegram_id=42, model="openai/gpt-4o-mini",
        prompt_tokens=10, completion_tokens=20,
        cost=float("inf"),
    )
    conn.execute.assert_not_awaited()


async def test_log_usage_skips_insert_for_negative_cost():
    """A finite-negative ``cost`` slips past the non-finite check
    but would still under-count the spend tile (``SUM`` would go
    negative for that bucket). Refuse with the same log + return
    shape so the only paths into ``usage_logs`` are non-negative.
    """
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.log_usage(
        telegram_id=42, model="openai/gpt-4o-mini",
        prompt_tokens=10, completion_tokens=20,
        cost=-0.001,
    )
    conn.execute.assert_not_awaited()


async def test_log_usage_zero_cost_still_inserts():
    """``cost == 0.0`` is the legitimate free-message-via-paid-path
    settlement (``chat_with_model`` calls through with cost=0 to
    keep ``log_usage`` honest about the call). MUST insert so the
    usage log is complete.
    """
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.log_usage(
        telegram_id=42, model="openai/gpt-4o-mini",
        prompt_tokens=10, completion_tokens=20,
        cost=0.0,
    )
    conn.execute.assert_awaited_once()


async def test_log_usage_finite_positive_cost_inserts():
    """Sanity: the happy path still works. Pins the contract so a
    typo in the new check (e.g. inverted ``not``) can't silently
    break every paid-message log.
    """
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.log_usage(
        telegram_id=42, model="openai/gpt-4o-mini",
        prompt_tokens=10, completion_tokens=20,
        cost=0.0042,
    )
    conn.execute.assert_awaited_once()


# ---------------------------------------------------------------------
# get_system_metrics: pending_payments tile (Stage-9-Step-9)
# ---------------------------------------------------------------------


async def test_get_system_metrics_includes_pending_payments_count():
    """The new tile reads from ``transactions WHERE status='PENDING'``
    via ``fetchrow`` — it must NOT include any other status (a
    SUCCESS or PARTIAL row in the bucket would inflate the alert).
    """
    conn = _make_conn()
    conn.fetchval = AsyncMock(side_effect=lambda *a, **k: 0)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value={
        "count": 3, "oldest_age_hours": 2.5,
    })

    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_system_metrics()

    assert result["pending_payments_count"] == 3
    assert result["pending_payments_oldest_age_hours"] == 2.5

    # Pin the SQL: filter MUST be exactly status='PENDING'.
    pending_sql = conn.fetchrow.await_args.args[0]
    normalized = " ".join(pending_sql.split())
    assert "WHERE status = 'PENDING'" in normalized, (
        f"pending-payments query doesn't filter by PENDING "
        f"(got: {normalized!r})"
    )
    assert "FROM transactions" in normalized


async def test_get_system_metrics_pending_zero_returns_none_age():
    """When zero pending rows exist, ``MIN(created_at)`` is NULL
    and the EXTRACT yields NULL too — surface as Python ``None``
    (not 0.0!) so the template can hide the "oldest Xh" sub-label
    cleanly.
    """
    conn = _make_conn()
    conn.fetchval = AsyncMock(side_effect=lambda *a, **k: 0)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value={
        "count": 0, "oldest_age_hours": None,
    })

    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_system_metrics()
    assert result["pending_payments_count"] == 0
    assert result["pending_payments_oldest_age_hours"] is None
