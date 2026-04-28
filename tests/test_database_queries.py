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

    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_system_metrics()

    assert set(result.keys()) == {
        "users_total", "users_active_7d", "revenue_usd",
        "spend_usd", "top_models",
    }
    assert result["users_total"] == 10
    assert result["users_active_7d"] == 3
    assert result["revenue_usd"] == 100.0
    assert result["spend_usd"] == 4.20
    assert result["top_models"] == []


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
