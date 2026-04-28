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
