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
    # Stage-12-Step-B: ``over_threshold_count`` joined onto the same
    # row so the dashboard tile and the proactive alert loop share
    # one canonical "stuck" definition.
    conn.fetchrow = AsyncMock(return_value={
        "count": 0,
        "oldest_age_hours": None,
        "over_threshold_count": 0,
    })

    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_system_metrics()

    assert set(result.keys()) == {
        "users_total", "users_active_7d", "revenue_usd",
        "spend_usd", "top_models",
        "pending_payments_count", "pending_payments_oldest_age_hours",
        "pending_payments_over_threshold_count",
        "pending_alert_threshold_hours",
    }
    assert result["users_total"] == 10
    assert result["users_active_7d"] == 3
    assert result["revenue_usd"] == 100.0
    assert result["spend_usd"] == 4.20
    assert result["top_models"] == []
    assert result["pending_payments_count"] == 0
    assert result["pending_payments_oldest_age_hours"] is None
    assert result["pending_payments_over_threshold_count"] == 0
    # Default threshold = 2 h (mirrors PENDING_ALERT_THRESHOLD_HOURS
    # default in pending_alert.py).
    assert result["pending_alert_threshold_hours"] == 2


# ---------------------------------------------------------------------
# get_monetization_summary (Stage-15-Step-E #9)
# ---------------------------------------------------------------------


async def test_get_monetization_summary_returns_shape(monkeypatch):
    """Sanity: the returned dict has every key the
    ``/admin/monetization`` template consumes, and the markup
    arithmetic flows through correctly. We pin a 2.0× markup so the
    "OpenRouter cost = charged / markup" relation has a tidy answer
    independent of the env default.
    """
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 2.0)

    conn = _make_conn()
    # fetchval order in the implementation:
    # revenue_total, charged_total, revenue_window, charged_window.
    fetchval_vals = iter([100.0, 60.0, 40.0, 24.0])
    conn.fetchval = AsyncMock(
        side_effect=lambda *a, **k: next(fetchval_vals)
    )
    conn.fetch = AsyncMock(return_value=[])

    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_monetization_summary()

    assert set(result.keys()) == {
        "markup", "lifetime", "window", "by_model", "top_users",
    }
    assert result["markup"] == 2.0

    expected_block_keys = {
        "revenue_usd", "charged_usd", "openrouter_cost_usd",
        "gross_margin_usd", "gross_margin_pct", "net_profit_usd",
    }
    assert set(result["lifetime"].keys()) == expected_block_keys
    # Window block has the same five money keys plus ``days``.
    assert set(result["window"].keys()) == expected_block_keys | {"days"}
    assert result["window"]["days"] == 30  # default

    # Lifetime: revenue=$100, charged=$60, markup=2.0 →
    # OpenRouter cost = $30, gross margin = $30, pct = 50%,
    # net = revenue − OR cost = $70.
    lifetime = result["lifetime"]
    assert lifetime["revenue_usd"] == 100.0
    assert lifetime["charged_usd"] == 60.0
    assert lifetime["openrouter_cost_usd"] == pytest.approx(30.0)
    assert lifetime["gross_margin_usd"] == pytest.approx(30.0)
    assert lifetime["gross_margin_pct"] == pytest.approx(50.0)
    assert lifetime["net_profit_usd"] == pytest.approx(70.0)

    # Window: revenue=$40, charged=$24, markup=2.0 →
    # OR cost = $12, margin = $12, pct = 50%, net = $28.
    window = result["window"]
    assert window["revenue_usd"] == 40.0
    assert window["charged_usd"] == 24.0
    assert window["openrouter_cost_usd"] == pytest.approx(12.0)
    assert window["gross_margin_usd"] == pytest.approx(12.0)
    assert window["gross_margin_pct"] == pytest.approx(50.0)
    assert window["net_profit_usd"] == pytest.approx(28.0)


async def test_get_monetization_summary_revenue_excludes_admin_and_gift(
    monkeypatch,
):
    """The revenue rollup must use the same gateway filter as
    ``get_system_metrics`` — ``admin`` (manual credit) and ``gift``
    (gift-code redemption) are free credit issued from nothing, NOT
    real earnings, and have been excluded from the dashboard's
    "Total revenue" tile since Stage-8-Part-4. The monetization page
    has to inherit that filter or the operator looking at the new
    page will see *higher* revenue than the dashboard, which is
    exactly the kind of cross-surface drift we want to avoid.
    """
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 1.5)
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_monetization_summary()

    sqls = [call.args[0] for call in conn.fetchval.await_args_list]
    revenue_sqls = [
        s for s in sqls if "amount_usd_credited" in s and "SUCCESS" in s
    ]
    # Two revenue queries (lifetime + window); both must carry the
    # same gateway-exclusion clause.
    assert len(revenue_sqls) == 2, (
        f"expected 2 revenue queries, got {len(revenue_sqls)}: {revenue_sqls}"
    )
    for sql in revenue_sqls:
        normalized = " ".join(sql.split())
        assert "gateway NOT IN ('admin', 'gift')" in normalized, (
            "revenue query missing the admin/gift exclusion: "
            f"{normalized!r}"
        )


async def test_get_monetization_summary_window_query_uses_interval(
    monkeypatch,
):
    """The trailing-window queries must filter by
    ``created_at >= NOW() - $::interval`` (transactions falls back to
    ``COALESCE(completed_at, created_at)`` since a row created right
    at the window boundary but completed inside it should still
    count).
    """
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 1.5)
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_monetization_summary(window_days=7)

    sqls = [call.args[0] for call in conn.fetchval.await_args_list]
    bind_args = [tuple(call.args[1:]) for call in conn.fetchval.await_args_list]
    # Two window queries; both must bind ``"7 days"`` as the
    # interval text.
    window_sqls = [s for s in sqls if "$1::interval" in s]
    assert len(window_sqls) == 2, (
        f"expected 2 window queries, got {len(window_sqls)}: {window_sqls}"
    )
    for binds in bind_args:
        if not binds:
            continue
        # The window queries' first positional bind is the interval.
        if binds[0] == "7 days":
            break
    else:
        raise AssertionError(
            f"no fetchval call bound '7 days' as the interval: "
            f"{bind_args!r}"
        )


async def test_get_monetization_summary_by_model_sorted_by_charged_desc(
    monkeypatch,
):
    """The per-model breakdown must rank by *charged USD desc*, not
    by request count (the dashboard's existing ``top_models`` tile
    already does request-count). Picking the same sort criterion
    on both pages would just duplicate the dashboard tile — the
    monetization page is a different question ("which models are
    earning money?") and needs a different ranking.
    """
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 2.0)
    conn = _make_conn()
    # The implementation issues two ``fetch`` calls: one for
    # ``by_model`` then one for ``top_users``. Route them by SQL
    # body so the ordering inside the method is the test's
    # business; this also documents the sequence for readers.
    def _fake_fetch(sql, *args, **kwargs):
        normalized = " ".join(sql.split())
        if "FROM usage_logs" in normalized and "GROUP BY model_used" in normalized:
            return [
                {"model": "openai/gpt-4o", "requests": 10, "charged_usd": 4.0},
                {"model": "anthropic/sonnet", "requests": 100, "charged_usd": 1.0},
            ]
        return []
    conn.fetch = AsyncMock(side_effect=_fake_fetch)
    conn.fetchval = AsyncMock(return_value=0)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_monetization_summary()

    fetched_sqls = [c.args[0] for c in conn.fetch.await_args_list]
    by_model_sql = next(
        s for s in fetched_sqls
        if "FROM usage_logs" in s and "GROUP BY model_used" in s
    )
    normalized = " ".join(by_model_sql.split())
    assert "ORDER BY charged_usd DESC" in normalized, (
        "per-model query must rank by charged USD descending; "
        f"got: {normalized!r}"
    )
    # Returned rows preserve the DB sort and carry the derived
    # OpenRouter-cost / margin columns.
    assert [r["model"] for r in result["by_model"]] == [
        "openai/gpt-4o", "anthropic/sonnet",
    ]
    first = result["by_model"][0]
    assert first["charged_usd"] == 4.0
    assert first["openrouter_cost_usd"] == pytest.approx(2.0)
    assert first["gross_margin_usd"] == pytest.approx(2.0)


async def test_get_monetization_summary_handles_unity_markup(monkeypatch):
    """``markup = 1.0`` is a legitimate "operating at-cost" config
    (every charged dollar pays exactly for OpenRouter, no profit).
    The percentage must be exactly 0% — not NaN, not divide-by-zero —
    and the gross-margin USD must be exactly zero too. Mirrors the
    ``pricing.get_markup`` clamp to ``>= 1.0``.
    """
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 1.0)
    conn = _make_conn()
    fetchval_vals = iter([100.0, 50.0, 30.0, 20.0])
    conn.fetchval = AsyncMock(
        side_effect=lambda *a, **k: next(fetchval_vals)
    )
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_monetization_summary()

    assert result["markup"] == 1.0
    assert result["lifetime"]["openrouter_cost_usd"] == pytest.approx(50.0)
    assert result["lifetime"]["gross_margin_usd"] == pytest.approx(0.0)
    assert result["lifetime"]["gross_margin_pct"] == 0.0
    # Net = revenue - OR cost = 100 - 50 = 50 (we still credited
    # users $50 of headroom that hasn't been burned yet).
    assert result["lifetime"]["net_profit_usd"] == pytest.approx(50.0)


async def test_get_monetization_summary_rejects_non_positive_window():
    """A buggy caller passing ``window_days=0`` would silently turn
    every "last N days" query into "the empty interval". Fail loudly.
    """
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())

    with pytest.raises(ValueError):
        await db.get_monetization_summary(window_days=0)
    with pytest.raises(ValueError):
        await db.get_monetization_summary(window_days=-7)
    with pytest.raises(ValueError):
        await db.get_monetization_summary(top_models_limit=0)
    with pytest.raises(ValueError):
        await db.get_monetization_summary(top_users_limit=0)
    with pytest.raises(ValueError):
        await db.get_monetization_summary(top_users_limit=-3)


# ---------------------------------------------------------------------
# get_monetization_summary — top-users-by-revenue panel
# (Stage-15-Step-E #9 follow-up)
# ---------------------------------------------------------------------


def _route_monetization_fetch(*, by_model_rows=None, top_users_rows=None):
    """Return a ``side_effect`` callable that routes the two
    ``connection.fetch`` calls inside ``get_monetization_summary``
    (one for ``by_model``, one for ``top_users``) by SQL body. The
    test-side dispatch keeps the production code free of any
    test-only marker.
    """
    by_model_rows = by_model_rows or []
    top_users_rows = top_users_rows or []

    def _fake_fetch(sql, *args, **kwargs):
        normalized = " ".join(sql.split())
        if (
            "FROM usage_logs" in normalized
            and "GROUP BY model_used" in normalized
        ):
            return list(by_model_rows)
        if "FROM transactions" in normalized and "GROUP BY t.telegram_id" in normalized:
            return list(top_users_rows)
        return []

    return _fake_fetch


async def test_get_monetization_summary_top_users_returns_shape(monkeypatch):
    """Top users panel surfaces telegram_id, username, revenue,
    topup_count and per-window charged_usd. Sort is preserved
    from the SQL (revenue DESC, telegram_id ASC).
    """
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 2.0)

    conn = _make_conn()
    conn.fetch = AsyncMock(
        side_effect=_route_monetization_fetch(
            top_users_rows=[
                {
                    "telegram_id": 100,
                    "username": "alice",
                    "revenue_usd": 80.0,
                    "topup_count": 4,
                    "charged_usd": 30.0,
                },
                {
                    "telegram_id": 200,
                    "username": None,
                    "revenue_usd": 50.0,
                    "topup_count": 1,
                    "charged_usd": 0.0,
                },
            ],
        )
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_monetization_summary()

    assert "top_users" in result
    assert len(result["top_users"]) == 2
    first = result["top_users"][0]
    assert first == {
        "telegram_id": 100,
        "username": "alice",
        "revenue_usd": 80.0,
        "topup_count": 4,
        "charged_usd": 30.0,
    }
    # username=None is preserved (NOT coerced to "" in the DB
    # layer — that's the template's call).
    second = result["top_users"][1]
    assert second["username"] is None
    assert second["telegram_id"] == 200
    assert second["topup_count"] == 1


async def test_get_monetization_summary_top_users_sql_filters(monkeypatch):
    """The top_users SQL must:

    1. Filter by SUCCESS / PARTIAL — same as the scope-level
       revenue rollup (so per-user sums to ≤ window revenue).
    2. Exclude admin / gift gateways — same exclusion as the
       dashboard's revenue tile.
    3. Use COALESCE(completed_at, created_at) for the window
       boundary so a row that completed inside the window but
       was created earlier still counts.
    4. Sort by revenue DESC, then telegram_id ASC for stability.
    """
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 1.5)

    conn = _make_conn()
    conn.fetch = AsyncMock(side_effect=_route_monetization_fetch())
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_monetization_summary(window_days=14, top_users_limit=5)

    fetched_sqls = [c.args[0] for c in conn.fetch.await_args_list]
    fetched_args = [c.args[1:] for c in conn.fetch.await_args_list]
    top_users_sql = next(
        s for s in fetched_sqls
        if "FROM transactions" in s and "GROUP BY t.telegram_id" in s
    )
    normalized = " ".join(top_users_sql.split())

    assert "status IN ('SUCCESS', 'PARTIAL')" in normalized, (
        f"top_users SQL must filter by SUCCESS/PARTIAL: {normalized!r}"
    )
    assert "gateway NOT IN ('admin', 'gift')" in normalized, (
        f"top_users SQL must exclude admin/gift: {normalized!r}"
    )
    assert "COALESCE(t.completed_at, t.created_at)" in normalized, (
        "top_users window filter must coalesce completed_at to "
        f"created_at: {normalized!r}"
    )
    assert "ORDER BY revenue_usd DESC, t.telegram_id ASC" in normalized, (
        f"top_users SQL must sort revenue DESC, tid ASC: {normalized!r}"
    )
    assert "LEFT JOIN users u ON u.telegram_id = t.telegram_id" in normalized, (
        "top_users must LEFT JOIN users so a deleted account still "
        f"surfaces: {normalized!r}"
    )

    # Bind args for the top_users query: ($1=interval text, $2=limit).
    top_users_idx = fetched_sqls.index(top_users_sql)
    top_users_binds = fetched_args[top_users_idx]
    assert top_users_binds == ("14 days", 5), (
        f"top_users SQL must bind ('14 days', 5); got {top_users_binds!r}"
    )


async def test_get_monetization_summary_top_users_charged_subquery_uses_window(
    monkeypatch,
):
    """The per-user ``charged_usd`` subquery must scope the
    wallet-charge sum to the SAME trailing window as the outer
    revenue filter — operators reading the panel expect the two
    USD figures to share a time slice.
    """
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 1.5)

    conn = _make_conn()
    conn.fetch = AsyncMock(side_effect=_route_monetization_fetch())
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_monetization_summary(window_days=7)

    top_users_sql = next(
        c.args[0] for c in conn.fetch.await_args_list
        if "FROM transactions" in c.args[0]
        and "GROUP BY t.telegram_id" in c.args[0]
    )
    normalized = " ".join(top_users_sql.split())
    # The charged_usd subquery must filter usage_logs to the
    # same window. We bind the window interval ONCE
    # (``$1::interval``) and re-use it inside the subquery.
    assert (
        "ul.created_at >= NOW() - $1::interval" in normalized
    ), (
        "top_users charged_usd subquery must scope to the same "
        f"window interval: {normalized!r}"
    )


async def test_get_monetization_summary_top_users_handles_decimal(monkeypatch):
    """asyncpg returns NUMERIC columns as ``Decimal`` by default;
    the production code must coerce to ``float`` so the JSON / CSV
    serializers don't choke. Mirrors the same defence in
    ``get_user_spending_summary``.
    """
    from decimal import Decimal
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 2.0)

    conn = _make_conn()
    conn.fetch = AsyncMock(
        side_effect=_route_monetization_fetch(
            top_users_rows=[
                {
                    "telegram_id": 999,
                    "username": "decimal-user",
                    "revenue_usd": Decimal("12.3400"),
                    "topup_count": 2,
                    "charged_usd": Decimal("4.5000"),
                },
            ],
        )
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_monetization_summary()

    row = result["top_users"][0]
    assert isinstance(row["revenue_usd"], float)
    assert isinstance(row["charged_usd"], float)
    assert isinstance(row["telegram_id"], int)
    assert isinstance(row["topup_count"], int)
    assert row["revenue_usd"] == pytest.approx(12.34)
    assert row["charged_usd"] == pytest.approx(4.5)


async def test_get_monetization_summary_top_users_empty_when_no_revenue(
    monkeypatch,
):
    """A fresh deployment with no paid top-ups in the window
    returns ``top_users=[]`` rather than crashing or emitting a
    placeholder row. The page renders an "empty state" panel for
    that case (template-side).
    """
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 1.5)
    conn = _make_conn()
    conn.fetch = AsyncMock(side_effect=_route_monetization_fetch())
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_monetization_summary()
    assert result["top_users"] == []


async def test_get_monetization_summary_top_users_default_limit_is_10(
    monkeypatch,
):
    """The default top_users_limit matches the on-page panel cap
    (10). Operators wanting the long tail go through the CSV
    export, which uses a wider limit (1000)."""
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 1.5)
    conn = _make_conn()
    conn.fetch = AsyncMock(side_effect=_route_monetization_fetch())
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_monetization_summary()

    top_users_call = next(
        c for c in conn.fetch.await_args_list
        if "FROM transactions" in c.args[0]
        and "GROUP BY t.telegram_id" in c.args[0]
    )
    # ($1=interval, $2=limit).
    assert top_users_call.args[2] == 10


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


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 28 bundled bug fix:
# _scrub_audit_meta keeps the audit feed lossless across Decimal /
# datetime / non-finite-float meta payloads.
# ---------------------------------------------------------------------


async def test_record_admin_audit_serializes_decimal_meta():
    """A caller passing ``Decimal`` (every money-handling code path
    produces these from asyncpg ``numeric`` reads) used to crash
    the JSON dump and silently lose the audit row. After the
    Step-E #10b row 28 scrub fix, the value round-trips as a
    JSON ``float``."""
    import json
    from decimal import Decimal

    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=42)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    new_id = await db.record_admin_audit(
        actor="web",
        action="refund_issued",
        meta={
            "telegram_id": 123,
            "amount_refunded_usd": Decimal("5.25"),
            "new_balance_usd": Decimal("10.00"),
        },
    )
    assert new_id == 42
    args = conn.fetchval.await_args.args[1:]
    decoded = json.loads(args[5])
    assert decoded == {
        "telegram_id": 123,
        "amount_refunded_usd": 5.25,
        "new_balance_usd": 10.0,
    }


async def test_record_admin_audit_drops_nan_and_infinity():
    """NaN / ±Infinity in a meta float used to land as
    Postgres-rejected ``NaN`` / ``Infinity`` JSON literals (not
    standard JSON), causing the whole audit row to vanish on the
    ``::jsonb`` cast. After the scrub fix, those values are
    coerced to JSON ``null`` so the row survives — the operator
    can still see "we tried to log this" and the affected fields
    are explicit nulls."""
    import json
    import math

    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=99)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    new_id = await db.record_admin_audit(
        actor="web",
        action="bot_health",
        meta={
            "score": math.nan,
            "ok_score": 0.85,
            "infinity": math.inf,
            "neg_infinity": -math.inf,
        },
    )
    assert new_id == 99
    args = conn.fetchval.await_args.args[1:]
    decoded = json.loads(args[5])
    assert decoded == {
        "score": None,
        "ok_score": 0.85,
        "infinity": None,
        "neg_infinity": None,
    }


async def test_record_admin_audit_serializes_datetime_meta():
    """``datetime`` / ``date`` / ``time`` in meta used to crash
    ``json.dumps`` with ``TypeError`` and silently lose the row.
    After the scrub fix, they round-trip as ISO-8601 strings."""
    import datetime
    import json

    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=7)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    when = datetime.datetime(2025, 4, 15, 12, 30, 45)
    delta = datetime.timedelta(seconds=90)
    await db.record_admin_audit(
        actor="reaper",
        action="pending_expired",
        meta={
            "ts": when,
            "interval": delta,
            "date_only": datetime.date(2025, 4, 15),
        },
    )
    args = conn.fetchval.await_args.args[1:]
    decoded = json.loads(args[5])
    assert decoded["ts"] == "2025-04-15T12:30:45"
    assert decoded["interval"] == 90.0
    assert decoded["date_only"] == "2025-04-15"


async def test_record_admin_audit_recursively_scrubs_nested_dict_and_list():
    """Nested structures inherit the scrub policy so a caller can
    pass arbitrary diff payloads without per-call coercion."""
    import json
    from decimal import Decimal

    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=1)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.record_admin_audit(
        actor="web",
        action="referral_update",
        meta={
            "before": {"percent": Decimal("10"), "max_usd": Decimal("5")},
            "after": {"percent": Decimal("12.5"), "max_usd": Decimal("7.5")},
            "targets": ("alice", "bob"),  # tuple → list
            "snapshot": [Decimal("0.1"), Decimal("0.2")],
        },
    )
    args = conn.fetchval.await_args.args[1:]
    decoded = json.loads(args[5])
    assert decoded["before"] == {"percent": 10.0, "max_usd": 5.0}
    assert decoded["after"] == {"percent": 12.5, "max_usd": 7.5}
    assert decoded["targets"] == ["alice", "bob"]
    assert decoded["snapshot"] == [0.1, 0.2]


def test_scrub_audit_meta_unscrubbable_object_falls_back_to_string():
    """A custom object that isn't otherwise handled lands as a
    sentinel-tagged string so it's grep-able rather than crashing."""
    class Custom:
        def __str__(self):
            return "weird-object"

    out = database_module._scrub_audit_meta(Custom())
    assert isinstance(out, str)
    assert "<unscrubbable" in out
    assert "Custom" in out


async def test_record_payment_status_transition_serializes_decimal_meta():
    """Sibling fix: the IPN-side helper used to crash on
    ``Decimal`` meta which would propagate up to the IPN handler
    (the method intentionally re-raises) and cause the gateway to
    mark the IPN as failed for no good reason. After the scrub
    fix, Decimals are coerced to JSON floats."""
    import json
    from decimal import Decimal

    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=10)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    new_id = await db.record_payment_status_transition(
        gateway_invoice_id="inv-1",
        payment_status="finished",
        outcome="applied",
        meta={"price_amount": Decimal("12.34")},
    )
    assert new_id == 10
    args = conn.fetchval.await_args.args[1:]
    decoded = json.loads(args[3])
    assert decoded == {"price_amount": 12.34}


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
# Stage-15-Step-E #2 follow-up: bundled bug fix + new
# ``export_user_usage_logs`` query.
#
# Both queries share ``_coerce_usage_log_row`` so a poisoned
# ``Decimal('NaN')`` row (legacy ``log_usage`` calls pre-PR-#75
# could write NaN into ``cost_deducted_usd``) lands as ``0.0``
# rather than ``"nan"`` / a ``ValueError`` crash. Pre-fix the
# admin-side ``GET /admin/users/{id}/usage`` either rendered
# ``$nan`` or 500'd; the parallel ``get_user_spending_summary``
# already scrubbed but the per-row mapper did not.
# ---------------------------------------------------------------------


async def test_list_user_usage_logs_scrubs_nan_cost_to_zero():
    """Bundled bug fix: a poisoned ``cost_deducted_usd`` row must
    not leak ``nan`` into the admin-side rendered table."""
    from decimal import Decimal

    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=1)
    conn.fetch = AsyncMock(
        return_value=[
            {
                "log_id": 99,
                "model_used": "openai/gpt-4o",
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "cost_deducted_usd": Decimal("NaN"),
                "created_at": None,
            }
        ]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    out = await db.list_user_usage_logs(telegram_id=7)
    row = out["rows"][0]
    assert row["cost_usd"] == 0.0
    assert row["total_tokens"] == 30


async def test_list_user_usage_logs_scrubs_infinity_cost_to_zero():
    """Same scrub policy for ``Inf`` / ``-Inf``."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=1)
    conn.fetch = AsyncMock(
        return_value=[
            {
                "log_id": 99,
                "model_used": "openai/gpt-4o",
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "cost_deducted_usd": float("inf"),
                "created_at": None,
            }
        ]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    out = await db.list_user_usage_logs(telegram_id=7)
    assert out["rows"][0]["cost_usd"] == 0.0


async def test_list_user_usage_logs_scrubs_negative_tokens_to_zero():
    """``prompt_tokens`` / ``completion_tokens`` are ``INT NOT NULL``
    in the schema — but the DB scrub clamps any sneaked-in
    negative value (a future migration could change the type;
    a buggy stub could feed -1) to 0 so neither column nor
    ``total_tokens`` go negative."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=1)
    conn.fetch = AsyncMock(
        return_value=[
            {
                "log_id": 99,
                "model_used": "openai/gpt-4o",
                "prompt_tokens": -5,
                "completion_tokens": 20,
                "cost_deducted_usd": 0.0,
                "created_at": None,
            }
        ]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    out = await db.list_user_usage_logs(telegram_id=7)
    row = out["rows"][0]
    assert row["prompt_tokens"] == 0
    assert row["completion_tokens"] == 20
    assert row["total_tokens"] == 20


# ----------------------------------------------------------------------
# export_user_usage_logs (the new user-facing CSV-export query)
# ----------------------------------------------------------------------


async def test_export_user_usage_logs_filters_by_telegram_id():
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.export_user_usage_logs(telegram_id=42)

    sql, *binds = conn.fetch.await_args.args
    assert "WHERE telegram_id = $1" in sql
    # Sorted oldest first so a CSV opened in Excel reads
    # top-to-bottom in chronological order.
    assert "ORDER BY log_id ASC" in sql
    assert binds[0] == 42


async def test_export_user_usage_logs_clamps_limit_to_max():
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.export_user_usage_logs(telegram_id=1, limit=10**9)
    binds = conn.fetch.await_args.args[1:]
    assert binds[1] == db.USAGE_LOGS_EXPORT_MAX_ROWS


async def test_export_user_usage_logs_clamps_limit_floor_to_one():
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.export_user_usage_logs(telegram_id=1, limit=0)
    binds = conn.fetch.await_args.args[1:]
    assert binds[1] == 1


async def test_export_user_usage_logs_default_limit_is_max():
    """``limit=None`` falls back to the cap, not to a smaller
    sensible default — the cap *is* the default for an unbounded
    "give me everything" export."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.export_user_usage_logs(telegram_id=1)
    binds = conn.fetch.await_args.args[1:]
    assert binds[1] == db.USAGE_LOGS_EXPORT_MAX_ROWS


async def test_export_user_usage_logs_rejects_non_positive_telegram_id():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError):
        await db.export_user_usage_logs(telegram_id=0)
    with pytest.raises(ValueError):
        await db.export_user_usage_logs(telegram_id=-5)


async def test_export_user_usage_logs_rejects_non_int_telegram_id():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError):
        await db.export_user_usage_logs(telegram_id="42")  # type: ignore[arg-type]


async def test_export_user_usage_logs_returns_empty_list_when_no_rows():
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    out = await db.export_user_usage_logs(telegram_id=7)
    assert out == []


async def test_export_user_usage_logs_maps_rows_with_total_tokens():
    """Same per-row shape as ``list_user_usage_logs`` — both use
    ``_coerce_usage_log_row`` so they cannot drift."""
    conn = _make_conn()
    conn.fetch = AsyncMock(
        return_value=[
            {
                "log_id": 1,
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
    out = await db.export_user_usage_logs(telegram_id=7)
    assert len(out) == 1
    assert out[0]["id"] == 1
    assert out[0]["model"] == "openai/gpt-4o"
    assert out[0]["prompt_tokens"] == 10
    assert out[0]["completion_tokens"] == 20
    assert out[0]["total_tokens"] == 30
    assert out[0]["cost_usd"] == pytest.approx(0.0042)
    assert out[0]["created_at"] is None


async def test_export_user_usage_logs_scrubs_nan_cost_at_boundary():
    """Same NaN scrub the admin-side path benefits from in the
    bundled bug fix above — a poisoned cost row must not leak
    ``nan`` into the user-facing CSV either."""
    from decimal import Decimal

    conn = _make_conn()
    conn.fetch = AsyncMock(
        return_value=[
            {
                "log_id": 1,
                "model_used": "openai/gpt-4o",
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "cost_deducted_usd": Decimal("NaN"),
                "created_at": None,
            }
        ]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    out = await db.export_user_usage_logs(telegram_id=7)
    assert out[0]["cost_usd"] == 0.0


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
        "count": 3,
        "oldest_age_hours": 2.5,
        "over_threshold_count": 2,
    })

    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_system_metrics()

    assert result["pending_payments_count"] == 3
    assert result["pending_payments_oldest_age_hours"] == 2.5
    assert result["pending_payments_over_threshold_count"] == 2

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
        "count": 0,
        "oldest_age_hours": None,
        "over_threshold_count": 0,
    })

    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_system_metrics()
    assert result["pending_payments_count"] == 0
    assert result["pending_payments_oldest_age_hours"] is None
    assert result["pending_payments_over_threshold_count"] == 0


# ---------------------------------------------------------------------
# Stage-9-Step-10: durable broadcast job registry
# ---------------------------------------------------------------------


async def test_insert_broadcast_job_writes_initial_queued_row():
    """Pin the INSERT shape: schema column ordering and the default
    ``state="queued"`` so a forensic ``SELECT *`` against the table
    is well-defined."""
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="INSERT 0 1")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.insert_broadcast_job(
        job_id="abc123",
        text_preview="preview",
        full_text_len=42,
        only_active_days=None,
    )

    assert conn.execute.await_count == 1
    sql = conn.execute.await_args.args[0]
    normalized = " ".join(sql.split())
    assert "INSERT INTO broadcast_jobs" in normalized
    assert (
        "(job_id, text_preview, full_text_len, only_active_days, state)"
        in normalized
    )
    args = conn.execute.await_args.args
    assert args[1:] == ("abc123", "preview", 42, None, "queued")


async def test_insert_broadcast_job_rejects_invalid_state():
    """Defense in depth: a typo at the call site shouldn't write a
    bogus state to the DB. The validation lives in the DB layer
    rather than the web layer so direct callers (CLI scripts, ad-hoc
    tests) get the same guarantee."""
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError, match="invalid broadcast job state"):
        await db.insert_broadcast_job(
            job_id="x", text_preview="x", full_text_len=1,
            only_active_days=None, state="not-a-real-state",
        )


async def test_update_broadcast_job_patches_only_specified_fields():
    """Pin: passing ``state="running"`` writes ONLY the state
    column (plus ``started_at = NOW()`` if flag set), NOT every
    other field as NULL. The opt-in shape is what makes the
    throttled progress flush cheap (single column UPDATE)."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value="abc")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.update_broadcast_job(
        "abc", state="running", started_at_now=True
    )

    sql = conn.fetchval.await_args.args[0]
    normalized = " ".join(sql.split())
    assert "UPDATE broadcast_jobs SET state = $1, started_at = NOW()" in normalized
    assert "WHERE job_id = $2" in normalized
    # Only one bound param (state) — no NULL writes for the
    # progress counters or completed_at.
    assert conn.fetchval.await_args.args[1:] == ("running", "abc")


async def test_update_broadcast_job_progress_throttle_shape():
    """A throttled progress flush patches the four counters + ``i``
    and nothing else. Pins the column-name mapping
    (``sent`` → ``sent_count``, etc.) so a future rename has to
    intentionally update both call sites."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value="abc")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.update_broadcast_job(
        "abc", total=100, sent=42, blocked=1, failed=2, i=45
    )

    sql = conn.fetchval.await_args.args[0]
    normalized = " ".join(sql.split())
    assert "total = $1" in normalized
    assert "sent_count = $2" in normalized
    assert "blocked_count = $3" in normalized
    assert "failed_count = $4" in normalized
    assert "i = $5" in normalized
    assert conn.fetchval.await_args.args[1:] == (100, 42, 1, 2, 45, "abc")


async def test_update_broadcast_job_returns_false_when_no_row_matches():
    """Pin: the boolean return — used by ``broadcast_cancel_post``
    fallback paths — is False when ``job_id`` doesn't exist."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    result = await db.update_broadcast_job("missing", state="running")
    assert result is False


async def test_update_broadcast_job_no_op_short_circuits():
    """Pin: an empty patch (no fields to update) skips the SQL
    entirely. Otherwise we'd issue a syntactically-broken
    ``UPDATE broadcast_jobs SET WHERE ...`` statement."""
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    result = await db.update_broadcast_job("any")
    assert result is True
    conn.fetchval.assert_not_awaited()


async def test_update_broadcast_job_rejects_invalid_state():
    """Same allow-list as ``insert_broadcast_job`` so a typo at the
    state-transition call site can't poison the row."""
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError):
        await db.update_broadcast_job("any", state="bogus")


async def test_get_broadcast_job_returns_none_for_missing_id():
    """Pin the None contract — ``broadcast_detail_get`` /
    ``broadcast_status_get`` fall back to the in-memory dict's
    behaviour (404 / redirect-with-flash) when ``None`` is
    returned, so we have to be sure the method returns ``None``
    rather than an AsyncMock."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    assert await db.get_broadcast_job("nope") is None


async def test_get_broadcast_job_coerces_record_to_dict_shape():
    """Pin the column → key mapping (notably the ``_count``
    suffix removal: ``sent_count`` → ``sent``, etc.) and
    timestamp .isoformat()-coercion so the web layer can
    consume the dict identically whether it came from the
    in-memory registry or the DB."""
    from datetime import datetime, timezone
    created = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    started = datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    completed = datetime(2026, 1, 1, 0, 0, 30, tzinfo=timezone.utc)
    fake_row = {
        "job_id": "abc123",
        "text_preview": "preview",
        "full_text_len": 42,
        "only_active_days": None,
        "state": "completed",
        "total": 100,
        "sent_count": 95,
        "blocked_count": 3,
        "failed_count": 2,
        "i": 100,
        "error": None,
        "cancel_requested": False,
        "created_at": created,
        "started_at": started,
        "completed_at": completed,
    }
    conn = _make_conn()
    conn.fetchrow = AsyncMock(return_value=fake_row)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_broadcast_job("abc123")
    assert result == {
        "id": "abc123",  # job_id → id
        "text_preview": "preview",
        "full_text_len": 42,
        "only_active_days": None,
        "state": "completed",
        "total": 100,
        "sent": 95,       # sent_count → sent
        "blocked": 3,     # blocked_count → blocked
        "failed": 2,      # failed_count → failed
        "i": 100,
        "error": None,
        "cancel_requested": False,
        "created_at": created.isoformat(),
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
    }


async def test_list_broadcast_jobs_orders_newest_first_and_clamps_limit():
    """Pin: ORDER BY created_at DESC, LIMIT clamped to
    ``BROADCAST_JOB_LIST_MAX_LIMIT`` so a pathological caller
    can't stream the entire table back."""
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    # Default limit
    await db.list_broadcast_jobs()
    sql = conn.fetch.await_args.args[0]
    normalized = " ".join(sql.split())
    assert "ORDER BY created_at DESC, job_id DESC" in normalized
    assert "LIMIT $1" in normalized
    assert conn.fetch.await_args.args[1] == (
        db.BROADCAST_JOB_LIST_DEFAULT_LIMIT
    )

    # Explicit huge limit gets clamped
    await db.list_broadcast_jobs(limit=10_000)
    assert conn.fetch.await_args.args[1] == db.BROADCAST_JOB_LIST_MAX_LIMIT

    # Explicit 0 / negative limit gets floored to 1
    await db.list_broadcast_jobs(limit=0)
    assert conn.fetch.await_args.args[1] == 1


async def test_mark_orphan_broadcast_jobs_interrupted_filters_by_state():
    """Pin the orphan-sweep WHERE clause: only flips
    ``queued`` / ``running`` rows (not ``completed`` / ``failed``
    / already-``interrupted`` ones), and writes
    ``state='interrupted'`` + ``completed_at = NOW()``."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {"job_id": "j1"}, {"job_id": "j2"}, {"job_id": "j3"},
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    n = await db.mark_orphan_broadcast_jobs_interrupted()
    assert n == 3
    sql = conn.fetch.await_args.args[0]
    normalized = " ".join(sql.split())
    assert "UPDATE broadcast_jobs" in normalized
    assert "state = 'interrupted'" in normalized
    assert "completed_at = NOW()" in normalized
    assert "WHERE state IN ('queued', 'running')" in normalized
    assert "RETURNING job_id" in normalized


async def test_mark_orphan_broadcast_jobs_interrupted_idempotent():
    """A second call after the first one has already swept the
    table returns 0 — ensures the orphan sweep is safe to run
    on every startup, not just the one immediately after a
    crash."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    n = await db.mark_orphan_broadcast_jobs_interrupted()
    assert n == 0


# =========================================================================
# Stage-12-Step-A: refund_transaction + mark_payment_refunded_via_ipn
# =========================================================================
#
# Both methods wrap the read-then-write pair in a single DB transaction
# with row-level locks. We pin:
#   * the validation surface (rejects bad inputs at the API boundary)
#   * the SQL shape on the happy path (status flip, refunded_at /
#     refund_reason write, FOR UPDATE locks)
#   * the refusal surface (NOT_SUCCESS / GATEWAY_NOT_REFUNDABLE /
#     INSUFFICIENT_BALANCE) — each must NOT issue any UPDATE
#
# Without these guards a future refactor could drop the SELECT FOR UPDATE
# (re-introducing the race) or accept a non-SUCCESS row (silently
# minting money via a double-refund of an already-refunded charge).


async def test_mark_payment_refunded_via_ipn_pending_row_flips_status():
    """Gateway-side refund of a PENDING row: no credit was issued, no
    debit needed. The row flips to REFUNDED and we record the IPN
    source on ``refund_reason``."""
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

    result = await db.mark_payment_refunded_via_ipn("inv-1")
    assert result is not None
    assert result["previous_status"] == "PENDING"
    assert result["telegram_id"] == 7

    # The UPDATE writes refunded_at + refund_reason and uses the
    # status guard for idempotence.
    sql, *_ = conn.execute.await_args.args
    assert "UPDATE transactions" in sql
    assert "refunded_at = CURRENT_TIMESTAMP" in sql
    assert "refund_reason = $2" in sql
    assert "status = 'REFUNDED'" in sql
    assert "AND status != 'REFUNDED'" in sql


async def test_mark_payment_refunded_via_ipn_partial_row_keeps_credit():
    """Documented limitation: a PARTIAL -> REFUNDED transition does
    NOT debit the user. The partial credit they already received
    stays put — same semantics as ``mark_transaction_terminal`` for
    EXPIRED / FAILED on PARTIAL rows."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(
        return_value={
            "telegram_id": 7,
            "status": "PARTIAL",
            "currency_used": "usdttrc20",
            "amount_usd_credited": 2.5,
        }
    )
    conn.execute = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.mark_payment_refunded_via_ipn("inv-1")
    assert result is not None
    assert result["previous_status"] == "PARTIAL"
    # Only one UPDATE — no balance write.
    assert conn.execute.await_count == 1


async def test_mark_payment_refunded_via_ipn_returns_none_on_terminal():
    """Idempotence: a row already in SUCCESS / REFUNDED / EXPIRED /
    FAILED is left alone. No second UPDATE."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(
        return_value={
            "telegram_id": 7,
            "status": "SUCCESS",
            "currency_used": "usdttrc20",
            "amount_usd_credited": 9.99,
        }
    )
    conn.execute = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.mark_payment_refunded_via_ipn("inv-1")
    assert result is None
    conn.execute.assert_not_awaited()


async def test_mark_payment_refunded_via_ipn_returns_none_when_unknown():
    conn = _make_conn()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.mark_payment_refunded_via_ipn("nope")
    assert result is None
    conn.execute.assert_not_awaited()


async def test_refund_transaction_rejects_non_positive_id():
    """A zero / negative / non-int transaction id never matches a
    real SERIAL row. ValueError at the API surface so the bug shows
    up at the call site, not as a silent no-op."""
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError):
        await db.refund_transaction(
            transaction_id=0, reason="x", admin_telegram_id=0
        )
    with pytest.raises(ValueError):
        await db.refund_transaction(
            transaction_id=-1, reason="x", admin_telegram_id=0
        )
    with pytest.raises(ValueError):
        await db.refund_transaction(
            transaction_id="not-an-int",  # type: ignore[arg-type]
            reason="x",
            admin_telegram_id=0,
        )


async def test_refund_transaction_rejects_empty_reason():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError):
        await db.refund_transaction(
            transaction_id=1, reason="", admin_telegram_id=0
        )
    with pytest.raises(ValueError):
        await db.refund_transaction(
            transaction_id=1, reason="   ", admin_telegram_id=0
        )


async def test_refund_transaction_rejects_oversize_reason():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    huge = "x" * (database_module.Database.REFUND_REASON_MAX_LEN + 1)
    with pytest.raises(ValueError):
        await db.refund_transaction(
            transaction_id=1, reason=huge, admin_telegram_id=0
        )


async def test_refund_transaction_returns_none_on_unknown_id():
    """The benign race: row was deleted between operator clicking
    Refund and the POST landing. The route surfaces this as a
    ``not_found`` banner."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.refund_transaction(
        transaction_id=999, reason="x", admin_telegram_id=0
    )
    assert result is None
    conn.execute.assert_not_awaited()


async def test_refund_transaction_refuses_non_success_status():
    """A PENDING / PARTIAL / EXPIRED / FAILED / REFUNDED row cannot
    be refunded via the admin flow — only SUCCESS rows are eligible.
    Pre-fix, this would have happily double-refunded an already-
    REFUNDED row, debiting the wallet twice for the same transaction.
    """
    for current in ("PENDING", "PARTIAL", "EXPIRED", "FAILED", "REFUNDED"):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(
            return_value={
                "transaction_id": 1,
                "telegram_id": 7,
                "gateway": "nowpayments",
                "amount_usd_credited": 9.99,
                "status": current,
            }
        )
        conn.execute = AsyncMock(return_value=None)
        db = database_module.Database()
        db.pool = _PoolStub(conn)
        result = await db.refund_transaction(
            transaction_id=1, reason="x", admin_telegram_id=0
        )
        assert isinstance(result, dict), f"expected refusal dict for {current}"
        assert result["error"] == db.REFUND_REFUSAL_NOT_SUCCESS
        assert result["current_status"] == current
        # No UPDATE on a refusal.
        conn.execute.assert_not_awaited()


async def test_refund_transaction_refuses_admin_or_gift_gateway():
    """Admin and gift rows are reversed via ``admin_adjust_balance``
    on the user detail page — they don't represent an external
    money movement, so the refund flow refuses them."""
    for gateway in ("admin", "gift"):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(
            return_value={
                "transaction_id": 1,
                "telegram_id": 7,
                "gateway": gateway,
                "amount_usd_credited": 9.99,
                "status": "SUCCESS",
            }
        )
        conn.execute = AsyncMock(return_value=None)
        db = database_module.Database()
        db.pool = _PoolStub(conn)
        result = await db.refund_transaction(
            transaction_id=1, reason="x", admin_telegram_id=0
        )
        assert isinstance(result, dict)
        assert result["error"] == db.REFUND_REFUSAL_GATEWAY_NOT_REFUNDABLE
        conn.execute.assert_not_awaited()


async def test_refund_transaction_refuses_when_balance_below_amount():
    """Operator must debit the user manually before retrying — we
    don't drive a wallet negative."""
    conn = _make_conn()
    # First fetchrow (transactions row), then second fetchrow (users row).
    conn.fetchrow = AsyncMock(side_effect=[
        {
            "transaction_id": 1,
            "telegram_id": 7,
            "gateway": "nowpayments",
            "amount_usd_credited": 9.99,
            "status": "SUCCESS",
        },
        {"balance_usd": 1.0},
    ])
    conn.execute = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.refund_transaction(
        transaction_id=1, reason="x", admin_telegram_id=0
    )
    assert isinstance(result, dict)
    assert result["error"] == db.REFUND_REFUSAL_INSUFFICIENT_BALANCE
    assert result["balance_usd"] == 1.0
    assert result["amount_usd"] == 9.99
    # No UPDATEs on a refusal.
    conn.execute.assert_not_awaited()


async def test_refund_transaction_happy_path_debits_and_flips():
    """SUCCESS row, sufficient balance: wallet is debited by the
    credited USD figure, status flips to REFUNDED, refunded_at /
    refund_reason are set. Two UPDATEs in order: balance, then
    transactions."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(side_effect=[
        {
            "transaction_id": 1,
            "telegram_id": 7,
            "gateway": "nowpayments",
            "amount_usd_credited": 9.99,
            "status": "SUCCESS",
        },
        {"balance_usd": 50.0},
    ])
    conn.execute = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.refund_transaction(
        transaction_id=1,
        reason="duplicate charge",
        admin_telegram_id=0,
    )
    assert isinstance(result, dict)
    assert result["transaction_id"] == 1
    assert result["telegram_id"] == 7
    assert result["amount_refunded_usd"] == 9.99
    # 50.0 - 9.99 (float subtraction precision: assert via tolerance)
    assert abs(result["new_balance_usd"] - 40.01) < 1e-9

    # Two UPDATEs: users.balance_usd, then transactions.status.
    assert conn.execute.await_count == 2
    sqls = [c.args[0] for c in conn.execute.await_args_list]
    assert any("UPDATE users" in s and "balance_usd" in s for s in sqls)
    refund_sql = next(
        s for s in sqls if "UPDATE transactions" in s
    )
    assert "status = 'REFUNDED'" in refund_sql
    assert "refunded_at = CURRENT_TIMESTAMP" in refund_sql
    assert "refund_reason = $2" in refund_sql
    # Idempotency: only flip a still-SUCCESS row.
    assert "AND status = 'SUCCESS'" in refund_sql


async def test_refund_transaction_locks_with_for_update():
    """The transactions read AND the users read must both use
    ``FOR UPDATE`` so a concurrent IPN / deduct_balance can't race
    the eligibility check."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(side_effect=[
        {
            "transaction_id": 1,
            "telegram_id": 7,
            "gateway": "nowpayments",
            "amount_usd_credited": 9.99,
            "status": "SUCCESS",
        },
        {"balance_usd": 50.0},
    ])
    conn.execute = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.refund_transaction(
        transaction_id=1, reason="x", admin_telegram_id=0
    )
    sqls = [c.args[0] for c in conn.fetchrow.await_args_list]
    assert any(
        "FROM transactions" in s and "FOR UPDATE" in s for s in sqls
    ), "transactions read must use FOR UPDATE"
    assert any(
        "FROM users" in s and "FOR UPDATE" in s for s in sqls
    ), "users read must use FOR UPDATE"


async def test_refundable_gateways_constant():
    """Pin the canonical set so a future refactor can't accidentally
    add ``admin`` / ``gift`` (which would then double-debit on the
    user detail page) or drop a real gateway."""
    assert database_module.Database.REFUNDABLE_GATEWAYS == frozenset(
        {"nowpayments", "tetrapay"}
    )


# ---------------------------------------------------------------------
# Stage-12-Step-D: per-code gift redemption drilldown DB methods.
# ---------------------------------------------------------------------


async def test_list_gift_code_redemptions_uppercases_code():
    """The code is upper-cased before being passed to the SQL — gift
    codes are stored upper in ``gift_codes.code``."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=0)
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_gift_code_redemptions(code="birthday5")

    # COUNT query passed the upper-cased code.
    count_call = conn.fetchval.await_args_list[0]
    assert count_call.args[1] == "BIRTHDAY5"
    # SELECT-rows query also passed the upper-cased code.
    rows_call = conn.fetch.await_args_list[0]
    assert rows_call.args[1] == "BIRTHDAY5"


async def test_list_gift_code_redemptions_clamps_per_page():
    """``per_page`` outside [1, GIFT_REDEMPTIONS_MAX_PER_PAGE] is clamped
    so a tampered ?per_page=999999 query string can't hand the DB a
    LIMIT 999999 row scan."""
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_gift_code_redemptions(code="X", per_page=999_999)
    # SELECT-rows LIMIT (positional arg index 2 in fetch call) is
    # clamped to the class constant.
    select_call = conn.fetch.await_args_list[0]
    assert (
        select_call.args[2]
        == database_module.Database.GIFT_REDEMPTIONS_MAX_PER_PAGE
    )

    conn.fetch.reset_mock()
    await db.list_gift_code_redemptions(code="X", per_page=0)
    select_call = conn.fetch.await_args_list[0]
    assert select_call.args[2] == 1  # clamped up to 1


async def test_list_gift_code_redemptions_clamps_page_to_at_least_1():
    """A page=0 / page=-1 query collapses to page=1 — never a negative
    OFFSET (which postgres would reject)."""
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_gift_code_redemptions(code="X", page=-5, per_page=10)
    select_call = conn.fetch.await_args_list[0]
    # OFFSET is positional arg 3; must be 0 (page=1 ⇒ 0 offset).
    assert select_call.args[3] == 0


async def test_list_gift_code_redemptions_offset_uses_page_minus_1():
    """OFFSET = (page - 1) * per_page, the standard pagination offset."""
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_gift_code_redemptions(code="X", page=3, per_page=25)
    select_call = conn.fetch.await_args_list[0]
    assert select_call.args[2] == 25
    assert select_call.args[3] == 50


async def test_list_gift_code_redemptions_sql_pinned():
    """Query must SELECT from gift_redemptions, LEFT JOIN both users
    and transactions (the latter is the new join — surface the actual
    credited amount, not the gift_codes row's amount_usd at page-render
    time), filter on r.code, ORDER BY r.redeemed_at DESC, and LIMIT/OFFSET
    via the right placeholder positions."""
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_gift_code_redemptions(code="X", page=1, per_page=10)

    sql = conn.fetch.await_args_list[0].args[0]
    assert "FROM gift_redemptions" in sql
    assert "LEFT JOIN users" in sql
    assert "LEFT JOIN transactions" in sql
    assert "amount_usd_credited" in sql
    assert "WHERE r.code = $1" in sql
    assert "ORDER BY r.redeemed_at DESC" in sql
    assert "LIMIT $2 OFFSET $3" in sql


async def test_list_gift_code_redemptions_pagination_metadata():
    """``total_pages`` = ceil(total / per_page); also: result dict
    surfaces the post-clamp ``page`` / ``per_page`` so the caller can
    build prev/next URLs from the same numbers the DB used."""
    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=125)  # total
    conn.fetch = AsyncMock(return_value=[])  # rows for this page
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.list_gift_code_redemptions(
        code="X", page=2, per_page=50,
    )
    assert result["total"] == 125
    assert result["page"] == 2
    assert result["per_page"] == 50
    assert result["total_pages"] == 3  # ceil(125 / 50)


async def test_list_gift_code_redemptions_normalises_rows():
    """Per-row dict shape: ints / iso strings / None passthroughs.
    Critically: ``amount_usd_credited`` is a float, transaction_id may
    be None (orphaned redemption — ON DELETE SET NULL on transactions),
    redeemed_at is iso-formatted."""
    from datetime import datetime, timezone

    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=2)
    conn.fetch = AsyncMock(return_value=[
        {
            "telegram_id": 1001,
            "redeemed_at": datetime(2026, 4, 29, 10, 0, tzinfo=timezone.utc),
            "transaction_id": 555,
            "username": "alice",
            "amount_usd_credited": 5.0,
        },
        {
            "telegram_id": 1002,
            "redeemed_at": None,
            "transaction_id": None,  # orphan: tx row was cleaned up
            "username": None,
            "amount_usd_credited": None,
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.list_gift_code_redemptions(code="X")
    rows = result["rows"]
    assert rows[0] == {
        "telegram_id": 1001,
        "username": "alice",
        "redeemed_at": "2026-04-29T10:00:00+00:00",
        "transaction_id": 555,
        "amount_usd_credited": 5.0,
    }
    assert rows[1] == {
        "telegram_id": 1002,
        "username": None,
        "redeemed_at": None,
        "transaction_id": None,
        "amount_usd_credited": None,
    }


async def test_get_gift_code_redemption_aggregates_sql_pinned():
    """The aggregate query MUST sum ``transactions.amount_usd_credited``
    (not ``gift_codes.amount_usd``) — a code can be re-priced after
    redemptions land. ``COALESCE(..., 0)`` so an all-orphan code
    surfaces 0, not None."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(return_value={
        "n": 5, "sum_usd": 25.0, "first_at": None, "last_at": None,
    })
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_gift_code_redemption_aggregates("birthday5")
    sql = conn.fetchrow.await_args_list[0].args[0]
    code_arg = conn.fetchrow.await_args_list[0].args[1]
    assert code_arg == "BIRTHDAY5"  # uppercased
    assert "FROM gift_redemptions" in sql
    assert "LEFT JOIN transactions" in sql
    assert "SUM(t.amount_usd_credited)" in sql
    assert "COALESCE" in sql
    assert "WHERE r.code = $1" in sql


async def test_get_gift_code_redemption_aggregates_returns_zero_when_empty():
    """No row → all-zero / all-None aggregates; never raise."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    result = await db.get_gift_code_redemption_aggregates("X")
    assert result == {
        "total_redemptions": 0,
        "total_credited_usd": 0.0,
        "first_redeemed_at": None,
        "last_redeemed_at": None,
    }


async def test_get_gift_code_uppercases_lookup():
    """get_gift_code(code) MUST upper-case before hitting the PK
    lookup; ``gift_codes.code`` is uppercase by table convention."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_gift_code("birthday5")
    code_arg = conn.fetchrow.await_args_list[0].args[1]
    assert code_arg == "BIRTHDAY5"


async def test_get_gift_code_returns_none_when_missing():
    """A missing code returns None, not an exception."""
    conn = _make_conn()
    conn.fetchrow = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    assert await db.get_gift_code("GHOST") is None


# ---------------------------------------------------------------------
# Stage-15-Step-E #5: admin role primitives
# ---------------------------------------------------------------------


async def test_get_admin_role_returns_role_when_present():
    conn = _make_conn()
    conn.fetchrow = AsyncMock(return_value={"role": "operator"})
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    assert await db.get_admin_role(777) == "operator"
    sql = conn.fetchrow.await_args.args[0]
    assert "SELECT role FROM admin_roles" in sql
    assert "telegram_id = $1" in sql
    assert conn.fetchrow.await_args.args[1] == 777


async def test_get_admin_role_returns_none_when_missing():
    conn = _make_conn()
    conn.fetchrow = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    assert await db.get_admin_role(777) is None


async def test_set_admin_role_normalizes_casing_and_whitespace():
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="INSERT 0 1")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    stored = await db.set_admin_role(
        777, "  Operator ", granted_by=1, notes="trusted",
    )
    assert stored == "operator"
    args = conn.execute.await_args.args[1:]
    assert args == (777, "operator", 1, "trusted")


@pytest.mark.parametrize("bad", ["", " ", None, "admin", "supr", "🛡"])
async def test_set_admin_role_rejects_invalid_role(bad):
    """Validate up-front so the caller sees a ``ValueError`` rather
    than the asyncpg ``CheckViolationError`` from the SQL CHECK."""
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with pytest.raises(ValueError, match="role must be one of"):
        await db.set_admin_role(777, bad)
    # Crucially: NEVER hit the DB for an invalid role. A roundtrip
    # would let a typo in the form parser pollute the connection
    # pool's transaction state on the failure path.
    conn.execute.assert_not_awaited()


async def test_set_admin_role_passes_null_granted_by_when_unset():
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="INSERT 0 1")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.set_admin_role(777, "viewer")
    args = conn.execute.await_args.args[1:]
    assert args[2] is None  # granted_by
    assert args[3] is None  # notes


async def test_set_admin_role_uses_upsert():
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="INSERT 0 1")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.set_admin_role(777, "viewer")
    sql = conn.execute.await_args.args[0]
    assert "ON CONFLICT (telegram_id)" in sql
    # NOW() refresh must be on the UPDATE branch so the
    # `granted_at` column reflects the most-recent change rather
    # than the original insert time.
    assert "granted_at = NOW()" in sql


async def test_set_admin_role_strips_nul_bytes_from_notes(caplog):
    """Postgres TEXT rejects ``\\x00`` outright. The new ``/admin/roles``
    web form exposes ``notes`` as a free-form textarea — same regression
    class ``append_conversation_message`` documented in
    Stage-15-Step-E #10. Strip-and-warn at the DB layer so a NUL-bearing
    paste from a binary file doesn't demote the whole grant to a generic
    "DB write failed" error.
    """
    import logging
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="INSERT 0 1")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with caplog.at_level(logging.WARNING):
        await db.set_admin_role(
            777, "viewer", notes="hello\x00world\x00\x00",
        )

    # NUL bytes stripped, the rest of the text preserved.
    args = conn.execute.await_args.args[1:]
    assert args[3] == "helloworld"
    # The strip is logged loud-and-once with the count so ops can
    # investigate where the NUL came from.
    assert any(
        "set_admin_role: stripping" in rec.message
        and "NUL byte(s) from notes" in rec.message
        for rec in caplog.records
    )


async def test_set_admin_role_preserves_non_nul_text():
    """No NUL bytes → notes are forwarded verbatim. Defence against a
    future "fix" that over-eagerly mangles a perfectly valid string.
    """
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="INSERT 0 1")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.set_admin_role(
        777, "viewer", notes="多 byte unicode → still fine\nwith newline",
    )
    args = conn.execute.await_args.args[1:]
    assert args[3] == "多 byte unicode → still fine\nwith newline"


async def test_set_admin_role_passes_through_none_notes():
    """Avoid the strip path when notes is ``None`` (the common case)."""
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="INSERT 0 1")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.set_admin_role(777, "viewer", notes=None)
    args = conn.execute.await_args.args[1:]
    assert args[3] is None


async def test_delete_admin_role_returns_true_on_delete_one():
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="DELETE 1")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    assert await db.delete_admin_role(777) is True


async def test_delete_admin_role_returns_false_on_delete_zero():
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="DELETE 0")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    assert await db.delete_admin_role(777) is False


async def test_delete_admin_role_returns_false_on_unexpected_command_tag():
    """Defence-in-depth: a future asyncpg release that returned a
    different tag (or a buggy mock) shouldn't be interpreted as a
    successful delete."""
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="UPDATE 1")
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    assert await db.delete_admin_role(777) is False


async def test_list_admin_roles_clamps_limit_into_safe_range():
    """A buggy caller passing ``limit=10**6`` shouldn't OOM the
    formatter. Clamp at 1000."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_admin_roles(limit=10**6)
    assert conn.fetch.await_args.args[1] == 1000


async def test_list_admin_roles_clamps_negative_limit_to_one():
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_admin_roles(limit=-5)
    assert conn.fetch.await_args.args[1] == 1


async def test_list_admin_roles_renders_rows():
    from datetime import datetime, timezone

    ts = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "telegram_id": 777,
            "role": "operator",
            "granted_at": ts,
            "granted_by": 1,
            "notes": "trusted",
        },
        {
            "telegram_id": 888,
            "role": "viewer",
            "granted_at": ts,
            "granted_by": None,
            "notes": None,
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_admin_roles()
    assert rows == [
        {
            "telegram_id": 777,
            "role": "operator",
            "granted_at": ts.isoformat(),
            "granted_by": 1,
            "notes": "trusted",
        },
        {
            "telegram_id": 888,
            "role": "viewer",
            "granted_at": ts.isoformat(),
            "granted_by": None,
            "notes": None,
        },
    ]


# ---------------------------------------------------------------------
# Stage-15-Step-E #5 bundled bug fix:
# ``list_admin_audit_log`` / ``list_payment_status_transitions`` no
# longer crash on asyncpg's default ``str``-encoded JSONB meta.
# ---------------------------------------------------------------------


async def test_list_admin_audit_log_decodes_jsonb_string_meta():
    """Pre-fix: ``dict("...JSON string...")`` raised ``ValueError``
    because asyncpg returns JSONB columns as raw ``str`` by default
    (no codec is registered on the pool). The audit page handler
    swallowed the exception and rendered "Database query failed",
    so the regression was silent in production. Pin the new
    ``_decode_jsonb_meta`` path so a future revert reintroducing
    ``dict(r["meta"])`` fails this test loudly."""
    from datetime import datetime, timezone

    ts = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1,
            "ts": ts,
            "actor": "web",
            "action": "user_adjust",
            "target": "user:777",
            "ip": "203.0.113.10",
            "outcome": "ok",
            "meta": '{"delta_usd": 5.0, "reason": "stuck_invoice"}',
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_admin_audit_log()
    assert rows[0]["meta"] == {"delta_usd": 5.0, "reason": "stuck_invoice"}


async def test_list_admin_audit_log_handles_dict_meta_for_codec_compat():
    """When a future deploy registers a JSONB codec on the pool, the
    column will already be a dict. The decoder must accept that
    shape too — otherwise switching to a codec would break this read
    path again the other way."""
    from datetime import datetime, timezone

    ts = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1, "ts": ts, "actor": "web", "action": "login_ok",
            "target": None, "ip": None, "outcome": "ok",
            "meta": {"reason": "ok"},
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_admin_audit_log()
    assert rows[0]["meta"] == {"reason": "ok"}


async def test_list_admin_audit_log_demotes_unparseable_meta_to_none():
    """A poisoned row (truncated JSON, a non-JSON insert from a
    legacy SQL script) should not blank the entire feed. The
    decoder logs a WARNING and demotes that row's meta to ``None``."""
    from datetime import datetime, timezone

    ts = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1, "ts": ts, "actor": "web", "action": "x",
            "target": None, "ip": None, "outcome": "ok",
            "meta": "{not valid json",
        },
        {
            "id": 2, "ts": ts, "actor": "web", "action": "x",
            "target": None, "ip": None, "outcome": "ok",
            "meta": '{"reason": "ok"}',
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_admin_audit_log()
    # Row 1 was poisoned but doesn't kill the feed; row 2 still
    # decodes cleanly.
    assert rows[0]["meta"] is None
    assert rows[1]["meta"] == {"reason": "ok"}


async def test_list_admin_audit_log_keeps_null_meta_as_none():
    """Sanity: an explicit NULL meta column round-trips cleanly."""
    from datetime import datetime, timezone

    ts = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1, "ts": ts, "actor": "web", "action": "login_ok",
            "target": None, "ip": None, "outcome": "ok", "meta": None,
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_admin_audit_log()
    assert rows[0]["meta"] is None


async def test_list_payment_status_transitions_decodes_jsonb_string_meta():
    """Same fix applies to ``list_payment_status_transitions`` —
    same shape, same decoder, same regression."""
    from datetime import datetime, timezone

    ts = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1,
            "gateway_invoice_id": "abc-123",
            "payment_status": "partially_paid",
            "recorded_at": ts,
            "outcome": "credited",
            "meta": '{"received_usd": 4.95}',
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_payment_status_transitions()
    assert rows[0]["meta"] == {"received_usd": 4.95}


# ---------------------------------------------------------------------
# Stage-15-Step-E #10 (this PR) bundled bug fix:
# ``append_conversation_message`` strips U+0000 NUL bytes before INSERT
# so a NUL-bearing prompt or reply doesn't poison the conversation
# buffer with the Postgres "invalid byte sequence for encoding UTF8:
# 0x00" rejection.
# ---------------------------------------------------------------------
# Pre-fix: PR #129 (Stage-15-Step-E #10 first slice) wrapped the
# upstream call site in ``ai_engine.chat_with_model`` in a defensive
# try/except so the AI reply isn't lost (and the user isn't double-
# billed on retry). But the underlying memory turn was still
# discarded. This test pins the root-cause fix: NUL bytes are
# silently stripped at the DB layer so the buffer stays intact.
# Telegram clients DO let users send U+0000 (paste-from-binary,
# Android emoji-keyboard bugs), so this isn't theoretical.
# ---------------------------------------------------------------------


async def test_append_conversation_message_strips_nul_bytes(caplog):
    """A prompt with embedded ``\\x00`` must reach the INSERT with
    the NUL stripped — every other character preserved verbatim
    so the conversation buffer keeps maximum fidelity."""
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with caplog.at_level("WARNING"):
        await db.append_conversation_message(
            42, "user", "hello\x00world\x00!",
        )

    # The INSERT was issued with the NUL bytes removed.
    assert conn.execute.await_count == 1
    args = conn.execute.await_args.args
    # ``execute(query, telegram_id, role, content)``
    assert args[1] == 42
    assert args[2] == "user"
    assert args[3] == "helloworld!"
    # Loud-and-once warning so ops can investigate the source.
    assert any(
        "stripping" in record.message and "NUL" in record.message
        for record in caplog.records
    ), "expected NUL-strip warning log entry"


async def test_append_conversation_message_no_nul_no_log():
    """A plain prompt without any NUL bytes must NOT emit the
    strip warning — the log is reserved for the actually-fired
    case so ops can spot row-corruption sources."""
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    import logging as _logging
    handler_records: list[_logging.LogRecord] = []

    class _Capture(_logging.Handler):
        def emit(self, record):
            handler_records.append(record)

    cap = _Capture(level=_logging.WARNING)
    database_module.log.addHandler(cap)
    try:
        await db.append_conversation_message(42, "user", "plain text only")
    finally:
        database_module.log.removeHandler(cap)

    assert conn.execute.await_count == 1
    args = conn.execute.await_args.args
    assert args[3] == "plain text only"
    assert not any(
        "stripping" in r.getMessage() for r in handler_records
    )


async def test_append_conversation_message_strip_then_truncate():
    """The strip step runs *before* the length-cap step so a
    prompt that's NUL-padded to be over the limit gets the NULs
    removed first; only the genuine content is then capped at
    ``MEMORY_CONTENT_MAX_CHARS``. Without this ordering a
    NUL-heavy prompt could be truncated mid-Unicode-codepoint
    or have its real content prematurely cut off."""
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    cap = db.MEMORY_CONTENT_MAX_CHARS
    nul_padding = "\x00" * 100
    real_content = "x" * (cap - 50)
    prompt = nul_padding + real_content

    await db.append_conversation_message(42, "user", prompt)

    args = conn.execute.await_args.args
    persisted = args[3]
    # NULs gone, real content fully preserved (still under cap).
    assert "\x00" not in persisted
    assert persisted == real_content


async def test_append_conversation_message_only_nul_persists_empty():
    """A prompt that's *entirely* NUL bytes ends up as the empty
    string. Postgres TEXT NOT NULL accepts ``''`` so the INSERT
    succeeds — better than blowing up the whole buffer write
    with a Postgres-level rejection. The upstream caller has its
    own defensive catch (PR #129 wrap) so we don't even need to
    flag this as exceptional from here; the warning log alone
    is enough for ops to investigate."""
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.append_conversation_message(42, "assistant", "\x00\x00\x00")

    args = conn.execute.await_args.args
    assert args[3] == ""


async def test_append_conversation_message_unicode_preserved_around_nul():
    """Non-NUL Unicode (RTL Persian text, emoji, control chars
    other than NUL) must round-trip unchanged. We strip ONLY
    U+0000 — every other code point is fine in Postgres TEXT."""
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    payload = "سلام\x00دنیا 🌍\x01\x02\nfoo"
    await db.append_conversation_message(42, "user", payload)

    args = conn.execute.await_args.args
    # Persian, emoji, \x01, \x02, \n all preserved; only \x00 stripped.
    assert args[3] == "سلامدنیا 🌍\x01\x02\nfoo"


def test_append_conversation_message_invalid_role_still_rejected():
    """The pre-existing role validation must still fire — the
    new strip step must run *after* the role check, not
    short-circuit it. Belt-and-braces: a future refactor that
    inadvertently moves the role check below the strip would
    let an invalid role reach the INSERT."""
    import asyncio
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    with pytest.raises(ValueError, match="invalid role"):
        asyncio.get_event_loop().run_until_complete(
            db.append_conversation_message(42, "system", "hi"),
        )


# ---------------------------------------------------------------------
# Stage-15-Step-E #4 follow-up #2: openrouter_api_keys CRUD
# ---------------------------------------------------------------------


def _make_pool_with_methods(
    *, fetch=None, fetchrow=None, fetchval=None, execute=None
):
    """Build a pool stub that exposes ``fetch`` / ``fetchrow`` /
    ``execute`` / ``fetchval`` directly (the openrouter_api_keys
    methods don't go through ``acquire()``).
    """
    pool = MagicMock()
    pool.fetch = AsyncMock(return_value=fetch if fetch is not None else [])
    pool.fetchrow = AsyncMock(return_value=fetchrow)
    pool.fetchval = AsyncMock(return_value=fetchval)
    pool.execute = AsyncMock(return_value=execute or "DELETE 0")
    return pool


async def test_list_openrouter_keys_returns_masked_tail():
    pool = _make_pool_with_methods(fetch=[
        {
            "id": 1, "label": "main", "api_key_tail": "abcd",
            "api_key_len": 48, "enabled": True,
            "created_at": None, "last_used_at": None, "notes": None,
        },
    ])
    db = database_module.Database()
    db.pool = pool
    rows = await db.list_openrouter_keys()
    assert rows == [
        {
            "id": 1, "label": "main", "api_key_tail": "abcd",
            "api_key_len": 48, "enabled": True,
            "created_at": None, "last_used_at": None, "notes": None,
        },
    ]
    # The query must NEVER select the raw api_key column — that's
    # the whole point of having a separate "_with_secret" method.
    sql = pool.fetch.await_args.args[0]
    assert "RIGHT(api_key, 4)" in sql
    # No SELECT api_key (without RIGHT/LEFT/etc) — only the tail.
    import re
    assert not re.search(r"SELECT[^,]*\bapi_key\b(?!_)", sql), sql


async def test_list_openrouter_keys_include_disabled_default():
    pool = _make_pool_with_methods(fetch=[])
    db = database_module.Database()
    db.pool = pool
    await db.list_openrouter_keys()
    sql = pool.fetch.await_args.args[0]
    # Default includes disabled rows (no WHERE enabled = TRUE).
    assert "WHERE enabled = TRUE" not in sql


async def test_list_openrouter_keys_filter_disabled_when_requested():
    pool = _make_pool_with_methods(fetch=[])
    db = database_module.Database()
    db.pool = pool
    await db.list_openrouter_keys(include_disabled=False)
    sql = pool.fetch.await_args.args[0]
    assert "WHERE enabled = TRUE" in sql


async def test_list_enabled_openrouter_keys_with_secret_returns_plaintext():
    pool = _make_pool_with_methods(fetch=[
        {"id": 1, "label": "main", "api_key": "sk-or-v1-secret"},
    ])
    db = database_module.Database()
    db.pool = pool
    rows = await db.list_enabled_openrouter_keys_with_secret()
    assert rows == [{"id": 1, "label": "main", "api_key": "sk-or-v1-secret"}]
    sql = pool.fetch.await_args.args[0]
    assert "WHERE enabled = TRUE" in sql


async def test_add_openrouter_key_inserts_and_returns_id():
    pool = _make_pool_with_methods(fetchrow={"id": 99})
    db = database_module.Database()
    db.pool = pool
    new_id = await db.add_openrouter_key(
        label="main", api_key="sk-or-v1-x", notes="primary",
    )
    assert new_id == 99
    args = pool.fetchrow.await_args.args
    assert args[1] == "main"
    assert args[2] == "sk-or-v1-x"
    assert args[3] == "primary"


@pytest.mark.parametrize("label", ["", "  ", None])
async def test_add_openrouter_key_rejects_blank_label(label):
    db = database_module.Database()
    db.pool = _make_pool_with_methods()
    with pytest.raises(ValueError, match="label"):
        await db.add_openrouter_key(label=label, api_key="sk-or-v1-x")


@pytest.mark.parametrize("api_key", ["", "  ", None])
async def test_add_openrouter_key_rejects_blank_api_key(api_key):
    db = database_module.Database()
    db.pool = _make_pool_with_methods()
    with pytest.raises(ValueError, match="api_key"):
        await db.add_openrouter_key(label="x", api_key=api_key)


async def test_add_openrouter_key_translates_unique_violation_to_value_error():
    """A duplicate api_key (UNIQUE constraint) must surface as a
    ValueError so the panel can render a flash banner without
    coupling to asyncpg's exception hierarchy."""

    class _UniqueViolationError(Exception):
        pass

    _UniqueViolationError.__name__ = "UniqueViolationError"
    pool = _make_pool_with_methods()
    pool.fetchrow = AsyncMock(side_effect=_UniqueViolationError("dup"))
    db = database_module.Database()
    db.pool = pool
    with pytest.raises(ValueError, match="already registered"):
        await db.add_openrouter_key(label="x", api_key="dup-key")


async def test_set_openrouter_key_enabled_returns_true_on_update():
    pool = _make_pool_with_methods(execute="UPDATE 1")
    db = database_module.Database()
    db.pool = pool
    assert await db.set_openrouter_key_enabled(7, enabled=False) is True


async def test_set_openrouter_key_enabled_returns_false_on_no_match():
    pool = _make_pool_with_methods(execute="UPDATE 0")
    db = database_module.Database()
    db.pool = pool
    assert await db.set_openrouter_key_enabled(7, enabled=False) is False


async def test_delete_openrouter_key_returns_true_on_delete():
    pool = _make_pool_with_methods(execute="DELETE 1")
    db = database_module.Database()
    db.pool = pool
    assert await db.delete_openrouter_key(7) is True


async def test_delete_openrouter_key_returns_false_on_no_match():
    pool = _make_pool_with_methods(execute="DELETE 0")
    db = database_module.Database()
    db.pool = pool
    assert await db.delete_openrouter_key(7) is False


async def test_mark_openrouter_key_used_runs_update():
    pool = _make_pool_with_methods(execute="UPDATE 1")
    db = database_module.Database()
    db.pool = pool
    await db.mark_openrouter_key_used(7)
    sql = pool.execute.await_args.args[0]
    assert "last_used_at = NOW()" in sql


# ---------------------------------------------------------------------
# Stage-15-Step-E #4 follow-up #2: _command_count helper
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "tag,expected",
    [
        ("UPDATE 1", 1),
        ("UPDATE 0", 0),
        ("UPDATE 11", 11),  # used to false-match endswith("1")
        ("DELETE 1", 1),
        ("DELETE 0", 0),
        ("INSERT 0 3", 3),  # the middle 0 is the OID, ignore it
        ("INSERT 0 0", 0),
        ("", 0),
        (None, 0),
        ("SELECT", 0),  # no trailing int
        ("UPDATE", 0),
    ],
)
def test_command_count_parses_pg_tags(tag, expected):
    assert database_module._command_count(tag) == expected


async def test_set_openrouter_key_enabled_handles_double_digit_count():
    """Regression: the previous ``endswith("1")`` pattern would
    have returned True for ``"UPDATE 11"`` even though that's
    impossible for the by-id path; the parsed-count helper is
    the right shape regardless."""
    pool = _make_pool_with_methods(execute="UPDATE 11")
    db = database_module.Database()
    db.pool = pool
    assert await db.set_openrouter_key_enabled(7, enabled=False) is True


# ---------------------------------------------------------------------
# Stage-15-Step-E #10 follow-up #2: JSONB-backed image refs on
# conversation_messages — append + read paths round-trip the image
# data URIs so a memory-enabled user's vision turn replays *with*
# the visual context on the next turn.
# ---------------------------------------------------------------------


_DATA_URI_RED_DOT = (
    # 1×1 transparent PNG inline as a representative data URI.
    "data:image/png;base64,"
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgAAIAAAUAAeImBZsAAAAASUVORK5CYII="
)
_DATA_URI_BLUE_DOT = (
    "data:image/jpeg;base64,"
    "/9j/4AAQSkZJRgABAQEAYABgAAD//gA8Q1JFQVRPUjogZ2QtanBlZyB2MS4wICh1c2luZyBJSkcgSlBFRyB2NjIp"
)


async def test_append_conversation_message_text_only_writes_null_image_uris():
    """Text-only turns must keep their existing INSERT shape — the
    new ``image_data_uris`` JSONB column is NULL by default so the
    overwhelming majority of writes don't change shape."""
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.append_conversation_message(42, "user", "plain text only")

    args = conn.execute.await_args.args
    # ``execute(query, telegram_id, role, content, image_uris_json)``
    assert args[1] == 42
    assert args[2] == "user"
    assert args[3] == "plain text only"
    assert args[4] is None  # JSONB column is NULL for text-only turns


async def test_append_conversation_message_writes_image_data_uris():
    """A vision turn must JSON-encode the image list and pass it as
    the ``$N::jsonb`` parameter so Postgres stores the structured
    payload rather than a stringified blob."""
    import json
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.append_conversation_message(
        42, "user", "describe this please",
        image_data_uris=[_DATA_URI_RED_DOT, _DATA_URI_BLUE_DOT],
    )

    args = conn.execute.await_args.args
    # The SQL must cast the param to JSONB (pin the cast so a future
    # refactor doesn't silently regress to a TEXT INSERT).
    sql = args[0]
    assert "$4::jsonb" in sql
    assert "image_data_uris" in sql
    # The Python-side payload is a JSON-encoded list of strings.
    assert isinstance(args[4], str)
    decoded = json.loads(args[4])
    assert decoded == [_DATA_URI_RED_DOT, _DATA_URI_BLUE_DOT]


async def test_append_conversation_message_empty_image_list_writes_null():
    """An explicitly empty list normalises to NULL so the column
    stays consistent with the text-only path. Pre-fix, an empty
    list would have written ``[]`` JSONB, which downstream readers
    would have to special-case."""
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.append_conversation_message(
        42, "user", "no images here", image_data_uris=[],
    )

    args = conn.execute.await_args.args
    assert args[4] is None


async def test_append_conversation_message_rejects_non_list_image_data_uris():
    """A buggy caller passing a non-list (e.g. a single string) must
    surface a clear ``TypeError`` rather than silently
    JSON-encoding the wrong shape."""
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with pytest.raises(TypeError, match="image_data_uris must be"):
        await db.append_conversation_message(
            42, "user", "x",
            image_data_uris=_DATA_URI_RED_DOT,  # type: ignore[arg-type]
        )
    # The INSERT must NOT have fired — the type-check rejects before
    # we waste a DB round-trip.
    conn.execute.assert_not_awaited()


@pytest.mark.parametrize("bad_uri", [None, "", 42, 3.14, b"bytes"])
async def test_append_conversation_message_rejects_invalid_uri_entry(bad_uri):
    """A single bad entry in the image list must reject the whole
    INSERT — partial writes leave inconsistent state in the
    memory buffer that's hard to debug."""
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with pytest.raises(ValueError, match="must be a non-empty str"):
        await db.append_conversation_message(
            42, "user", "x",
            image_data_uris=[_DATA_URI_RED_DOT, bad_uri],
        )
    conn.execute.assert_not_awaited()


async def test_append_conversation_message_image_uris_after_nul_strip():
    """The strip step still runs for the content even when the
    image list is supplied — ordering matters; a NUL-bearing
    prompt with a valid image list must surface a clean row, not
    a Postgres-level rejection."""
    conn = _make_conn()
    conn.execute = AsyncMock()
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.append_conversation_message(
        42, "user", "hello\x00world",
        image_data_uris=[_DATA_URI_RED_DOT],
    )

    args = conn.execute.await_args.args
    assert args[3] == "helloworld"
    assert args[4] is not None  # JSONB written


async def test_get_recent_messages_text_only_returns_legacy_shape():
    """Rows with NULL ``image_data_uris`` (the overwhelming
    majority — every text turn) must return the legacy
    ``{"role": ..., "content": <str>}`` shape so the existing
    prompt-assembly path is unchanged."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {"role": "user", "content": "hi", "image_data_uris": None},
        {"role": "assistant", "content": "hello", "image_data_uris": None},
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.get_recent_messages(42)
    # Newest-first DB → reversed → oldest-first array. The fixture
    # already returns the in-display order to keep the test simple,
    # but the reversal still flips it; pin both elements.
    assert rows == [
        {"role": "assistant", "content": "hello"},
        {"role": "user", "content": "hi"},
    ]


async def test_get_recent_messages_vision_turn_reconstructs_multimodal():
    """A user-row with non-null ``image_data_uris`` must reconstruct
    the OpenAI/OpenRouter multimodal shape (``content`` is a list
    of ``{type, ...}`` parts) so a memory-enabled vision turn
    replays *with* the visual context on the next turn."""
    import json
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "role": "user", "content": "what is in this picture?",
            "image_data_uris": json.dumps([_DATA_URI_RED_DOT]),
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.get_recent_messages(42)
    assert rows == [{
        "role": "user",
        "content": [
            {"type": "text", "text": "what is in this picture?"},
            {"type": "image_url",
             "image_url": {"url": _DATA_URI_RED_DOT}},
        ],
    }]


async def test_get_recent_messages_vision_turn_preserves_uri_order():
    """Multi-image turns must preserve the original URI order —
    most providers treat ordering as informative ("image 1 vs.
    image 2"), and a future user-feedback regression that
    silently re-ordered the images would be hard to attribute
    without this pin."""
    import json
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "role": "user", "content": "compare these:",
            "image_data_uris": json.dumps(
                [_DATA_URI_RED_DOT, _DATA_URI_BLUE_DOT],
            ),
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.get_recent_messages(42)
    parts = rows[0]["content"]
    assert parts[0] == {"type": "text", "text": "compare these:"}
    assert parts[1]["image_url"]["url"] == _DATA_URI_RED_DOT
    assert parts[2]["image_url"]["url"] == _DATA_URI_BLUE_DOT


async def test_get_recent_messages_vision_turn_with_empty_text():
    """Image-only turn (caption was empty / image-only attach)
    must round-trip without an empty text part — pre-fix, an
    empty-string text part would still be added, which some
    providers reject as an empty content block."""
    import json
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {"role": "user", "content": "",
         "image_data_uris": json.dumps([_DATA_URI_RED_DOT])},
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.get_recent_messages(42)
    parts = rows[0]["content"]
    assert all(p.get("type") != "text" for p in parts)
    assert parts == [
        {"type": "image_url", "image_url": {"url": _DATA_URI_RED_DOT}},
    ]


async def test_get_recent_messages_assistant_image_uris_ignored():
    """Defence-in-depth: the schema column applies to BOTH roles
    but assistant rows never carry image refs today. If a row
    somehow lands with role=assistant and non-null URIs (a manual
    SQL fix, a future model that emits images), keep the legacy
    plain-string shape rather than fabricating a multimodal
    content array — the assistant row's text reply is still the
    primary signal, and treating it as multimodal would change
    the wire shape sent to OpenRouter for replay."""
    import json
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {"role": "assistant", "content": "I see a red dot",
         "image_data_uris": json.dumps([_DATA_URI_RED_DOT])},
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.get_recent_messages(42)
    assert rows == [{"role": "assistant", "content": "I see a red dot"}]


async def test_get_recent_messages_malformed_json_falls_back_to_text(caplog):
    """A poisoned row (``image_data_uris`` JSONB cell that doesn't
    decode) must NOT blank the entire memory buffer. Fail-soft:
    log loud-and-once for ops, return the legacy text shape so
    the user keeps their conversational thread."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {"role": "user", "content": "hi",
         "image_data_uris": "{not valid json"},
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    with caplog.at_level("WARNING"):
        rows = await db.get_recent_messages(42)

    assert rows == [{"role": "user", "content": "hi"}]
    assert any(
        "could not decode" in r.message for r in caplog.records
    )


async def test_get_recent_messages_query_selects_image_data_uris_column():
    """Pin the SELECT list — a future refactor that drops
    ``image_data_uris`` from the projection would silently
    surface text-only memory replays for vision turns, the very
    bug this PR fixes. Cheap belt-and-braces against drift."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_recent_messages(42)

    sql = conn.fetch.await_args.args[0]
    assert "image_data_uris" in sql


# ---------------------------------------------------------------------
# Stage-15-Step-E #10 follow-up #2: _decode_jsonb_str_list helper
# ---------------------------------------------------------------------


def test_decode_jsonb_str_list_none_returns_none():
    assert database_module._decode_jsonb_str_list(None) is None


def test_decode_jsonb_str_list_accepts_list_directly():
    """A future asyncpg codec registration would yield a Python
    list straight away — the helper must accept that shape too."""
    raw = ["a", "b", "c"]
    assert database_module._decode_jsonb_str_list(raw) == ["a", "b", "c"]


def test_decode_jsonb_str_list_decodes_json_string():
    assert database_module._decode_jsonb_str_list(
        '["a", "b"]'
    ) == ["a", "b"]


def test_decode_jsonb_str_list_decodes_bytes():
    assert database_module._decode_jsonb_str_list(
        b'["a", "b"]'
    ) == ["a", "b"]


def test_decode_jsonb_str_list_malformed_returns_none(caplog):
    with caplog.at_level("WARNING"):
        assert database_module._decode_jsonb_str_list(
            "{not json"
        ) is None
    assert any("could not decode" in r.message for r in caplog.records)


def test_decode_jsonb_str_list_non_list_json_returns_none(caplog):
    """A JSON object / string / number is structurally wrong for
    this column — log and return None so the read path falls back
    to the text-only shape."""
    with caplog.at_level("WARNING"):
        assert database_module._decode_jsonb_str_list(
            '{"oops": true}'
        ) is None
    assert any(
        "decoded to non-list" in r.message for r in caplog.records
    )


def test_decode_jsonb_str_list_drops_non_string_entries(caplog):
    """A list with a mix of valid + invalid entries surfaces only
    the valid strings — and logs a warning for each dropped
    entry so the source row is identifiable."""
    with caplog.at_level("WARNING"):
        result = database_module._decode_jsonb_str_list(
            '["valid", 42, null, "also-valid", ""]'
        )
    assert result == ["valid", "also-valid"]


def test_decode_jsonb_str_list_all_invalid_returns_none():
    """Every entry rejected → return None so the caller falls
    back to text-only rather than producing an empty-list shape
    the multimodal payload assembler would reject as
    ``empty_message``."""
    assert database_module._decode_jsonb_str_list(
        '[42, null, true]'
    ) is None


def test_decode_jsonb_str_list_unexpected_type_returns_none(caplog):
    """An int / float / dict landing here means a future codec
    or an upstream caller invariant broke. Log + return None."""
    with caplog.at_level("WARNING"):
        assert database_module._decode_jsonb_str_list(42) is None
    assert any(
        "unexpected JSONB string-list value type" in r.message
        for r in caplog.records
    )


# ---------------------------------------------------------------------
# list_markup_history (Stage-15-Step-E #10b row 12)
# ---------------------------------------------------------------------


async def test_list_markup_history_filters_by_action():
    """The query must only return ``monetization_markup_update`` rows
    so the page never accidentally surfaces unrelated audit traffic
    (e.g. ``user_adjust``) on the markup-history card."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_markup_history()
    sql = conn.fetch.await_args.args[0]
    normalized = " ".join(sql.split())
    assert "FROM admin_audit_log" in normalized
    assert (
        "WHERE action = 'monetization_markup_update'" in normalized
    )
    assert "ORDER BY ts DESC" in normalized


async def test_list_markup_history_decodes_meta_into_typed_fields():
    """Happy path: a JSONB-string meta is decoded into the typed
    ``before`` / ``after`` / ``before_source`` / ``after_source`` /
    ``kind`` fields the template consumes."""
    from datetime import datetime, timezone

    ts = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 7,
            "ts": ts,
            "actor": "web",
            "ip": "203.0.113.10",
            "meta": (
                '{"action": "set", "before": 1.5, '
                '"before_source": "default", "after": 1.7, '
                '"after_source": "db"}'
            ),
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_markup_history()
    assert rows == [{
        "id": 7,
        "ts": ts.isoformat(),
        "actor": "web",
        "kind": "set",
        "before": 1.5,
        "before_source": "default",
        "after": 1.7,
        "after_source": "db",
        "ip": "203.0.113.10",
    }]


async def test_list_markup_history_handles_clear_action():
    """``meta.action == 'clear'`` is a known kind and surfaces as
    ``kind=clear`` in the output."""
    from datetime import datetime, timezone

    ts = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 8, "ts": ts, "actor": "web", "ip": None,
            "meta": (
                '{"action": "clear", "before": 1.7, '
                '"before_source": "db", "after": 1.5, '
                '"after_source": "default"}'
            ),
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_markup_history()
    assert rows[0]["kind"] == "clear"


async def test_list_markup_history_demotes_unknown_action_to_unknown():
    """An audit row whose ``meta.action`` is missing or unrecognised
    must NOT crash the page — kind falls back to ``unknown``. This
    is the surface that protects against schema drift in a future
    PR that adds a third action verb."""
    from datetime import datetime, timezone

    ts = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 9, "ts": ts, "actor": "web", "ip": None,
            "meta": '{"action": "rename", "before": 1.5, "after": 1.6}',
        },
        {
            "id": 10, "ts": ts, "actor": "web", "ip": None,
            "meta": '{}',
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_markup_history()
    assert rows[0]["kind"] == "unknown"
    assert rows[1]["kind"] == "unknown"


async def test_list_markup_history_handles_corrupt_meta_per_row():
    """A poisoned JSON in one row must NOT blank the rest. Sibling
    test to ``test_list_admin_audit_log_demotes_unparseable_meta_to_none``
    — same fault-isolation invariant for the markup-history surface."""
    from datetime import datetime, timezone

    ts = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1, "ts": ts, "actor": "web", "ip": None,
            "meta": "{not valid json",
        },
        {
            "id": 2, "ts": ts, "actor": "web", "ip": None,
            "meta": (
                '{"action": "set", "before": 1.5, '
                '"before_source": "env", "after": 2.0, '
                '"after_source": "db"}'
            ),
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_markup_history()
    # Bad row -> None fields, no crash.
    assert rows[0]["before"] is None and rows[0]["after"] is None
    # Good row decodes cleanly.
    assert rows[1]["before"] == 1.5
    assert rows[1]["after"] == 2.0
    assert rows[1]["before_source"] == "env"
    assert rows[1]["after_source"] == "db"


async def test_list_markup_history_drops_non_finite_before_after():
    """A meta row with NaN / ±Inf in ``before`` / ``after`` must
    surface as ``None`` rather than letting NaN propagate into the
    template (which would render as ``nan×`` and crash float
    comparisons in the per-era SQL)."""
    from datetime import datetime, timezone

    ts = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[
        {
            "id": 1, "ts": ts, "actor": "web", "ip": None,
            "meta": (
                '{"action": "set", "before": "NaN", '
                '"after": "Infinity"}'
            ),
        },
    ])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    rows = await db.list_markup_history()
    assert rows[0]["before"] is None
    assert rows[0]["after"] is None


async def test_list_markup_history_empty_when_limit_zero_or_negative():
    """A 0 / negative limit should short-circuit to an empty list
    rather than dispatching a SQL query that postgres rejects."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    assert await db.list_markup_history(limit=0) == []
    assert await db.list_markup_history(limit=-5) == []
    conn.fetch.assert_not_awaited()


async def test_list_markup_history_caps_limit_at_1000():
    """Defence-in-depth: even if a caller passes a million rows
    the DB query is bounded so we can't OOM the web worker."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_markup_history(limit=10_000_000)
    assert conn.fetch.await_args.args[1] == 1000


async def test_list_markup_history_rejects_non_int_limit():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError, match="must be an int"):
        await db.list_markup_history(limit="seventeen")


# ---------------------------------------------------------------------
# get_markup_eras (Stage-15-Step-E #10b row 12)
# ---------------------------------------------------------------------


async def test_get_markup_eras_synthetic_when_no_history(monkeypatch):
    """A fresh deploy with no markup audit rows yet should still
    render the per-era card. Returns one synthetic "current" era at
    the live markup spanning the entire ``usage_logs`` table."""
    from datetime import datetime, timezone
    import pricing

    monkeypatch.setattr(pricing, "get_markup", lambda: 1.5)
    monkeypatch.setattr(pricing, "get_markup_source", lambda: "default")

    first_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])  # no history rows
    conn.fetchrow = AsyncMock(return_value={
        "first_ts": first_ts,
        "requests": 10,
        "charged": 30.0,
    })
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    eras = await db.get_markup_eras()
    assert len(eras) == 1
    era = eras[0]
    assert era["from_ts"] == first_ts.isoformat()
    assert era["to_ts"] is None
    assert era["markup"] == 1.5
    assert era["source"] == "default"
    assert era["kind"] == "current"
    assert era["requests"] == 10
    assert era["charged_usd"] == 30.0
    # OR cost = 30 / 1.5 = 20.0; margin = 30 - 20 = 10.0
    assert era["openrouter_cost_usd"] == pytest.approx(20.0)
    assert era["gross_margin_usd"] == pytest.approx(10.0)


async def test_get_markup_eras_synthetic_when_no_usage_either(monkeypatch):
    """No history rows AND no usage_logs at all should produce a
    single zeroed-out current era rather than crashing on ``None``."""
    import pricing
    monkeypatch.setattr(pricing, "get_markup", lambda: 1.5)
    monkeypatch.setattr(pricing, "get_markup_source", lambda: "default")

    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value={
        "first_ts": None, "requests": 0, "charged": 0.0,
    })
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    eras = await db.get_markup_eras()
    assert len(eras) == 1
    assert eras[0]["from_ts"] is None
    assert eras[0]["requests"] == 0
    assert eras[0]["charged_usd"] == 0.0


async def test_get_markup_eras_builds_eras_from_history():
    """With two markup-update rows in the audit log, the result is
    1 "current" era (after the latest change) + 1 historical era
    (between the two changes). Charged-USD aggregates per-era.
    """
    from datetime import datetime, timezone

    ts_old = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ts_new = datetime(2026, 2, 1, tzinfo=timezone.utc)

    conn = _make_conn()
    # First fetch (history): newest first.
    history_rows = [
        {
            "id": 2, "ts": ts_new, "actor": "web", "ip": None,
            "meta": (
                '{"action": "set", "before": 1.5, '
                '"before_source": "default", "after": 2.0, '
                '"after_source": "db"}'
            ),
        },
        {
            "id": 1, "ts": ts_old, "actor": "web", "ip": None,
            "meta": (
                '{"action": "set", "before": 1.0, '
                '"before_source": "default", "after": 1.5, '
                '"after_source": "db"}'
            ),
        },
    ]
    # Second fetch (per-era aggregate). Bucket 0 = current era
    # (markup 2.0, charged $40, 4 reqs); bucket 1 = previous era
    # (markup 1.5, charged $30, 3 reqs).
    agg_rows = [
        {"bucket": 0, "requests": 4, "charged": 40.0},
        {"bucket": 1, "requests": 3, "charged": 30.0},
    ]
    conn.fetch = AsyncMock(side_effect=[history_rows, agg_rows])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    eras = await db.get_markup_eras()
    assert len(eras) == 2
    # Current era (bucket 0).
    assert eras[0]["from_ts"] == ts_new.isoformat()
    assert eras[0]["to_ts"] is None
    assert eras[0]["markup"] == 2.0
    assert eras[0]["source"] == "db"
    assert eras[0]["kind"] == "current"
    assert eras[0]["requests"] == 4
    assert eras[0]["charged_usd"] == 40.0
    assert eras[0]["openrouter_cost_usd"] == pytest.approx(20.0)
    assert eras[0]["gross_margin_usd"] == pytest.approx(20.0)
    # Previous era (bucket 1).
    assert eras[1]["from_ts"] == ts_old.isoformat()
    assert eras[1]["to_ts"] == ts_new.isoformat()
    assert eras[1]["markup"] == 1.5
    assert eras[1]["source"] == "db"
    assert eras[1]["kind"] == "set"
    assert eras[1]["requests"] == 3
    assert eras[1]["charged_usd"] == 30.0
    assert eras[1]["openrouter_cost_usd"] == pytest.approx(20.0)
    assert eras[1]["gross_margin_usd"] == pytest.approx(10.0)


async def test_get_markup_eras_skips_row_with_unparseable_after():
    """An audit row whose ``meta.after`` is unparseable (corrupt /
    legacy / future schema) must NOT corrupt the era list — the
    affected era is silently dropped, but the surrounding eras
    still aggregate correctly."""
    from datetime import datetime, timezone

    ts_a = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ts_b = datetime(2026, 2, 1, tzinfo=timezone.utc)
    ts_c = datetime(2026, 3, 1, tzinfo=timezone.utc)

    conn = _make_conn()
    history_rows = [
        # Latest — good (current era).
        {
            "id": 3, "ts": ts_c, "actor": "web", "ip": None,
            "meta": '{"action": "set", "after": 2.0, "after_source": "db"}',
        },
        # Middle — corrupt (no after key).
        {
            "id": 2, "ts": ts_b, "actor": "web", "ip": None,
            "meta": '{"action": "set", "before": 1.5}',
        },
        # Oldest — good.
        {
            "id": 1, "ts": ts_a, "actor": "web", "ip": None,
            "meta": '{"action": "set", "after": 1.5, "after_source": "db"}',
        },
    ]
    agg_rows = [
        {"bucket": 0, "requests": 1, "charged": 2.0},
        {"bucket": 1, "requests": 1, "charged": 1.5},
    ]
    conn.fetch = AsyncMock(side_effect=[history_rows, agg_rows])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    eras = await db.get_markup_eras()
    # 2 eras: current (markup 2.0) + the "1.5" one. Middle row dropped.
    assert len(eras) == 2
    assert eras[0]["markup"] == 2.0
    assert eras[1]["markup"] == 1.5


async def test_get_markup_eras_caps_limit():
    """A massive caller-side ``limit`` should be capped at 100 so
    the WHERE-CASE clause stays bounded."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value={
        "first_ts": None, "requests": 0, "charged": 0.0,
    })
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.get_markup_eras(limit=1000)
    # The first call was list_markup_history; check it was capped.
    history_call = conn.fetch.await_args_list[0]
    # list_markup_history queries with limit + 1 = 101 (since
    # get_markup_eras passes limit+1).
    assert history_call.args[1] == 101


async def test_get_markup_eras_empty_when_limit_zero():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    assert await db.get_markup_eras(limit=0) == []


async def test_get_markup_eras_rejects_non_int_limit():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError, match="must be an int"):
        await db.get_markup_eras(limit="ten")


async def test_get_markup_eras_clamps_zero_markup_defensive():
    """A tampered audit row could persist a markup of 0 (the
    coerce paths reject < 1.0 but a malicious DB write could
    bypass that). The era's OR-cost computation must NOT divide
    by zero — the row falls back to ``openrouter_cost_usd=0`` /
    ``gross_margin_usd=charged`` rather than producing inf."""
    from datetime import datetime, timezone

    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    conn = _make_conn()
    history_rows = [
        {
            "id": 1, "ts": ts, "actor": "web", "ip": None,
            "meta": '{"action": "set", "after": 0, "after_source": "db"}',
        },
    ]
    agg_rows = [
        {"bucket": 0, "requests": 5, "charged": 10.0},
    ]
    conn.fetch = AsyncMock(side_effect=[history_rows, agg_rows])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    eras = await db.get_markup_eras()
    assert len(eras) == 1
    assert eras[0]["markup"] == 0.0
    assert eras[0]["openrouter_cost_usd"] == 0.0
    assert eras[0]["gross_margin_usd"] == 10.0


# ---------------------------------------------------------------------
# _finite_float_or_none (Stage-15-Step-E #10b row 12)
# ---------------------------------------------------------------------


def test_finite_float_or_none_passes_through_finite_floats():
    assert database_module._finite_float_or_none(1.5) == 1.5
    assert database_module._finite_float_or_none(0) == 0.0
    assert database_module._finite_float_or_none(-3.14) == -3.14


def test_finite_float_or_none_returns_none_for_none_or_invalid():
    assert database_module._finite_float_or_none(None) is None
    assert database_module._finite_float_or_none("not a number") is None
    assert database_module._finite_float_or_none(object()) is None


def test_finite_float_or_none_returns_none_for_nan_inf():
    assert database_module._finite_float_or_none(float("nan")) is None
    assert database_module._finite_float_or_none(float("inf")) is None
    assert database_module._finite_float_or_none(float("-inf")) is None


def test_finite_float_or_none_rejects_bool():
    """``bool`` is a subclass of ``int`` — must be rejected explicitly
    so ``True`` (which would coerce to 1.0) doesn't sneak through and
    silently corrupt audit-log meta parsing."""
    assert database_module._finite_float_or_none(True) is None
    assert database_module._finite_float_or_none(False) is None
