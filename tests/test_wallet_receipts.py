"""Stage-12-Step-C: tests for the user-facing wallet receipts feature.

Three layers under test:

1. ``Database.list_user_transactions`` — the new DB method that
   hard-codes the ``WHERE telegram_id = …`` clause so a future
   buggy caller can't drop the user-scope filter and leak someone
   else's history.
2. ``wallet_receipts.format_receipt_line`` / ``get_receipts_page_size``
   — the rendering helpers (status badges, gateway-friendly labels,
   TetraPay locked-rate Toman annotation, env-driven page size).
3. ``handlers.hub_receipts_handler`` / ``receipts_more_handler`` —
   the wallet sub-screen and its cursor-paginated "Show more" flow.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import database as database_module
import wallet_receipts
from tests.test_database_queries import _PoolStub, _make_conn


# ---------------------------------------------------------------------
# get_receipts_page_size
# ---------------------------------------------------------------------


def test_get_receipts_page_size_default_is_5():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("RECEIPTS_PAGE_SIZE", None)
        assert wallet_receipts.get_receipts_page_size() == 5


def test_get_receipts_page_size_clamps_to_max():
    """A typo of 9999 must clamp to RECEIPTS_PAGE_SIZE_MAX (20) so the
    rendered receipt page never blows the 4 KB Telegram body cap."""
    with patch.dict(os.environ, {"RECEIPTS_PAGE_SIZE": "9999"}):
        assert wallet_receipts.get_receipts_page_size() == 20


def test_get_receipts_page_size_clamps_to_min():
    with patch.dict(os.environ, {"RECEIPTS_PAGE_SIZE": "0"}):
        assert wallet_receipts.get_receipts_page_size() == 1


def test_get_receipts_page_size_typo_falls_back():
    with patch.dict(os.environ, {"RECEIPTS_PAGE_SIZE": "abc"}):
        assert wallet_receipts.get_receipts_page_size() == 5


# ---------------------------------------------------------------------
# format_receipt_line
# ---------------------------------------------------------------------


def _row(**overrides) -> dict:
    base = {
        "id": 1,
        "gateway": "nowpayments",
        "currency": "USDT-TRC20",
        "amount_crypto_or_rial": None,
        "amount_usd": 5.0,
        "status": "SUCCESS",
        "gateway_invoice_id": "abc",
        "created_at": "2024-03-12T10:00:00",
        "completed_at": "2024-03-12T10:05:00",
        "refunded_at": None,
        "gateway_locked_rate_toman_per_usd": None,
    }
    base.update(overrides)
    return base


def test_format_receipt_line_nowpayments_success():
    line = wallet_receipts.format_receipt_line(_row(), lang="en")
    assert "✅" in line
    assert "$5.00" in line
    assert "USDT-TRC20" in line
    assert "2024-03-12" in line


def test_format_receipt_line_partial_uses_partial_badge():
    line = wallet_receipts.format_receipt_line(
        _row(status="PARTIAL", amount_usd=3.50), lang="en"
    )
    assert "⚠️" in line
    assert "$3.50" in line


def test_format_receipt_line_refunded_uses_refunded_date():
    """A REFUNDED row's date must come from ``refunded_at`` — that's
    when the refund actually went through, which is the date the
    user wants to see, not the original payment date."""
    line = wallet_receipts.format_receipt_line(
        _row(
            status="REFUNDED",
            completed_at="2024-03-12T10:05:00",
            refunded_at="2024-03-20T15:30:00",
        ),
        lang="en",
    )
    assert "🔄" in line
    assert "2024-03-20" in line  # the refund date, not the payment date
    assert "2024-03-12" not in line


def test_format_receipt_line_tetrapay_renders_locked_toman():
    """A TetraPay row must render the rial-equivalent at the locked
    rate, NOT the live snapshot — the user verifies against what
    they actually paid, which is the locked amount."""
    line = wallet_receipts.format_receipt_line(
        _row(
            gateway="tetrapay",
            currency="IRR",
            amount_usd=5.0,
            gateway_locked_rate_toman_per_usd=82_500.0,
        ),
        lang="en",
    )
    assert "TetraPay" in line
    assert "412,500 TMN" in line  # 5.0 * 82_500


def test_format_receipt_line_tetrapay_without_locked_rate_omits_toman():
    """Legacy TetraPay rows (pre alembic 0011) lack the locked rate
    column. Render the row without the Toman annotation rather than
    fabricating one with the live rate."""
    line = wallet_receipts.format_receipt_line(
        _row(
            gateway="tetrapay",
            currency="IRR",
            amount_usd=5.0,
            gateway_locked_rate_toman_per_usd=None,
        ),
        lang="en",
    )
    assert "TetraPay" in line
    assert "TMN" not in line


def test_format_receipt_line_tetrapay_rejects_nonfinite_rate():
    """A NaN locked rate must NOT render ``≈ nan TMN`` — match the
    NaN-defense policy in :mod:`wallet_display`."""
    line = wallet_receipts.format_receipt_line(
        _row(
            gateway="tetrapay",
            amount_usd=5.0,
            gateway_locked_rate_toman_per_usd=float("nan"),
        ),
        lang="en",
    )
    assert "nan" not in line.lower()
    assert "TMN" not in line


def test_format_receipt_line_admin_credit_has_friendly_label():
    line = wallet_receipts.format_receipt_line(
        _row(gateway="admin", currency="USD"), lang="en"
    )
    assert "Manual credit" in line


def test_format_receipt_line_gift_redemption_has_friendly_label():
    line = wallet_receipts.format_receipt_line(
        _row(gateway="gift", currency="USD"), lang="en"
    )
    assert "Gift code" in line


def test_format_receipt_line_handles_nonfinite_amount():
    """A NaN amount renders as ``$0.00`` — same NaN-defense policy as
    :mod:`wallet_display.format_balance_block`. Never ``$nan``."""
    line = wallet_receipts.format_receipt_line(
        _row(amount_usd=float("nan")), lang="en"
    )
    assert "$0.00" in line
    assert "nan" not in line.lower()


def test_format_receipt_line_falls_back_to_created_at_when_completed_at_missing():
    """Legacy rows might have a NULL completed_at; use created_at
    instead of crashing or rendering '—'."""
    line = wallet_receipts.format_receipt_line(
        _row(status="SUCCESS", completed_at=None,
             created_at="2024-01-15T08:00:00"),
        lang="en",
    )
    assert "2024-01-15" in line


# ---------------------------------------------------------------------
# Database.list_user_transactions
# ---------------------------------------------------------------------


async def test_list_user_transactions_rejects_zero_telegram_id():
    """The whole point of this method is to *guarantee* the
    ``WHERE telegram_id = …`` filter is present. A ``0`` telegram_id
    must crash loudly — silently returning everything would defeat
    the safety purpose."""
    db = database_module.Database()
    with pytest.raises(ValueError, match="telegram_id"):
        await db.list_user_transactions(telegram_id=0, limit=5)


async def test_list_user_transactions_rejects_negative_telegram_id():
    db = database_module.Database()
    with pytest.raises(ValueError, match="telegram_id"):
        await db.list_user_transactions(telegram_id=-1, limit=5)


async def test_list_user_transactions_rejects_invalid_before_id():
    db = database_module.Database()
    with pytest.raises(ValueError, match="before_id"):
        await db.list_user_transactions(
            telegram_id=42, limit=5, before_id=0
        )


async def test_list_user_transactions_first_page_sql_pinned():
    """First page (no cursor) must filter by both telegram_id AND the
    user-visible status whitelist. NEVER touch PENDING / EXPIRED /
    FAILED — those are operational state, not paid receipts."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_user_transactions(telegram_id=42, limit=5)

    sql = conn.fetch.await_args.args[0]
    normalized = " ".join(sql.split())
    assert "WHERE telegram_id = $1" in normalized
    assert "status = ANY($2::text[])" in normalized
    # The cursor clause must NOT be present on a first-page request
    # — that branch is for ``before_id``.
    assert "transaction_id <" not in normalized
    assert "ORDER BY transaction_id DESC" in normalized
    # The status whitelist must be exactly the user-visible set.
    statuses = conn.fetch.await_args.args[2]
    assert set(statuses) == {"SUCCESS", "PARTIAL", "REFUNDED"}


async def test_list_user_transactions_with_cursor_uses_before_clause():
    """``before_id=99`` must add ``AND transaction_id < $3`` so the
    next page returns rows older than the cursor — stable cursor
    pagination, no shifting when a fresh top-up lands."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_user_transactions(telegram_id=42, limit=5, before_id=99)

    sql = conn.fetch.await_args.args[0]
    normalized = " ".join(sql.split())
    assert "transaction_id < $3" in normalized
    args = conn.fetch.await_args.args
    assert args[1] == 42
    assert args[3] == 99
    assert args[4] == 6  # limit + 1 for has_more probe


async def test_list_user_transactions_clamps_limit():
    """``limit=999`` must clamp to USER_RECEIPTS_MAX_PER_PAGE (20).
    The hard cap is independent of the env-driven default — even
    a buggy caller bypassing the env helper can't blow Telegram's
    body cap."""
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_user_transactions(telegram_id=42, limit=9999)
    args = conn.fetch.await_args.args
    # First-page (no cursor): limit is the LAST positional bind.
    assert args[-1] == 21  # 20 + 1


async def test_list_user_transactions_returns_normalized_rows():
    """Async-pg Records → plain dicts with float ``amount_usd`` so the
    rendering layer can format without re-typing. Each timestamp is
    ISO-8601 (or None for legacy rows)."""
    from datetime import datetime, timezone
    completed = datetime(2024, 3, 12, 10, 5, tzinfo=timezone.utc)
    refunded = datetime(2024, 3, 20, 15, 30, tzinfo=timezone.utc)

    conn = _make_conn()
    conn.fetch = AsyncMock(
        return_value=[
            {
                "transaction_id": 7,
                "gateway": "tetrapay",
                "currency_used": "IRR",
                "amount_crypto_or_rial": 412_500.0,
                "amount_usd_credited": 5.0,
                "status": "SUCCESS",
                "gateway_invoice_id": "auth-7",
                "created_at": completed,
                "completed_at": completed,
                "refunded_at": None,
                "gateway_locked_rate_toman_per_usd": 82_500.0,
            },
            {
                "transaction_id": 6,
                "gateway": "nowpayments",
                "currency_used": "USDT-TRC20",
                "amount_crypto_or_rial": 3.5,
                "amount_usd_credited": 3.5,
                "status": "REFUNDED",
                "gateway_invoice_id": "abc",
                "created_at": completed,
                "completed_at": completed,
                "refunded_at": refunded,
                "gateway_locked_rate_toman_per_usd": None,
            },
        ]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    page = await db.list_user_transactions(telegram_id=42, limit=5)
    assert page["has_more"] is False
    assert page["next_before_id"] is None
    assert len(page["rows"]) == 2
    r0, r1 = page["rows"]
    assert r0["id"] == 7
    assert r0["gateway"] == "tetrapay"
    assert r0["amount_usd"] == 5.0
    assert r0["gateway_locked_rate_toman_per_usd"] == 82_500.0
    assert r0["completed_at"].startswith("2024-03-12")
    assert r1["status"] == "REFUNDED"
    assert r1["refunded_at"].startswith("2024-03-20")


async def test_list_user_transactions_has_more_when_extra_row_returned():
    """When the DB returns ``limit + 1`` rows, ``has_more`` is True
    and the (limit+1)-th row is trimmed off. ``next_before_id`` is
    the smallest tx-id in the trimmed page (the cursor for the next
    page)."""
    rows = [
        {
            "transaction_id": tx_id,
            "gateway": "nowpayments",
            "currency_used": "BTC",
            "amount_crypto_or_rial": 0.0001,
            "amount_usd_credited": 5.0,
            "status": "SUCCESS",
            "gateway_invoice_id": f"inv-{tx_id}",
            "created_at": None,
            "completed_at": None,
            "refunded_at": None,
            "gateway_locked_rate_toman_per_usd": None,
        }
        for tx_id in (10, 9, 8, 7, 6, 5)  # 6 rows, limit=5 → has_more
    ]
    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=rows)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    page = await db.list_user_transactions(telegram_id=42, limit=5)
    assert page["has_more"] is True
    assert len(page["rows"]) == 5  # the (limit+1)-th row is trimmed
    # Cursor is the smallest tx-id IN THE PAGE — the trimmed row's
    # tx-id is what the next query starts below.
    assert page["next_before_id"] == 6


# ---------------------------------------------------------------------
# handlers.hub_receipts_handler / receipts_more_handler
# ---------------------------------------------------------------------


def _make_callback(*, user_id: int = 42, data: str = "hub_receipts"):
    msg = SimpleNamespace(
        edit_text=AsyncMock(),
        chat=SimpleNamespace(id=user_id),
    )
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, username="u"),
        message=msg,
        answer=AsyncMock(),
        data=data,
    )


def _make_state():
    return SimpleNamespace(
        clear=AsyncMock(),
        set_state=AsyncMock(),
    )


async def test_hub_receipts_handler_renders_first_page():
    """First page renders the title + the formatted receipt lines."""
    from handlers import hub_receipts_handler

    callback = _make_callback()
    state = _make_state()
    page = {
        "rows": [_row(id=7, amount_usd=5.0, currency="USDT-TRC20")],
        "has_more": False,
        "next_before_id": None,
    }
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.list_user_transactions",
        new=AsyncMock(return_value=page),
    ):
        await hub_receipts_handler(callback, state)

    rendered = callback.message.edit_text.await_args.args[0]
    assert "Recent top-ups" in rendered
    assert "$5.00" in rendered
    assert "USDT-TRC20" in rendered


async def test_hub_receipts_handler_empty_state_for_brand_new_user():
    """No transactions → friendly empty-state copy, NOT a blank list."""
    from handlers import hub_receipts_handler

    callback = _make_callback()
    state = _make_state()
    page = {"rows": [], "has_more": False, "next_before_id": None}
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.list_user_transactions",
        new=AsyncMock(return_value=page),
    ):
        await hub_receipts_handler(callback, state)

    rendered = callback.message.edit_text.await_args.args[0]
    assert "No top-ups yet" in rendered


async def test_hub_receipts_handler_passes_user_id_to_db():
    """The user-scope filter is the safety contract here. Pin that
    we forward ``callback.from_user.id`` to the DB method as
    ``telegram_id`` on every render."""
    from handlers import hub_receipts_handler

    callback = _make_callback(user_id=123)
    state = _make_state()
    page = {"rows": [], "has_more": False, "next_before_id": None}
    list_mock = AsyncMock(return_value=page)
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.list_user_transactions", new=list_mock
    ):
        await hub_receipts_handler(callback, state)

    list_mock.assert_awaited_once()
    kwargs = list_mock.await_args.kwargs
    assert kwargs["telegram_id"] == 123
    assert kwargs["before_id"] is None


async def test_hub_receipts_handler_renders_show_more_button_when_has_more():
    """A populated page with ``has_more=True`` must render the
    ⏬ Show more button with the cursor in the callback_data."""
    from handlers import hub_receipts_handler

    callback = _make_callback()
    state = _make_state()
    page = {
        "rows": [_row(id=7)],
        "has_more": True,
        "next_before_id": 7,
    }
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.list_user_transactions",
        new=AsyncMock(return_value=page),
    ):
        await hub_receipts_handler(callback, state)

    markup = callback.message.edit_text.await_args.kwargs["reply_markup"]
    callback_datas = [
        b.callback_data
        for row in markup.inline_keyboard
        for b in row
        if b.callback_data
    ]
    assert any(c == "receipts_more:7" for c in callback_datas)


async def test_receipts_more_handler_parses_cursor_and_passes_to_db():
    from handlers import receipts_more_handler

    callback = _make_callback(data="receipts_more:42")
    state = _make_state()
    page = {"rows": [], "has_more": False, "next_before_id": None}
    list_mock = AsyncMock(return_value=page)
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.list_user_transactions", new=list_mock
    ):
        await receipts_more_handler(callback, state)
    kwargs = list_mock.await_args.kwargs
    assert kwargs["before_id"] == 42


async def test_receipts_more_handler_tampered_payload_falls_back_to_first_page():
    """A malformed callback payload (``receipts_more:abc``) must not
    500 — fall back to a first-page render."""
    from handlers import receipts_more_handler

    callback = _make_callback(data="receipts_more:abc")
    state = _make_state()
    page = {"rows": [], "has_more": False, "next_before_id": None}
    list_mock = AsyncMock(return_value=page)
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.list_user_transactions", new=list_mock
    ):
        await receipts_more_handler(callback, state)
    kwargs = list_mock.await_args.kwargs
    assert kwargs["before_id"] is None


async def test_wallet_keyboard_includes_receipts_button():
    """The wallet keyboard must surface the ``hub_receipts`` callback
    so the receipts feed is reachable from the menu."""
    from handlers import _build_wallet_keyboard

    builder = _build_wallet_keyboard("en")
    markup = builder.as_markup()
    callback_datas = [
        b.callback_data
        for row in markup.inline_keyboard
        for b in row
        if b.callback_data
    ]
    assert "hub_receipts" in callback_datas
