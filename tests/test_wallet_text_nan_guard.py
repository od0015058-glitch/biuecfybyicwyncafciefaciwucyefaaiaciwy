"""Regression pins for the wallet-view + redeem-confirmation NaN
guards.

Stage-13-Step-A fixed the ``hub_title`` rendering against a corrupted
``users.balance_usd`` row (legacy ``NaN`` from a manual SQL fix gone
wrong, or any future write site that sneaks past the ``_is_finite_amount``
check). The same regression sits one screen over: ``wallet_text``
(rendered by ``hub_wallet_handler`` and ``back_to_wallet_handler``)
and ``redeem_ok`` (rendered by ``_redeem_code_for_user``) both pass
the raw ``float()``-coerced balance into ``strings.t``, which formats
``${balance:.2f}`` directly. ``f"${math.nan:.2f}"`` renders literally
``$nan`` in Python — without these guards a corrupted row would leak
the same broken UI that Stage-13-Step-A removed from the hub.

These tests pin the fix at the call sites; the broader module-level
:func:`format_balance_block` already has its own NaN/Inf coverage in
``tests/test_wallet_display.py`` for the future wallet sub-screens
that route through it (e.g. the post-credit DM body).
"""

from __future__ import annotations

import math
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------
# hub_wallet_handler — primary wallet entry point from the hub.
# ---------------------------------------------------------------------


def _make_callback(user_id: int = 555) -> MagicMock:
    """Build a minimal ``CallbackQuery`` mock that the wallet handlers
    accept. ``edit_text`` captures the rendered body so the test can
    assert against it."""
    cb = MagicMock()
    cb.from_user = SimpleNamespace(id=user_id)
    cb.message = MagicMock()
    cb.message.edit_text = AsyncMock()
    cb.answer = AsyncMock()
    return cb


@pytest.mark.asyncio
async def test_hub_wallet_handler_nan_balance_renders_zero_not_nan():
    """A corrupted ``users.balance_usd = NaN`` row must NOT leak
    ``$nan`` into the wallet view. Falls back to ``$0.00`` per the
    same policy ``_hub_text_and_kb`` adopted in Stage-13-Step-A."""
    import handlers

    cb = _make_callback()
    state = MagicMock()
    state.clear = AsyncMock()
    fake_user = {"balance_usd": math.nan}

    with (
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(handlers.db, "get_user", AsyncMock(return_value=fake_user)),
        patch.object(
            handlers, "get_usd_to_toman_snapshot", AsyncMock(return_value=None)
        ),
    ):
        await handlers.hub_wallet_handler(cb, state)

    body = cb.message.edit_text.await_args.args[0]
    assert "nan" not in body.lower()
    assert "$0.00" in body


@pytest.mark.asyncio
async def test_hub_wallet_handler_inf_balance_renders_zero_not_inf():
    """``+Infinity`` is the symmetric failure mode: ``f"${inf:.2f}"``
    renders ``$inf``. Same fallback."""
    import handlers

    cb = _make_callback()
    state = MagicMock()
    state.clear = AsyncMock()
    fake_user = {"balance_usd": math.inf}

    with (
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(handlers.db, "get_user", AsyncMock(return_value=fake_user)),
        patch.object(
            handlers, "get_usd_to_toman_snapshot", AsyncMock(return_value=None)
        ),
    ):
        await handlers.hub_wallet_handler(cb, state)

    body = cb.message.edit_text.await_args.args[0]
    assert "inf" not in body.lower()
    assert "$0.00" in body


@pytest.mark.asyncio
async def test_hub_wallet_handler_finite_balance_passes_through():
    """Sanity: a normal balance must NOT be rewritten to $0.00 by the
    NaN guard. Pins that the guard is conditional on ``isfinite`` and
    not a blanket override."""
    import handlers

    cb = _make_callback()
    state = MagicMock()
    state.clear = AsyncMock()
    fake_user = {"balance_usd": 12.34}

    with (
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(handlers.db, "get_user", AsyncMock(return_value=fake_user)),
        patch.object(
            handlers, "get_usd_to_toman_snapshot", AsyncMock(return_value=None)
        ),
    ):
        await handlers.hub_wallet_handler(cb, state)

    body = cb.message.edit_text.await_args.args[0]
    assert "$12.34" in body
    assert "$0.00" not in body


# ---------------------------------------------------------------------
# back_to_wallet_handler — same template, separate code path (the user
# returns from the charge flow). Both call sites must be guarded.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_back_to_wallet_handler_nan_balance_renders_zero_not_nan():
    import handlers

    cb = _make_callback()
    state = MagicMock()
    state.clear = AsyncMock()
    fake_user = {"balance_usd": math.nan}

    with (
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(handlers.db, "get_user", AsyncMock(return_value=fake_user)),
        patch.object(
            handlers, "get_usd_to_toman_snapshot", AsyncMock(return_value=None)
        ),
    ):
        await handlers.back_to_wallet_handler(cb, state)

    body = cb.message.edit_text.await_args.args[0]
    assert "nan" not in body.lower()
    assert "$0.00" in body


@pytest.mark.asyncio
async def test_back_to_wallet_handler_negative_inf_balance_renders_zero():
    """``-Infinity`` is rare (the wallet ledger guards against
    minting a negative balance, and ``isfinite`` rejects both
    polarities), but a manual SQL fix could in theory write one.
    Same fallback. Pinning the symmetry keeps the policy explicit."""
    import handlers

    cb = _make_callback()
    state = MagicMock()
    state.clear = AsyncMock()
    fake_user = {"balance_usd": -math.inf}

    with (
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(handlers.db, "get_user", AsyncMock(return_value=fake_user)),
        patch.object(
            handlers, "get_usd_to_toman_snapshot", AsyncMock(return_value=None)
        ),
    ):
        await handlers.back_to_wallet_handler(cb, state)

    body = cb.message.edit_text.await_args.args[0]
    assert "inf" not in body.lower()
    assert "$0.00" in body


# ---------------------------------------------------------------------
# _redeem_code_for_user — the redeem-success template formats the
# post-credit balance the same way ``wallet_text`` does. Same guard.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redeem_ok_nan_new_balance_renders_zero_not_nan():
    """If ``redeem_gift_code`` ever returns a non-finite
    ``new_balance_usd`` (legacy DB row, future bug in the credit
    path that bypasses the ledger guards), the ``redeem_ok``
    confirmation must NOT leak ``$nan``."""
    import handlers

    redeem_result = {
        "status": "ok",
        "amount_usd": 5.00,
        "new_balance_usd": math.nan,
    }
    with patch.object(
        handlers.db, "redeem_gift_code", AsyncMock(return_value=redeem_result)
    ):
        out = await handlers._redeem_code_for_user(123, "TESTCODE", "en")

    assert "nan" not in out.lower()
    # The amount itself is finite ($5.00) and IS expected to render.
    # The post-credit balance is the part that must be $0.00 fallback.
    assert "$0.00" in out


@pytest.mark.asyncio
async def test_redeem_ok_finite_new_balance_passes_through():
    """Sanity pin: a normal redemption must show the actual new
    balance, not the fallback."""
    import handlers

    redeem_result = {
        "status": "ok",
        "amount_usd": 5.00,
        "new_balance_usd": 17.34,
    }
    with patch.object(
        handlers.db, "redeem_gift_code", AsyncMock(return_value=redeem_result)
    ):
        out = await handlers._redeem_code_for_user(123, "TESTCODE", "en")

    assert "$17.34" in out
