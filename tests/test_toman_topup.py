"""Stage-11-Step-B: the Toman top-up entry flow.

Covers:

* ``amt_toman`` callback transitions to ``waiting_toman_amount`` and
  renders the Toman prompt with the live rate + $2-equivalent.
* ``amt_toman`` with NO rate available refuses gracefully without
  transitioning state (prevents a dead-end FSM).
* ``waiting_toman_amount`` input handler accepts fa-digits / mixed
  separators / trailing "تومان", converts via ``convert_toman_to_usd``,
  and stashes the USD figure in FSM data (``custom_amount``) so the
  downstream currency-picker path treats it identically to a USD
  custom amount.
* ``waiting_toman_amount`` rejects below-$2-equivalent entries with
  the Toman-specific error (showing both the min and what the user
  typed, in Toman).
* The amount-picker keyboard now exposes a Toman button.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import fx_rates


@pytest.fixture(autouse=True)
def _seed_fx_rate():
    """Most tests below want a known rate. ``normalize_amount`` /
    handler logic doesn't care about rate value so long as the
    conversions are consistent — pin to 100 000 TMN/USD for easy
    mental arithmetic (400 000 TMN → $4)."""
    fx_rates._cache = fx_rates.FxRateSnapshot(
        toman_per_usd=100_000.0,
        fetched_at=time.time(),
        source="test",
    )
    yield
    fx_rates._reset_cache_for_tests()


def _make_callback(*, user_id: int = 42):
    msg = SimpleNamespace(
        edit_text=AsyncMock(),
        chat=SimpleNamespace(id=user_id),
    )
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, username="test"),
        message=msg,
        answer=AsyncMock(),
        data="amt_toman",
    )


def _make_state():
    return SimpleNamespace(
        clear=AsyncMock(),
        set_state=AsyncMock(),
        update_data=AsyncMock(),
        get_data=AsyncMock(return_value={}),
    )


def _make_message(text: str, user_id: int = 42):
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, username="test"),
        text=text,
        answer=AsyncMock(),
    )


# ---------------------------------------------------------------------
# amt_toman button + prompt
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_amt_toman_prompt_renders_rate_and_min_equivalent():
    from handlers import UserStates, process_toman_amount_request

    cb = _make_callback()
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="fa")):
        await process_toman_amount_request(cb, state)

    state.set_state.assert_awaited_once_with(UserStates.waiting_toman_amount)
    cb.answer.assert_awaited_once()
    edit_call = cb.message.edit_text.await_args
    prompt_text = edit_call.args[0]
    # Shows the current rate and the $2-equivalent figure (=$200 000 TMN here).
    assert "100,000" in prompt_text
    assert "200,000" in prompt_text


@pytest.mark.asyncio
async def test_amt_toman_with_no_rate_refuses_without_transition():
    """If fx_rates has no rate at all, we must NOT park the user in
    waiting_toman_amount (their next message would have nothing to
    convert with). Show the 'no rate' error and stay on the picker."""
    from handlers import process_toman_amount_request

    fx_rates._reset_cache_for_tests()
    with patch(
        "handlers.get_usd_to_toman_snapshot",
        new=AsyncMock(return_value=None),
    ):
        cb = _make_callback()
        state = _make_state()
        with patch(
            "handlers._get_user_language", new=AsyncMock(return_value="fa")
        ):
            await process_toman_amount_request(cb, state)

    state.set_state.assert_not_called()
    edit_call = cb.message.edit_text.await_args
    # fa string literally contains "نرخ زنده" per strings.py
    assert "نرخ زنده" in edit_call.args[0]


# ---------------------------------------------------------------------
# waiting_toman_amount input handler
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_toman_input_converts_to_usd_and_stashes_in_fsm():
    from handlers import process_toman_amount_input

    msg = _make_message("۴۰۰٬۰۰۰ تومان")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="fa")):
        await process_toman_amount_input(msg, state)

    # 400 000 / 100 000 = $4.00 stashed as custom_amount (USD path).
    state.update_data.assert_awaited_once()
    kwargs = state.update_data.await_args.kwargs
    assert kwargs["custom_amount"] == pytest.approx(4.0)
    assert kwargs["toman_entry"] == 400_000.0
    assert kwargs["toman_rate_at_entry"] == 100_000.0

    # Confirmation shows both the Toman figure and the USD figure.
    confirm = msg.answer.await_args.args[0]
    assert "400,000" in confirm
    assert "$4.00" in confirm


@pytest.mark.asyncio
async def test_toman_input_below_2_usd_equivalent_rejected():
    from handlers import process_toman_amount_input

    # 100 000 TMN at rate 100 000/USD = $1 → below $2 floor.
    msg = _make_message("100000")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")):
        await process_toman_amount_input(msg, state)

    state.update_data.assert_not_called()
    err = msg.answer.await_args.args[0]
    assert "Minimum" in err
    assert "200,000" in err  # $2 equivalent at the seeded rate
    assert "100,000" in err  # the user's entry


@pytest.mark.asyncio
async def test_toman_input_nonsense_text_rejected():
    from handlers import process_toman_amount_input

    msg = _make_message("hello world")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")):
        await process_toman_amount_input(msg, state)

    state.update_data.assert_not_called()
    assert "valid number" in msg.answer.await_args.args[0]


@pytest.mark.asyncio
async def test_toman_input_above_10k_usd_equivalent_rejected():
    """100 000 000 000 TMN at rate 100 000/USD = $1 000 000 → well
    over the $10k invoice cap. Reject, don't create a NowPayments
    invoice we'd never close out."""
    from handlers import process_toman_amount_input

    msg = _make_message("100000000000")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")):
        await process_toman_amount_input(msg, state)

    state.update_data.assert_not_called()


@pytest.mark.asyncio
async def test_toman_input_with_no_rate_refuses_after_user_typed():
    """Rate may disappear between prompt and submit (outage). Refuse
    with the same 'no rate' string rather than silently using 0."""
    from handlers import process_toman_amount_input

    fx_rates._reset_cache_for_tests()
    with patch(
        "handlers.convert_toman_to_usd",
        new=AsyncMock(return_value=None),
    ), patch(
        "handlers.get_usd_to_toman_snapshot",
        new=AsyncMock(return_value=None),
    ):
        msg = _make_message("400000")
        state = _make_state()
        with patch(
            "handlers._get_user_language", new=AsyncMock(return_value="fa")
        ):
            await process_toman_amount_input(msg, state)

    state.update_data.assert_not_called()
    err = msg.answer.await_args.args[0]
    assert "نرخ زنده" in err


# ---------------------------------------------------------------------
# Amount-picker keyboard wiring
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_amount_picker_includes_toman_button():
    from handlers import _render_charge_pick_amount

    msg = SimpleNamespace(edit_text=AsyncMock())
    state = _make_state()
    await _render_charge_pick_amount(msg, "fa", state)
    markup = msg.edit_text.await_args.kwargs["reply_markup"]
    callbacks = [btn.callback_data for row in markup.inline_keyboard for btn in row]
    assert "amt_custom" in callbacks
    assert "amt_toman" in callbacks
