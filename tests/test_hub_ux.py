"""Tests for the post-cleanup hub UX (Stage-9-Step-1.5):

* The hub keyboard now shows 6 buttons including a dedicated
  "🧠 Memory: ON/OFF" toggle alongside "🆕 New Chat".
* Tapping "🆕 New Chat" wipes the conversation buffer immediately
  rather than opening a settings screen.
* Tapping "🧠 Memory: …" opens the memory settings screen.
* The wallet keyboard exposes a "🎁 Redeem gift code" button and a
  matching FSM input flow that reuses the same eligibility
  branches as ``/redeem CODE``.
* ``_render_memory_screen`` swallows only ``TelegramBadRequest``
  (the legitimate "message is not modified" / parse-mode no-op
  case), not every ``Exception``. Pre-fix any unrelated DB / network
  exception on ``edit_text`` was silenced as a single ``log.debug``
  line, masking real bugs.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from aiogram.exceptions import TelegramBadRequest


def _make_callback(*, user_id: int = 12345, edit_text_side_effect=None):
    """Build a CallbackQuery-shaped stub the handlers can chew on."""
    msg = SimpleNamespace(
        edit_text=AsyncMock(side_effect=edit_text_side_effect),
        chat=SimpleNamespace(id=user_id),
    )
    cb = SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, username="tester"),
        message=msg,
        answer=AsyncMock(),
        data="",
    )
    return cb


def _make_state():
    return SimpleNamespace(
        clear=AsyncMock(),
        set_state=AsyncMock(),
        set_data=AsyncMock(),
        get_data=AsyncMock(return_value={}),
    )


# --- Hub keyboard ----------------------------------------------------------


@pytest.mark.asyncio
async def test_hub_keyboard_has_six_buttons_including_memory_toggle():
    """Pre-fix the hub had 5 buttons and the memory toggle was hidden
    behind "🆕 New Chat". Now there's a dedicated memory button."""
    from handlers import _hub_text_and_kb

    fake_user = {
        "active_model": "openai/gpt-4o",
        "balance_usd": 1.23,
        "memory_enabled": False,
    }
    with patch("handlers.db.get_user", new=AsyncMock(return_value=fake_user)):
        text, kb = await _hub_text_and_kb(12345, "en")

    markup = kb.as_markup()
    flat = [btn for row in markup.inline_keyboard for btn in row]
    callbacks = [b.callback_data for b in flat]
    assert "hub_wallet" in callbacks
    assert "hub_models" in callbacks
    assert "hub_newchat" in callbacks
    assert "hub_memory" in callbacks
    assert "hub_support" in callbacks
    assert "hub_language" in callbacks
    assert len(flat) == 6


@pytest.mark.asyncio
async def test_hub_memory_button_label_reflects_current_state():
    """When memory is OFF the button text contains the OFF state
    marker so the user can see at a glance what tapping it will do."""
    from handlers import _hub_text_and_kb

    fake_user = {
        "active_model": "x",
        "balance_usd": 0.0,
        "memory_enabled": True,
    }
    with patch("handlers.db.get_user", new=AsyncMock(return_value=fake_user)):
        _, kb = await _hub_text_and_kb(12345, "en")
    flat = [btn for row in kb.as_markup().inline_keyboard for btn in row]
    memory_btn = next(b for b in flat if b.callback_data == "hub_memory")
    assert "ON" in memory_btn.text


# --- "New Chat" button now wipes immediately --------------------------------


@pytest.mark.asyncio
async def test_hub_newchat_handler_wipes_conversation_and_toasts_count():
    from handlers import hub_newchat_handler

    cb = _make_callback()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.clear_conversation", new=AsyncMock(return_value=7)
    ) as mock_clear:
        await hub_newchat_handler(cb, state)

    mock_clear.assert_awaited_once_with(12345)
    cb.answer.assert_awaited_once()
    sent = cb.answer.await_args.args[0]
    assert "7" in sent
    # The hub message itself is left in place — we don't edit_text.
    cb.message.edit_text.assert_not_called()


@pytest.mark.asyncio
async def test_hub_newchat_handler_when_buffer_empty_and_memory_off_nudges_user():
    """If the user taps New Chat while memory is OFF and there's
    nothing to wipe, surface a helpful alert pointing at the memory
    toggle rather than silently doing nothing."""
    from handlers import hub_newchat_handler

    cb = _make_callback()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.clear_conversation", new=AsyncMock(return_value=0)
    ), patch(
        "handlers.db.get_memory_enabled", new=AsyncMock(return_value=False)
    ):
        await hub_newchat_handler(cb, state)

    cb.answer.assert_awaited_once()
    kwargs = cb.answer.await_args.kwargs
    assert kwargs.get("show_alert") is True


@pytest.mark.asyncio
async def test_hub_newchat_handler_when_buffer_empty_and_memory_on_quiet_toast():
    """Memory ON + nothing to clear → quiet toast (not an alert).
    User asked to wipe, there's nothing to wipe, no big deal."""
    from handlers import hub_newchat_handler

    cb = _make_callback()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.clear_conversation", new=AsyncMock(return_value=0)
    ), patch(
        "handlers.db.get_memory_enabled", new=AsyncMock(return_value=True)
    ):
        await hub_newchat_handler(cb, state)
    cb.answer.assert_awaited_once()
    kwargs = cb.answer.await_args.kwargs
    assert kwargs.get("show_alert") is not True


# --- Memory button opens the settings screen --------------------------------


@pytest.mark.asyncio
async def test_hub_memory_handler_opens_settings_screen():
    from handlers import hub_memory_handler

    cb = _make_callback()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_memory_enabled", new=AsyncMock(return_value=False)
    ):
        await hub_memory_handler(cb, state)

    cb.message.edit_text.assert_awaited_once()
    sent_text = cb.message.edit_text.await_args.args[0]
    assert "memory" in sent_text.lower()
    cb.answer.assert_awaited_once()


# --- _render_memory_screen exception tightening (bundled bug fix) ----------


@pytest.mark.asyncio
async def test_render_memory_screen_swallows_telegram_bad_request():
    """The "message is not modified" no-op is a TelegramBadRequest;
    the handler must continue normally."""
    from handlers import _render_memory_screen

    bad_req = TelegramBadRequest(method=None, message="message is not modified")
    cb = _make_callback(edit_text_side_effect=bad_req)
    with patch(
        "handlers.db.get_memory_enabled", new=AsyncMock(return_value=True)
    ):
        # Must not raise.
        await _render_memory_screen(cb, "en")


@pytest.mark.asyncio
async def test_render_memory_screen_propagates_other_exceptions():
    """Pre-fix bug: ``except Exception`` swallowed every error here,
    masking DB drops, ``TelegramForbiddenError`` (bot was blocked),
    and unrelated network blips. Post-fix only TelegramBadRequest is
    swallowed; everything else propagates so it surfaces in logs."""
    from handlers import _render_memory_screen

    cb = _make_callback(edit_text_side_effect=RuntimeError("boom"))
    with patch(
        "handlers.db.get_memory_enabled", new=AsyncMock(return_value=False)
    ):
        with pytest.raises(RuntimeError):
            await _render_memory_screen(cb, "en")


# --- Wallet redeem button ---------------------------------------------------


def test_wallet_keyboard_contains_redeem_gift_button():
    """Pre-fix wallet kb only had Add crypto + Back. Now it also has
    the gift-code redemption button so users can find the flow without
    typing /redeem."""
    from handlers import _build_wallet_keyboard

    kb = _build_wallet_keyboard("en")
    flat = [btn for row in kb.as_markup().inline_keyboard for btn in row]
    callbacks = [b.callback_data for b in flat]
    assert "add_crypto" in callbacks
    assert "hub_redeem_gift" in callbacks


@pytest.mark.asyncio
async def test_hub_redeem_gift_handler_arms_fsm_state_and_renders_prompt():
    from handlers import UserStates, hub_redeem_gift_handler

    cb = _make_callback()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ):
        await hub_redeem_gift_handler(cb, state)

    state.set_state.assert_awaited_once_with(UserStates.waiting_gift_code)
    cb.message.edit_text.assert_awaited_once()
    sent_text = cb.message.edit_text.await_args.args[0]
    assert "gift code" in sent_text.lower() or "Send" in sent_text


@pytest.mark.asyncio
async def test_process_gift_code_input_redeems_and_clears_state():
    from handlers import process_gift_code_input

    msg = SimpleNamespace(
        text="GIFT5",
        from_user=SimpleNamespace(id=999, username="tester"),
        answer=AsyncMock(),
    )
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.redeem_gift_code",
        new=AsyncMock(
            return_value={
                "status": "ok",
                "amount_usd": 5.0,
                "new_balance_usd": 17.5,
                "transaction_id": 123,
            }
        ),
    ) as mock_redeem:
        await process_gift_code_input(msg, state)

    mock_redeem.assert_awaited_once_with("GIFT5", 999)
    state.set_state.assert_awaited_once_with(None)
    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    assert "$5.00" in sent
    assert "$17.50" in sent


@pytest.mark.asyncio
async def test_process_gift_code_input_returns_silently_when_from_user_none():
    """Same defensive guard as the other waiting_* handlers."""
    from handlers import process_gift_code_input

    msg = SimpleNamespace(text="GIFT5", from_user=None, answer=AsyncMock())
    state = _make_state()
    await process_gift_code_input(msg, state)
    msg.answer.assert_not_called()
    state.set_state.assert_not_called()


@pytest.mark.asyncio
async def test_process_gift_code_input_blank_input_shows_bad_code():
    from handlers import process_gift_code_input

    msg = SimpleNamespace(
        text="   ",
        from_user=SimpleNamespace(id=999, username="tester"),
        answer=AsyncMock(),
    )
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.redeem_gift_code", new=AsyncMock()
    ) as mock_redeem:
        await process_gift_code_input(msg, state)
    mock_redeem.assert_not_awaited()
    msg.answer.assert_awaited_once()


# --- Shared redeem helper ---------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,expected_substr",
    [
        ("not_found", "not found"),
        ("inactive", "revoked"),
        ("expired", "expired"),
        ("exhausted", "fully redeemed"),
        ("already_redeemed", "already redeemed"),
        ("user_unknown", "/start"),
    ],
)
async def test_redeem_code_for_user_status_branches(status, expected_substr):
    """The shared helper used by both /redeem and the wallet-menu
    flow maps every DB status to a friendly localized message."""
    from handlers import _redeem_code_for_user

    with patch(
        "handlers.db.redeem_gift_code",
        new=AsyncMock(
            return_value={
                "status": status,
                "amount_usd": None,
                "new_balance_usd": None,
                "transaction_id": None,
            }
        ),
    ):
        reply = await _redeem_code_for_user(999, "GIFT5", "en")
    assert expected_substr in reply


@pytest.mark.asyncio
async def test_redeem_code_for_user_db_exception_returns_friendly_error():
    from handlers import _redeem_code_for_user

    with patch(
        "handlers.db.redeem_gift_code",
        new=AsyncMock(side_effect=RuntimeError("pool exhausted")),
    ):
        reply = await _redeem_code_for_user(999, "GIFT5", "en")
    assert "wrong" in reply.lower() or "later" in reply.lower()
