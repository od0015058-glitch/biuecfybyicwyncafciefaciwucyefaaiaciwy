"""Regression tests for the ``message.from_user is None`` defensive
guards in the FSM-state handlers.

Anonymous-group-admin / channel-bot edge cases deliver messages with
``from_user`` set to None. Touching ``message.from_user.id`` without
the guard raises ``AttributeError`` and bubbles up as a poller-level
crash for that update. Same pattern that PR #51 fixed in
``process_chat`` — these tests pin the equivalent guard in the two
remaining FSM-state handlers (`process_promo_input`,
`process_custom_amount_input`) so a future refactor doesn't silently
re-introduce the regression.

We don't drive the aiogram dispatcher (needs a live Bot session +
Redis); we call the handler coroutines directly with a mocked
message/state and assert the early-return path takes effect.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.fixture
def mock_message_no_from_user():
    """A Message-shaped stub with ``from_user is None``.

    ``answer`` is an AsyncMock so we can also assert the handler
    didn't try to reply (the guard should be a silent no-op).
    """
    return SimpleNamespace(
        from_user=None,
        text="ANYTHING",
        answer=AsyncMock(),
    )


@pytest.fixture
def mock_state():
    """A minimal FSMContext stub. The handlers under test should never
    touch state when from_user is None — if they do, the AsyncMocks
    here will register the call and we can flag it."""
    return SimpleNamespace(
        get_data=AsyncMock(return_value={}),
        set_data=AsyncMock(),
        set_state=AsyncMock(),
        update_data=AsyncMock(),
        clear=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_process_promo_input_returns_silently_when_from_user_none(
    mock_message_no_from_user, mock_state
):
    """``process_promo_input`` must not crash when from_user is None.

    Pre-fix: ``await _get_user_language(message.from_user.id)`` raised
    ``AttributeError: 'NoneType' object has no attribute 'id'`` and
    the update bubbled up as a polling error.
    Post-fix: silent early return — no answer sent, no state touched.
    """
    from handlers import process_promo_input

    await process_promo_input(mock_message_no_from_user, mock_state)

    mock_message_no_from_user.answer.assert_not_called()
    mock_state.set_data.assert_not_called()
    mock_state.set_state.assert_not_called()


@pytest.mark.asyncio
async def test_process_custom_amount_input_returns_silently_when_from_user_none(
    mock_message_no_from_user, mock_state
):
    """``process_custom_amount_input`` must not crash when from_user is None.

    Same defensive guard as process_promo_input. Pre-fix: AttributeError
    on ``.id`` access. Post-fix: silent early return.
    """
    from handlers import process_custom_amount_input

    await process_custom_amount_input(mock_message_no_from_user, mock_state)

    mock_message_no_from_user.answer.assert_not_called()
    mock_state.set_state.assert_not_called()
    mock_state.update_data.assert_not_called()


@pytest.mark.asyncio
async def test_cmd_start_returns_silently_when_from_user_none(
    mock_message_no_from_user, mock_state
):
    """``cmd_start`` must not crash when from_user is None.

    /start is the most-reachable command from groups (anonymous group
    admins). Pre-fix: ``db.create_user(message.from_user.id, ...)``
    AttributeError'd on .id access. Post-fix: silent early return.
    The state.clear() call is allowed (it's a defensive cleanup
    that runs before the from_user check).
    """
    from handlers import cmd_start

    await cmd_start(mock_message_no_from_user, mock_state)

    mock_message_no_from_user.answer.assert_not_called()


@pytest.mark.asyncio
async def test_route_legacy_text_to_hub_returns_silently_when_from_user_none(
    mock_message_no_from_user, mock_state
):
    """``_route_legacy_text_to_hub`` must not crash when from_user is None.

    The legacy reply-keyboard buttons are matched by F.text equality,
    so an anonymous group admin posting one of the legacy labels
    would route here without ``from_user``.
    """
    from handlers import _route_legacy_text_to_hub

    await _route_legacy_text_to_hub(mock_message_no_from_user, mock_state)

    mock_message_no_from_user.answer.assert_not_called()
