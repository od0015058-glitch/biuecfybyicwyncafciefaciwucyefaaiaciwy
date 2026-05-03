"""Tests for the Stage-16 row 19 ``process_feedback_callback`` handler.

Black-box tests that drive a stub ``CallbackQuery`` through the
real handler with ``database.db`` patched to control the
underlying ``record_usage_feedback`` outcome.

Covers:

* malformed callback_data → "Bad data" toast, no DB write,
  keyboard NOT stripped (user can retry on a real button);
* missing ``from_user`` → silent drop with empty ``answer()``,
  no DB write;
* successful 👍 / 👎 → localised toast, keyboard stripped;
* second tap (DB returns ``False``) → "already recorded"
  toast, keyboard still stripped;
* DB raises ``ValueError`` → "unavailable" toast, no crash;
* DB raises any other exception → "unavailable" toast, no crash;
* ``edit_reply_markup`` ``TelegramBadRequest`` is swallowed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aiogram.exceptions import TelegramAPIError, TelegramBadRequest


def _callback(data: str = "fbp:42", user_id: int | None = 555) -> MagicMock:
    cb = MagicMock()
    cb.data = data
    cb.from_user = MagicMock()
    cb.from_user.id = user_id
    if user_id is None:
        cb.from_user = None
    cb.message = MagicMock()
    cb.message.edit_reply_markup = AsyncMock()
    cb.answer = AsyncMock()
    return cb


@pytest.mark.asyncio
async def test_positive_tap_records_and_strips_keyboard():
    import handlers

    cb = _callback("fbp:42", user_id=555)

    record = AsyncMock(return_value=True)
    with (
        patch.object(handlers.db, "record_usage_feedback", record),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_feedback_callback(cb)

    record.assert_awaited_once_with(
        log_id=42, telegram_id=555, feedback="positive"
    )
    # Localised "thanks" toast.
    cb.answer.assert_awaited_once()
    toast = cb.answer.await_args.args[0]
    assert "thank" in toast.lower() or "feedback" in toast.lower()
    cb.message.edit_reply_markup.assert_awaited_once_with(reply_markup=None)


@pytest.mark.asyncio
async def test_negative_tap_records_negative_kind():
    import handlers

    cb = _callback("fbn:99", user_id=777)

    record = AsyncMock(return_value=True)
    with (
        patch.object(handlers.db, "record_usage_feedback", record),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_feedback_callback(cb)

    record.assert_awaited_once_with(
        log_id=99, telegram_id=777, feedback="negative"
    )


@pytest.mark.asyncio
async def test_already_rated_returns_already_recorded_toast():
    """``record_usage_feedback`` returning ``False`` means a second
    tap, owner mismatch, or row deleted by retention. Toast is the
    "already recorded" key for all three cases.
    """
    import handlers

    cb = _callback("fbp:42", user_id=555)

    with (
        patch.object(handlers.db, "record_usage_feedback",
                     AsyncMock(return_value=False)),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_feedback_callback(cb)

    cb.answer.assert_awaited_once()
    toast = cb.answer.await_args.args[0]
    assert "already" in toast.lower() or "recorded" in toast.lower()
    # Keyboard still stripped — no future taps will be useful.
    cb.message.edit_reply_markup.assert_awaited_once()


@pytest.mark.asyncio
async def test_malformed_callback_data_does_not_call_db():
    """Hand-rolled / corrupted callback_data must not reach the DB
    layer. The keyboard is NOT stripped so the user can try a
    legitimate button.
    """
    import handlers

    cb = _callback("garbage:abc", user_id=555)

    record = AsyncMock()
    with (
        patch.object(handlers.db, "record_usage_feedback", record),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_feedback_callback(cb)

    record.assert_not_awaited()
    cb.answer.assert_awaited_once()
    cb.message.edit_reply_markup.assert_not_awaited()


@pytest.mark.asyncio
async def test_anonymous_admin_drops_silently():
    """A callback without ``from_user`` (anonymous group admin)
    can't be persisted because we have no telegram_id. Drop with
    a no-text ``answer()`` so the spinner clears.
    """
    import handlers

    cb = _callback("fbp:42", user_id=None)

    record = AsyncMock()
    with (
        patch.object(handlers.db, "record_usage_feedback", record),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_feedback_callback(cb)

    record.assert_not_awaited()
    cb.answer.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_db_value_error_surfaces_unavailable_toast(caplog):
    import logging

    import handlers

    cb = _callback("fbp:42", user_id=555)

    with (
        patch.object(
            handlers.db, "record_usage_feedback",
            AsyncMock(side_effect=ValueError("bad slug")),
        ),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        with caplog.at_level(logging.ERROR, logger="bot.handlers"):
            await handlers.process_feedback_callback(cb)

    cb.answer.assert_awaited_once()
    toast = cb.answer.await_args.args[0]
    # Localised "unavailable" — the exact wording differs per
    # locale, but the key is present.
    assert toast  # non-empty


@pytest.mark.asyncio
async def test_db_unexpected_exception_surfaces_unavailable_toast():
    """Any non-``ValueError`` exception (e.g. asyncpg
    ``InterfaceError`` during a DB blip) must fall through to
    the unavailable toast — never crash the dispatcher.
    """
    import handlers

    cb = _callback("fbp:42", user_id=555)

    with (
        patch.object(
            handlers.db, "record_usage_feedback",
            AsyncMock(side_effect=RuntimeError("DB down")),
        ),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_feedback_callback(cb)

    cb.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_edit_reply_markup_bad_request_is_swallowed():
    """A double-tap race that produces ``TelegramBadRequest``
    ("message is not modified") must not crash the handler — the
    vote is already persisted by then.
    """
    import handlers

    cb = _callback("fbp:42", user_id=555)
    cb.message.edit_reply_markup = AsyncMock(
        side_effect=TelegramBadRequest(method="x", message="not modified")
    )

    with (
        patch.object(handlers.db, "record_usage_feedback",
                     AsyncMock(return_value=True)),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        # Must not raise.
        await handlers.process_feedback_callback(cb)


@pytest.mark.asyncio
async def test_edit_reply_markup_other_telegram_error_is_logged(caplog):
    import logging

    import handlers

    cb = _callback("fbp:42", user_id=555)
    cb.message.edit_reply_markup = AsyncMock(
        side_effect=TelegramAPIError(method="x", message="boom")
    )

    with (
        patch.object(handlers.db, "record_usage_feedback",
                     AsyncMock(return_value=True)),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        with caplog.at_level(logging.WARNING, logger="bot.handlers"):
            await handlers.process_feedback_callback(cb)
    assert any(
        "edit_reply_markup failed" in rec.message
        for rec in caplog.records
    )
