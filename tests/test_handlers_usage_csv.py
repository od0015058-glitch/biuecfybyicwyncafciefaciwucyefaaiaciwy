"""Handler-wiring tests for the Stage-15-Step-E #2 follow-up
usage-log CSV export.

Mirrors ``tests/test_conversation_export.py``'s handler test
shape — the pure formatter is covered in
``tests/test_usage_csv_export.py``; the DB query in
``tests/test_database_queries.py``; this module covers the
callback-handler / slash-command wiring on top of those.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------
# helpers (mirrored from tests/test_conversation_export.py)
# ---------------------------------------------------------------------


def _make_callback(*, user_id: int = 12345, username: str | None = "tester"):
    msg = SimpleNamespace(
        edit_text=AsyncMock(),
        answer_document=AsyncMock(),
        chat=SimpleNamespace(id=user_id),
    )
    cb = SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, username=username),
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


def _make_message(
    *,
    user_id: int | None = 12345,
    username: str | None = "tester",
    text: str = "/usage_csv",
):
    return SimpleNamespace(
        from_user=(
            SimpleNamespace(id=user_id, username=username)
            if user_id is not None else None
        ),
        text=text,
        chat=SimpleNamespace(id=user_id or 0),
        answer=AsyncMock(),
        answer_document=AsyncMock(),
    )


def _row(
    *,
    log_id: int = 1,
    model: str = "openai/gpt-4o",
    prompt_tokens: int = 10,
    completion_tokens: int = 20,
    cost_usd: float = 0.0042,
) -> dict:
    """Mirror of ``Database.export_user_usage_logs`` per-row shape."""
    return {
        "id": log_id,
        "created_at": None,
        "model": model,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
        "cost_usd": cost_usd,
    }


# ---------------------------------------------------------------------
# usage_export_handler (callback path)
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_usage_export_handler_sends_csv_document_with_full_history():
    from handlers import usage_export_handler

    cb = _make_callback()
    state = _make_state()
    rows = [_row(log_id=1), _row(log_id=2)]
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.export_user_usage_logs",
        new=AsyncMock(return_value=rows),
    ):
        await usage_export_handler(cb, state)

    cb.message.answer_document.assert_awaited_once()
    sent_doc = cb.message.answer_document.await_args.args[0]
    body = sent_doc.data.decode("utf-8")
    assert body.startswith("\ufeff")  # UTF-8 BOM
    assert "id,created_at,model,prompt_tokens" in body
    assert sent_doc.filename.startswith("meowassist-usage-12345-")
    assert sent_doc.filename.endswith(".csv")
    cb.answer.assert_awaited_once()
    state.clear.assert_awaited()


@pytest.mark.asyncio
async def test_usage_export_handler_empty_buffer_shows_alert_no_document():
    """Empty case is a toast alert (button stays visible) rather
    than a silently-empty file. Same shape as
    :func:`memory_export_handler`."""
    from handlers import usage_export_handler

    cb = _make_callback()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.export_user_usage_logs",
        new=AsyncMock(return_value=[]),
    ):
        await usage_export_handler(cb, state)

    cb.message.answer_document.assert_not_called()
    cb.answer.assert_awaited_once()
    assert cb.answer.await_args.kwargs.get("show_alert") is True


@pytest.mark.asyncio
async def test_usage_export_handler_caption_uses_kept_count_after_trim(
    monkeypatch,
):
    """When the rendered CSV exceeds ``EXPORT_MAX_BYTES`` and
    ``format_usage_logs_as_csv`` trims the oldest rows, the
    caption + toast must report the *kept* count (what's actually
    in the file), not ``len(rows)`` (the untrimmed count).

    Same pre-fix shape as the conversation-export caption bug
    closed in Stage-15-Step-E #1: a heavy user whose buffer was
    trimmed under them would see a caption that lied about the
    file's contents."""
    from handlers import usage_export_handler

    # Force a tiny budget so we can trigger the trim path with a
    # small number of rows.
    monkeypatch.setattr(
        "usage_csv_export.EXPORT_MAX_BYTES", 500
    )
    cb = _make_callback()
    state = _make_state()
    rows = [_row(log_id=i) for i in range(100)]
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.export_user_usage_logs",
        new=AsyncMock(return_value=rows),
    ):
        await usage_export_handler(cb, state)

    sent_doc = cb.message.answer_document.await_args.args[0]
    body = sent_doc.data.decode("utf-8").lstrip("\ufeff")
    # Header + N data rows
    actual_data_rows = body.strip().count("\n")  # rows after header
    assert actual_data_rows < 100  # sanity: trim happened
    caption = cb.message.answer_document.await_args.kwargs["caption"]
    assert f"({actual_data_rows} rows)" in caption, (
        f"caption {caption!r} should report kept count "
        f"{actual_data_rows}, not 100"
    )
    assert "(100 rows)" not in caption
    toast = cb.answer.await_args.args[0]
    assert str(actual_data_rows) in toast


@pytest.mark.asyncio
async def test_usage_export_handler_filename_includes_telegram_id():
    """The filename embeds the user's telegram id so a user with
    multiple sessions saved doesn't have to rename them by hand."""
    from handlers import usage_export_handler

    cb = _make_callback(user_id=98765)
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.export_user_usage_logs",
        new=AsyncMock(return_value=[_row()]),
    ):
        await usage_export_handler(cb, state)

    sent_doc = cb.message.answer_document.await_args.args[0]
    assert "98765" in sent_doc.filename


# ---------------------------------------------------------------------
# cmd_usage_csv (slash-command path)
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_usage_csv_sends_csv_document_with_full_history():
    """``/usage_csv`` ships the same document as the stats-screen
    "Download CSV" button — same filename pattern, same caption
    count, same body."""
    from handlers import cmd_usage_csv

    msg = _make_message()
    state = _make_state()
    rows = [_row(log_id=1), _row(log_id=2)]
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.export_user_usage_logs",
        new=AsyncMock(return_value=rows),
    ), patch(
        "handlers.consume_chat_token", new=AsyncMock(return_value=True)
    ):
        await cmd_usage_csv(msg, state)

    msg.answer_document.assert_awaited_once()
    sent_doc = msg.answer_document.await_args.args[0]
    body = sent_doc.data.decode("utf-8").lstrip("\ufeff")
    assert "id,created_at,model" in body
    assert sent_doc.filename.startswith("meowassist-usage-12345-")
    state.clear.assert_awaited()


@pytest.mark.asyncio
async def test_cmd_usage_csv_empty_buffer_sends_chat_message_no_document():
    """The slash path can't show a callback toast; surfaces the
    empty case as a fresh chat message instead of silently doing
    nothing."""
    from handlers import cmd_usage_csv

    msg = _make_message()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.export_user_usage_logs",
        new=AsyncMock(return_value=[]),
    ), patch(
        "handlers.consume_chat_token", new=AsyncMock(return_value=True)
    ):
        await cmd_usage_csv(msg, state)

    msg.answer_document.assert_not_called()
    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    # The empty string from i18n includes "no usage" in EN.
    assert "usage" in sent.lower() or "no" in sent.lower()


@pytest.mark.asyncio
async def test_cmd_usage_csv_rate_limited_does_not_query_db():
    """When the chat-token bucket is empty, ``cmd_usage_csv`` must
    short-circuit *before* hitting
    :meth:`Database.export_user_usage_logs` — the whole point of
    the rate limit is to keep an abusive user from spamming a
    full table scan up to ``USAGE_LOGS_EXPORT_MAX_ROWS``."""
    from handlers import cmd_usage_csv

    msg = _make_message()
    state = _make_state()
    db_mock = AsyncMock(return_value=[])
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.export_user_usage_logs", new=db_mock
    ), patch(
        "handlers.consume_chat_token",
        new=AsyncMock(return_value=False),
    ):
        await cmd_usage_csv(msg, state)

    db_mock.assert_not_called()
    msg.answer_document.assert_not_called()
    # User gets the rate-limit notice on chat.
    msg.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_cmd_usage_csv_anonymous_user_returns_silently():
    """Aiogram's ``message.from_user`` is ``None`` for anonymous
    group admin / channel-bot edge cases. The handler must
    short-circuit rather than crash on attribute access."""
    from handlers import cmd_usage_csv

    msg = _make_message(user_id=None)
    state = _make_state()
    with patch(
        "handlers.db.export_user_usage_logs",
        new=AsyncMock(return_value=[]),
    ) as db_mock:
        await cmd_usage_csv(msg, state)

    db_mock.assert_not_called()
    msg.answer.assert_not_called()
    msg.answer_document.assert_not_called()


# ---------------------------------------------------------------------
# Stats keyboard wiring — the new "Download CSV" button is registered
# ---------------------------------------------------------------------


def test_stats_keyboard_includes_download_csv_button():
    """Defends against a future refactor accidentally dropping the
    button. The button row is its own row (`builder.adjust(4, 1, 2)`)
    above back/home so a user comparing the stats numbers to "the
    raw data" doesn't have to scroll."""
    from handlers import _build_stats_keyboard

    builder = _build_stats_keyboard("en", window_days=30)
    keyboard = builder.as_markup().inline_keyboard
    flat = [btn for row in keyboard for btn in row]
    csv_buttons = [
        b for b in flat if b.callback_data == "usage_export"
    ]
    assert len(csv_buttons) == 1, (
        f"expected exactly one CSV-export button, got "
        f"{[b.text for b in csv_buttons]!r}"
    )
    # Button text matches the slug.
    assert "CSV" in csv_buttons[0].text or "csv" in csv_buttons[0].text.lower()
