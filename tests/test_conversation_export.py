"""Tests for ``conversation_export`` (Stage-15-Step-E #1, first slice)."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from conversation_export import (
    EXPORT_MAX_BYTES,
    export_filename_for,
    format_history_as_text,
)


# ---------------------------------------------------------------------
# format_history_as_text
# ---------------------------------------------------------------------


def _row(role: str, content: str, ts: datetime) -> dict:
    return {"role": role, "content": content, "created_at": ts}


def test_format_history_renders_role_label_timestamp_and_content():
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    out = format_history_as_text(
        [_row("user", "hello", ts), _row("assistant", "hi there", ts)],
        user_handle="alice",
    )
    assert "Conversation history for @alice" in out
    assert "Messages: 2" in out
    assert "[2026-01-02 03:04:05 UTC] You:" in out
    assert "[2026-01-02 03:04:05 UTC] Assistant:" in out
    assert "hello" in out
    assert "hi there" in out


def test_format_history_handles_missing_username():
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    out = format_history_as_text(
        [_row("user", "hi", ts)], user_handle=None
    )
    # No "for @None" or stray @ in the header.
    assert "for @" not in out
    assert "Conversation history" in out


def test_format_history_naive_timestamp_treated_as_utc():
    """An asyncpg row with a naive ``created_at`` (which shouldn't
    happen in practice — the column is TIMESTAMPTZ — but defensive)
    must NOT silently render in local time. We force UTC so the
    file is reproducible across deploys."""
    ts = datetime(2026, 1, 2, 3, 4, 5)  # naive
    out = format_history_as_text([_row("user", "hi", ts)])
    assert "[2026-01-02 03:04:05 UTC]" in out


def test_format_history_unknown_role_falls_back_to_capitalised_label():
    """If the schema ever grows a new role (e.g. ``system``), the
    formatter must not crash — render the role with a capitalised
    label so the file is still readable."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    out = format_history_as_text(
        [_row("system", "boot", ts)]
    )
    assert "System:" in out


def test_format_history_missing_timestamp_renders_placeholder():
    out = format_history_as_text(
        [{"role": "user", "content": "x", "created_at": None}]
    )
    assert "(unknown time)" in out
    assert "x" in out


def test_format_history_truncates_oldest_first_on_oversize():
    """A buffer that would render past EXPORT_MAX_BYTES gets the
    *oldest* messages dropped first so the most recent context
    survives. The header carries a note explaining the trim."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    big = "x" * 100_000  # 100KB per message
    rows = [_row("user", big, ts) for _ in range(20)]  # 2MB total
    out = format_history_as_text(rows)
    assert len(out.encode("utf-8")) <= EXPORT_MAX_BYTES
    assert "trimmed" in out
    # Header must reflect the *kept* count, not the original count.
    # 20 input messages, ~10 fit under 1MB, so the header should
    # read "Messages: <kept> (trimmed <dropped> oldest)" with kept
    # < 20 and kept + dropped == 20.
    import re
    m = re.search(r"Messages: (\d+) \(trimmed (\d+) oldest\)", out)
    assert m is not None, f"missing trim header: {out[:500]!r}"
    kept, dropped = int(m.group(1)), int(m.group(2))
    assert kept + dropped == 20
    assert kept >= 1 and dropped >= 1


def test_format_history_empty_rows_renders_empty_body():
    """No history → header still renders so the user knows what
    the file is, but the body is empty."""
    out = format_history_as_text([])
    assert "Conversation history" in out
    assert "Messages: 0" in out


# ---------------------------------------------------------------------
# export_filename_for
# ---------------------------------------------------------------------


def test_export_filename_format():
    name = export_filename_for(12345)
    assert name.startswith("meowassist-history-12345-")
    assert name.endswith(".txt")
    # YYYY-MM-DD date shape
    assert len(name.split("-")) >= 5


# ---------------------------------------------------------------------
# memory_export_handler (handler-level test reusing test_hub_ux pattern)
# ---------------------------------------------------------------------


def _make_callback(*, user_id: int = 12345, username: str = "tester"):
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


@pytest.mark.asyncio
async def test_memory_export_handler_sends_document_with_full_history():
    from handlers import memory_export_handler

    cb = _make_callback()
    state = _make_state()
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    rows = [
        {"role": "user", "content": "hello", "created_at": ts},
        {"role": "assistant", "content": "hi back", "created_at": ts},
    ]
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_full_conversation",
        new=AsyncMock(return_value=rows),
    ):
        await memory_export_handler(cb, state)

    cb.message.answer_document.assert_awaited_once()
    document, _kwargs = cb.message.answer_document.call_args.args, cb.message.answer_document.call_args.kwargs
    sent_doc = cb.message.answer_document.await_args.args[0]
    body = sent_doc.data.decode("utf-8")
    assert "hello" in body
    assert "hi back" in body
    assert sent_doc.filename.startswith("meowassist-history-12345-")
    cb.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_memory_export_handler_empty_buffer_shows_alert_no_document():
    from handlers import memory_export_handler

    cb = _make_callback()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_full_conversation",
        new=AsyncMock(return_value=[]),
    ):
        await memory_export_handler(cb, state)

    cb.message.answer_document.assert_not_called()
    cb.answer.assert_awaited_once()
    # Show alert (not toast).
    assert cb.answer.await_args.kwargs.get("show_alert") is True


@pytest.mark.asyncio
async def test_memory_export_handler_omits_handle_when_username_is_none():
    """A user with no Telegram username must not crash the export
    (telegram allows anonymous users). The handler should pass
    ``user_handle=None`` and the formatter handles the rest."""
    from handlers import memory_export_handler

    cb = _make_callback(username=None)
    state = _make_state()
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    rows = [{"role": "user", "content": "hi", "created_at": ts}]
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_full_conversation",
        new=AsyncMock(return_value=rows),
    ):
        await memory_export_handler(cb, state)

    sent_doc = cb.message.answer_document.await_args.args[0]
    body = sent_doc.data.decode("utf-8")
    assert "for @" not in body


# ---------------------------------------------------------------------
# Memory screen renders the export button regardless of memory state
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_memory_screen_export_button_shown_when_memory_off():
    """A user who turned memory off may still want to export the
    history they accumulated when it was on. Button must be
    visible in both states."""
    from handlers import _render_memory_screen

    cb = _make_callback()
    with patch(
        "handlers.db.get_memory_enabled", new=AsyncMock(return_value=False)
    ):
        await _render_memory_screen(cb, "en")

    sent_kwargs = cb.message.edit_text.await_args.kwargs
    markup = sent_kwargs["reply_markup"]
    flat = [b for row in markup.inline_keyboard for b in row]
    callbacks = [b.callback_data for b in flat]
    assert "mem_export" in callbacks


@pytest.mark.asyncio
async def test_memory_screen_export_button_shown_when_memory_on():
    from handlers import _render_memory_screen

    cb = _make_callback()
    with patch(
        "handlers.db.get_memory_enabled", new=AsyncMock(return_value=True)
    ):
        await _render_memory_screen(cb, "en")

    sent_kwargs = cb.message.edit_text.await_args.kwargs
    markup = sent_kwargs["reply_markup"]
    flat = [b for row in markup.inline_keyboard for b in row]
    callbacks = [b.callback_data for b in flat]
    assert "mem_export" in callbacks
    assert "mem_toggle" in callbacks
    assert "mem_reset" in callbacks
