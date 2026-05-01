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
    out, kept = format_history_as_text(
        [_row("user", "hello", ts), _row("assistant", "hi there", ts)],
        user_handle="alice",
    )
    assert "Conversation history for @alice" in out
    assert "Messages: 2" in out
    assert "[2026-01-02 03:04:05 UTC] You:" in out
    assert "[2026-01-02 03:04:05 UTC] Assistant:" in out
    assert "hello" in out
    assert "hi there" in out
    assert kept == 2


def test_format_history_handles_missing_username():
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    out, kept = format_history_as_text(
        [_row("user", "hi", ts)], user_handle=None
    )
    # No "for @None" or stray @ in the header.
    assert "for @" not in out
    assert "Conversation history" in out
    assert kept == 1


def test_format_history_naive_timestamp_treated_as_utc():
    """An asyncpg row with a naive ``created_at`` (which shouldn't
    happen in practice — the column is TIMESTAMPTZ — but defensive)
    must NOT silently render in local time. We force UTC so the
    file is reproducible across deploys."""
    ts = datetime(2026, 1, 2, 3, 4, 5)  # naive
    out, _ = format_history_as_text([_row("user", "hi", ts)])
    assert "[2026-01-02 03:04:05 UTC]" in out


def test_format_history_unknown_role_falls_back_to_capitalised_label():
    """If the schema ever grows a new role (e.g. ``system``), the
    formatter must not crash — render the role with a capitalised
    label so the file is still readable."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    out, _ = format_history_as_text(
        [_row("system", "boot", ts)]
    )
    assert "System:" in out


def test_format_history_missing_timestamp_renders_placeholder():
    out, _ = format_history_as_text(
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
    out, kept = format_history_as_text(rows)
    assert len(out.encode("utf-8")) <= EXPORT_MAX_BYTES
    assert "trimmed" in out
    # Header must reflect the *kept* count, not the original count.
    # 20 input messages, ~10 fit under 1MB, so the header should
    # read "Messages: <kept> (trimmed <dropped> oldest)" with kept
    # < 20 and kept + dropped == 20.
    import re
    m = re.search(r"Messages: (\d+) \(trimmed (\d+) oldest\)", out)
    assert m is not None, f"missing trim header: {out[:500]!r}"
    kept_in_header, dropped_in_header = int(m.group(1)), int(m.group(2))
    assert kept_in_header + dropped_in_header == 20
    assert kept_in_header >= 1 and dropped_in_header >= 1
    # Bundled bug fix in this PR: the second element of the tuple
    # must be the *kept* count (matches the header), not the
    # original input count. Pre-fix the handler wrote
    # ``count=len(rows)`` to the caption while the .txt itself
    # contained ``kept`` messages — caption lied to the user.
    assert kept == kept_in_header
    assert kept < 20


def test_format_history_returns_kept_count_equal_to_input_when_no_trim():
    """When the rendered text fits under ``EXPORT_MAX_BYTES`` no trim
    happens and ``kept`` equals the input row count.

    This is the common case (the running window is capped at 30
    messages × 8000 chars ≈ 240 KB, well below the 1 MB limit) so
    the new return shape must not regress it."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    rows = [_row("user", "small", ts) for _ in range(5)]
    _, kept = format_history_as_text(rows)
    assert kept == 5


def test_format_history_empty_rows_renders_empty_body():
    """No history → header still renders so the user knows what
    the file is, but the body is empty."""
    out, kept = format_history_as_text([])
    assert "Conversation history" in out
    assert "Messages: 0" in out
    assert kept == 0


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
async def test_memory_export_handler_caption_uses_kept_count_after_trim():
    """Bundled bug fix in this PR (Stage-15-Step-E #2): when the
    rendered .txt would exceed ``EXPORT_MAX_BYTES`` and
    ``format_history_as_text`` trims the oldest messages, the
    caption + toast must report the *kept* count (what's actually
    in the file), not ``len(rows)`` (the untrimmed input count).

    Pre-fix a heavy user with 20 large turns would see
    "Conversation history (20 messages)" in the caption while the
    .txt only contained ~10 of the most recent messages — the
    in-file header said the truth ("Messages: 10 (trimmed 10
    oldest)") but the caption lied."""
    from handlers import memory_export_handler

    cb = _make_callback()
    state = _make_state()
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    big = "x" * 100_000  # 100KB per message
    rows = [
        {"role": "user", "content": big, "created_at": ts}
        for _ in range(20)  # 2 MB total — well over the 1 MB cap
    ]
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_full_conversation",
        new=AsyncMock(return_value=rows),
    ):
        await memory_export_handler(cb, state)

    # The caption / toast count must equal the kept count (parsed
    # out of the in-file header), not the original 20.
    sent_doc = cb.message.answer_document.await_args.args[0]
    body = sent_doc.data.decode("utf-8")
    import re
    m = re.search(r"Messages: (\d+) \(trimmed (\d+) oldest\)", body)
    assert m is not None, f"missing trim header: {body[:500]!r}"
    kept = int(m.group(1))
    assert kept < 20  # sanity: the trim actually happened
    caption = cb.message.answer_document.await_args.kwargs["caption"]
    assert f"({kept} messages)" in caption, (
        f"caption {caption!r} should report kept count {kept}, not 20"
    )
    assert "(20 messages)" not in caption
    toast = cb.answer.await_args.args[0]
    assert str(kept) in toast
    assert "20" not in toast.split()


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


# ---------------------------------------------------------------------
# Stage-15-Step-E #1 follow-up: /history slash-command alias
# ---------------------------------------------------------------------


def _make_message(*, user_id: int = 12345, username: str | None = "tester",
                  text: str = "/history"):
    """Mock ``Message`` for the slash-command handler tests."""
    return SimpleNamespace(
        from_user=(
            SimpleNamespace(id=user_id, username=username)
            if user_id is not None else None
        ),
        text=text,
        chat=SimpleNamespace(id=user_id),
        answer=AsyncMock(),
        answer_document=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_cmd_history_sends_document_with_full_history():
    """``/history`` ships the same document as the wallet-menu's
    "Export history" button — same filename pattern, same body
    (modulo timestamp drift), same caption count."""
    from handlers import cmd_history

    msg = _make_message()
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
    ), patch(
        "handlers.consume_chat_token", new=AsyncMock(return_value=True)
    ):
        await cmd_history(msg, state)

    msg.answer_document.assert_awaited_once()
    sent_doc = msg.answer_document.await_args.args[0]
    body = sent_doc.data.decode("utf-8")
    assert "hello" in body
    assert "hi back" in body
    assert sent_doc.filename.startswith("meowassist-history-12345-")
    state.clear.assert_awaited()


@pytest.mark.asyncio
async def test_cmd_history_empty_buffer_sends_chat_message_no_document():
    """The slash-command path can't show a callback toast; it
    must surface the empty-buffer case as a fresh chat message
    instead of silently doing nothing."""
    from handlers import cmd_history

    msg = _make_message()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_full_conversation",
        new=AsyncMock(return_value=[]),
    ), patch(
        "handlers.consume_chat_token", new=AsyncMock(return_value=True)
    ):
        await cmd_history(msg, state)

    msg.answer_document.assert_not_called()
    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    # The empty-buffer string from i18n includes "No history" in EN.
    assert "history" in sent.lower() or "no" in sent.lower()


@pytest.mark.asyncio
async def test_cmd_history_rate_limited_does_not_query_db():
    """When the chat-token bucket is empty, ``cmd_history`` must
    short-circuit *before* hitting ``Database.get_full_conversation``
    — the whole point of the rate limit is to keep an abusive
    user from spamming an unbounded table scan."""
    from handlers import cmd_history

    msg = _make_message()
    state = _make_state()
    db_mock = AsyncMock()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_full_conversation", new=db_mock,
    ), patch(
        "handlers.consume_chat_token", new=AsyncMock(return_value=False)
    ):
        await cmd_history(msg, state)

    db_mock.assert_not_called()
    msg.answer_document.assert_not_called()
    # The "slow down" flash must land as a chat bubble.
    msg.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_cmd_history_skips_when_no_from_user():
    """Anonymous group admin / channel-bot edge case — same
    defensive guard as ``cmd_start`` / ``cmd_redeem`` /
    ``cmd_stats``. Must early-return without touching the DB or
    rate limiter."""
    from handlers import cmd_history

    msg = _make_message()
    msg.from_user = None
    state = _make_state()
    db_mock = AsyncMock()
    rl_mock = AsyncMock(return_value=True)
    with patch(
        "handlers.db.get_full_conversation", new=db_mock,
    ), patch(
        "handlers.consume_chat_token", new=rl_mock,
    ):
        await cmd_history(msg, state)

    db_mock.assert_not_called()
    rl_mock.assert_not_called()
    msg.answer_document.assert_not_called()


@pytest.mark.asyncio
async def test_cmd_history_clears_fsm_state():
    """Same FSM-clear policy as ``cmd_start`` / ``cmd_stats`` —
    /history is a hard exit from any in-flight charge / promo /
    gift-code flow."""
    from handlers import cmd_history

    msg = _make_message()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_full_conversation",
        new=AsyncMock(return_value=[]),
    ), patch(
        "handlers.consume_chat_token", new=AsyncMock(return_value=True)
    ):
        await cmd_history(msg, state)
    state.clear.assert_awaited()


@pytest.mark.asyncio
async def test_cmd_history_caption_uses_kept_count_after_trim():
    """Same trim regression-pin as the wallet-menu callback path:
    the caption must report what's actually in the file, not the
    untrimmed input count."""
    from handlers import cmd_history

    msg = _make_message()
    state = _make_state()
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    big = "x" * 100_000
    rows = [
        {"role": "user", "content": big, "created_at": ts}
        for _ in range(20)  # ~2 MB → triggers trim
    ]
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_full_conversation",
        new=AsyncMock(return_value=rows),
    ), patch(
        "handlers.consume_chat_token", new=AsyncMock(return_value=True)
    ):
        await cmd_history(msg, state)

    sent_doc = msg.answer_document.await_args.args[0]
    body = sent_doc.data.decode("utf-8")
    import re
    m = re.search(r"Messages: (\d+) \(trimmed (\d+) oldest\)", body)
    assert m is not None
    kept = int(m.group(1))
    assert kept < 20
    caption = msg.answer_document.await_args.kwargs["caption"]
    assert f"({kept} messages)" in caption
    assert "(20 messages)" not in caption


@pytest.mark.asyncio
async def test_cmd_history_supports_at_bot_suffix():
    """Telegram's group-chat shape ``/history@MyBot`` parses
    identically to the bare slash."""
    from handlers import cmd_history

    msg = _make_message(text="/history@meowbot")
    state = _make_state()
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    rows = [{"role": "user", "content": "hi", "created_at": ts}]
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_full_conversation",
        new=AsyncMock(return_value=rows),
    ), patch(
        "handlers.consume_chat_token", new=AsyncMock(return_value=True)
    ):
        await cmd_history(msg, state)

    msg.answer_document.assert_awaited_once()


# ---------------------------------------------------------------------
# Bundled bug fix: O(n²) trim → O(n)
# ---------------------------------------------------------------------


def test_format_history_trim_is_linear_not_quadratic():
    """Stage-15-Step-E #1 follow-up bundled bug fix.

    Pre-fix, the trim loop in :func:`format_history_as_text` re-rendered
    + re-encoded the entire buffer on every iteration, which is O(n²)
    on the kept-rows count. A user with a 5 MB buffer triggering
    trim would burn ~12 MB of repeated UTF-8 encoding work per
    dropped message — for the ~4 MB they'd have to drop, that's
    ~50 MB of useless encoding before the document even gets sent.

    Post-fix, the loop pre-computes each message's encoded byte
    size once and runs a single forward pass. Verifying the time
    complexity directly is brittle in CI, so this test pins the
    *behaviour* contract: a buffer that needs heavy trimming
    produces (a) the same kept count it always did, (b) a body
    under EXPORT_MAX_BYTES, (c) the most recent messages survive.
    A separate timing-based smoke check verifies the trim runs in
    well under the pre-fix worst case for a synthetic 5 MB
    buffer.
    """
    import time
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    # 50 messages of 100 KB each = 5 MB total; the trim has to
    # drop roughly 40 of them to fit under the 1 MB cap.
    big = "y" * 100_000
    rows = [
        {"role": "user" if i % 2 == 0 else "assistant",
         "content": f"msg{i}-{big}", "created_at": ts}
        for i in range(50)
    ]
    t0 = time.perf_counter()
    out, kept = format_history_as_text(rows)
    elapsed = time.perf_counter() - t0
    # Behaviour contract — same as before.
    assert len(out.encode("utf-8")) <= EXPORT_MAX_BYTES
    assert "trimmed" in out
    assert kept >= 1 and kept < 50
    # The most recent messages must survive (we drop oldest first).
    assert "msg49" in out
    assert "msg0-" not in out
    # Generous upper bound — the pre-fix O(n²) trim on a 5 MB
    # buffer with ~40 drops took ~1.5 s on a CI box; the post-fix
    # O(n) trim runs in <100 ms. Set the assertion at 1 s so the
    # test is robust to slow CI but still catches a regression.
    assert elapsed < 1.0, (
        f"trim took {elapsed:.3f}s — likely O(n²) regression"
    )


def test_format_history_trim_drops_only_oldest_messages():
    """Belt-and-suspenders for the perf-fix: the rewritten trim
    loop must still drop strictly from the front (oldest first),
    so the most-recent context is preserved. Pre-fix this was
    enforced by ``rendered.pop(0)`` in the loop; post-fix the
    same invariant holds via the pre-computed ``encoded_sizes``
    list."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    big = "z" * 100_000
    rows = [
        {"role": "user", "content": f"mark{i}-{big}", "created_at": ts}
        for i in range(20)
    ]
    out, kept = format_history_as_text(rows)
    # Find the smallest surviving "mark<N>" — every "mark<M>"
    # for M < N must have been dropped, every M >= N kept.
    import re
    surviving = sorted(
        int(m.group(1)) for m in re.finditer(r"mark(\d+)-", out)
    )
    assert len(surviving) == kept
    # Strictly increasing run from some N to 19 — no gaps.
    assert surviving == list(range(surviving[0], surviving[0] + kept))
    assert surviving[-1] == 19  # the most recent message survives
