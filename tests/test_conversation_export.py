"""Tests for ``conversation_export`` (Stage-15-Step-E #1, first slice)."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from conversation_export import (
    EXPORT_MAX_BYTES,
    EXPORT_MAX_PARTS,
    EXPORT_PART_MAX_BYTES,
    EXPORT_TOTAL_MAX_BYTES,
    export_filename_for,
    export_filename_for_part,
    format_history_as_text,
    format_history_as_text_multipart,
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
async def test_memory_export_handler_paginates_oversize_buffer_into_multiple_parts():
    """Stage-15-Step-E #1 follow-up #2: a buffer that exceeds
    ``EXPORT_PART_MAX_BYTES`` (1 MB) but fits under the total
    ``EXPORT_TOTAL_MAX_BYTES`` budget (10 MB) gets split across
    multiple ``.txt`` documents instead of having the oldest
    messages trimmed away. Each part lands as its own
    ``answer_document`` call with a per-part caption.

    Regression-pin for the kept-count caption contract: the
    caption count for each part must match the messages actually
    inside that part (not ``len(rows)``, not the cross-part total).
    Pre-bug-fix shape — the legacy single-file path used to send
    one document with caption ``(20 messages)`` while the .txt
    body only held ~10 of them after trim. Post-fix shape — every
    message survives via pagination, and each part's caption
    matches its own body.
    """
    from handlers import memory_export_handler

    cb = _make_callback()
    state = _make_state()
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    big = "x" * 100_000  # 100KB per message
    rows = [
        {"role": "user", "content": big, "created_at": ts}
        for _ in range(20)  # 2 MB total — splits into 2+ parts
    ]
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_full_conversation",
        new=AsyncMock(return_value=rows),
    ):
        await memory_export_handler(cb, state)

    # Each call to ``answer_document`` is one part — must be at
    # least 2, and the cross-part body covers all 20 messages.
    calls = cb.message.answer_document.await_args_list
    assert len(calls) >= 2, f"expected pagination, got {len(calls)} call"
    total_kept_in_files = 0
    for call in calls:
        sent_doc = call.args[0]
        body = sent_doc.data.decode("utf-8")
        # No trim header on a paginated buffer that fits under the
        # total budget — every message survived via pagination.
        assert "trimmed" not in body, (
            f"pagination should NOT trim under total budget: {body[:300]!r}"
        )
        # The per-part caption count must match the per-part body.
        import re
        m = re.search(r"Messages: (\d+)", body)
        assert m is not None
        kept_in_part = int(m.group(1))
        caption = call.kwargs["caption"]
        assert f"({kept_in_part} messages)" in caption, (
            f"per-part caption {caption!r} must match body's {kept_in_part}"
        )
        total_kept_in_files += kept_in_part
    # Pagination is lossless under the total budget.
    assert total_kept_in_files == 20
    # Final toast announces the multi-part total.
    toast = cb.answer.await_args.args[0]
    assert "20" in toast and str(len(calls)) in toast


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
async def test_cmd_history_paginates_oversize_buffer_into_multiple_parts():
    """Slash-command mirror of
    :func:`test_memory_export_handler_paginates_oversize_buffer_into_multiple_parts`:
    the per-part caption must match each part's body (not
    ``len(rows)``, not the cross-part total). Pagination replaces
    the legacy oldest-first trim for buffers under the total
    budget so every message survives across the parts."""
    from handlers import cmd_history

    msg = _make_message()
    state = _make_state()
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    big = "x" * 100_000
    rows = [
        {"role": "user", "content": big, "created_at": ts}
        for _ in range(20)  # 2 MB → splits across multiple parts
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

    calls = msg.answer_document.await_args_list
    assert len(calls) >= 2
    total_kept_in_files = 0
    for call in calls:
        sent_doc = call.args[0]
        body = sent_doc.data.decode("utf-8")
        assert "trimmed" not in body
        import re
        m = re.search(r"Messages: (\d+)", body)
        assert m is not None
        kept_in_part = int(m.group(1))
        caption = call.kwargs["caption"]
        assert f"({kept_in_part} messages)" in caption
        total_kept_in_files += kept_in_part
    assert total_kept_in_files == 20


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


# ---------------------------------------------------------------------
# Stage-15-Step-E #1 follow-up #2 — multi-part export pagination
# ---------------------------------------------------------------------


def test_format_multipart_small_buffer_returns_single_part_legacy_shape():
    """A buffer that fits in one part returns a one-element list.

    The single-part body is byte-for-byte identical to the legacy
    :func:`format_history_as_text` output (same header, no
    ``Part:`` line) so callers that flip to the multipart entry
    point do not change the user-visible output for the common
    small-buffer case.
    """
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    rows = [
        {"role": "user", "content": "hi", "created_at": ts},
        {"role": "assistant", "content": "hello", "created_at": ts},
    ]
    parts = format_history_as_text_multipart(rows, user_handle="alice")
    assert len(parts) == 1
    text, kept = parts[0]
    assert kept == 2
    # Single-part output must NOT include the ``Part:`` line so
    # the legacy small-buffer shape is preserved.
    assert "Part:" not in text
    assert "Part 1/1" not in text
    # Compare against the legacy single-file render (modulo the
    # ``Exported:`` line, which embeds ``datetime.now``).
    legacy_text, legacy_kept = format_history_as_text(
        rows, user_handle="alice"
    )
    assert kept == legacy_kept
    # Drop the ``Exported:`` line from each side before comparing.
    def _strip_exported(s: str) -> str:
        return "\n".join(
            line for line in s.splitlines() if not line.startswith("Exported:")
        )
    assert _strip_exported(text) == _strip_exported(legacy_text)


def test_format_multipart_oversize_buffer_splits_into_multiple_parts():
    """A 2 MB buffer splits into 2+ parts under the 1 MB per-part
    cap. No trim happens because we're well under the 10 MB total
    budget — every message survives across the parts."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    big = "x" * 100_000  # 100 KB per message
    rows = [
        {"role": "user", "content": big, "created_at": ts}
        for _ in range(20)  # 2 MB total
    ]
    parts = format_history_as_text_multipart(rows)
    assert len(parts) >= 2
    total_kept = sum(kept for _, kept in parts)
    assert total_kept == 20  # lossless under total budget
    for text, _ in parts:
        # No trim header anywhere — pagination was lossless.
        assert "trimmed" not in text
        # Each part is under the per-part cap (we budgeted with the
        # worst-case header so the actual rendered size is always
        # under the cap, never over).
        assert len(text.encode("utf-8")) <= EXPORT_PART_MAX_BYTES


def test_format_multipart_part_header_shows_n_of_m():
    """Multi-part exports include a ``Part: N/M`` line in each
    part's header so the user can immediately tell which file
    they're looking at."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    big = "x" * 100_000
    rows = [
        {"role": "user", "content": big, "created_at": ts}
        for _ in range(20)
    ]
    parts = format_history_as_text_multipart(rows)
    total = len(parts)
    assert total >= 2
    for index, (text, _) in enumerate(parts, start=1):
        assert f"Part: {index}/{total}" in text


def test_format_multipart_oldest_first_packing_preserves_order():
    """Pagination must pack messages oldest-first so part 1 always
    contains the oldest survivors and part N contains the
    newest. The user reading the parts in order sees the same
    chronological flow as the running conversation."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    big = "y" * 100_000
    rows = [
        {"role": "user", "content": f"mark{i}-{big}", "created_at": ts}
        for i in range(20)
    ]
    parts = format_history_as_text_multipart(rows)
    assert len(parts) >= 2
    import re
    last_seen = -1
    for text, _ in parts:
        marks = sorted(
            int(m.group(1)) for m in re.finditer(r"mark(\d+)-", text)
        )
        assert marks == list(range(marks[0], marks[0] + len(marks)))
        assert marks[0] > last_seen
        last_seen = marks[-1]
    # Together the parts cover every original message.
    assert last_seen == 19


def test_format_multipart_over_total_budget_trims_oldest_first():
    """A buffer that would exceed ``EXPORT_TOTAL_MAX_BYTES``
    (10 MB) gets oldest messages trimmed first, then split. The
    trim header lands on part 1 only so the user sees the trim
    note exactly once when paging through the files."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    # 200 messages × 100 KB = 20 MB — double the total budget.
    big = "z" * 100_000
    rows = [
        {"role": "user", "content": f"m{i}-{big}", "created_at": ts}
        for i in range(200)
    ]
    parts = format_history_as_text_multipart(rows)
    assert 1 <= len(parts) <= EXPORT_MAX_PARTS
    total_kept = sum(kept for _, kept in parts)
    assert total_kept < 200  # trim happened
    # Cross-part total stays under the 10 MB budget.
    total_bytes = sum(len(text.encode("utf-8")) for text, _ in parts)
    assert total_bytes <= EXPORT_TOTAL_MAX_BYTES
    # ``trimmed N oldest`` shows on part 1 only — parts 2..N just
    # show their per-part message count.
    assert "trimmed" in parts[0][0]
    for text, _ in parts[1:]:
        assert "trimmed" not in text
    # Most recent messages survive (the trim is oldest-first).
    last_part_text = parts[-1][0]
    assert "m199-" in last_part_text
    # Earliest dropped messages are gone from every part.
    for text, _ in parts:
        assert "m0-" not in text


def test_format_multipart_empty_rows_returns_one_empty_part():
    """No history → still returns a one-element list with a
    placeholder header so the caller doesn't have to special-
    case ``len(parts) == 0`` everywhere."""
    parts = format_history_as_text_multipart([])
    assert len(parts) == 1
    text, kept = parts[0]
    assert kept == 0
    assert "Conversation history" in text
    assert "Messages: 0" in text
    # No ``Part:`` line for the single-part case.
    assert "Part:" not in text


def test_format_multipart_kept_counts_sum_equals_total_kept():
    """The sum of per-part kept counts equals the total number
    of messages in the export (after trim, if any)."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    big = "a" * 100_000
    rows = [
        {"role": "user", "content": big, "created_at": ts}
        for _ in range(15)
    ]
    parts = format_history_as_text_multipart(rows)
    total_kept = sum(kept for _, kept in parts)
    assert total_kept == 15  # under total budget — no trim


# ---------------------------------------------------------------------
# export_filename_for_part
# ---------------------------------------------------------------------


def test_export_filename_for_part_single_matches_legacy_shape():
    """A single-part export uses the legacy filename pattern so
    a user with both single- and multi-part exports in their
    downloads folder sees a consistent naming for the common
    case."""
    name = export_filename_for_part(12345, 1, 1)
    legacy = export_filename_for(12345)
    assert name == legacy


def test_export_filename_for_part_multi_includes_part_suffix():
    name = export_filename_for_part(12345, 2, 5)
    assert "part-2-of-5" in name
    assert name.endswith(".txt")
    assert name.startswith("meowassist-history-12345-")


def test_export_filename_for_part_pads_part_index_for_lex_sort():
    """Padding ``part_index`` to ``len(str(total_parts))`` digits
    means a lexicographic sort places ``part-02`` before
    ``part-10`` so most file managers (which sort by name) show
    the parts in the right order out of the box."""
    name_2 = export_filename_for_part(12345, 2, 12)
    name_10 = export_filename_for_part(12345, 10, 12)
    assert "part-02-of-12" in name_2
    assert "part-10-of-12" in name_10
    assert name_2 < name_10  # lexicographic sort matches numeric


def test_export_filename_for_part_rejects_invalid_args():
    with pytest.raises(ValueError):
        export_filename_for_part(12345, 0, 5)
    with pytest.raises(ValueError):
        export_filename_for_part(12345, 1, 0)
    with pytest.raises(ValueError):
        export_filename_for_part(12345, 6, 5)


# ---------------------------------------------------------------------
# Bundled bug fix (Stage-15-Step-E #1 follow-up #2):
#   ``_format_one_message`` previously rendered ``None`` content/role
#   as the literal four-character string ``"None"`` (Python's
#   ``str(None)`` is ``"None"``, not ``""``). Same shape on bytes
#   inputs (``str(b"x")`` is ``"b'x'"``) and arbitrary objects.
#   Fix: route both fields through a defensive coercion helper.
# ---------------------------------------------------------------------


def test_format_history_does_not_render_literal_none_for_null_content():
    """Pre-fix a row with ``content=None`` rendered as the literal
    string ``"None"`` in the export. Post-fix it renders as an
    empty body (the role/timestamp header still appears so the
    file shape is preserved). The DB column is ``TEXT NOT NULL``
    so this only triggers from a hand-built test fixture or a
    future schema change, but both paths used to leak Python's
    ``str(None)`` repr into the user's archive."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    rows = [{"role": "user", "content": None, "created_at": ts}]
    text, _ = format_history_as_text(rows)
    # Header line for the message still renders.
    assert "[2026-01-02 03:04:05 UTC] You:" in text
    # The literal "None" must NOT appear anywhere in the body.
    body = text.split("—" * 40, 1)[1] if "—" * 40 in text else text
    assert "None" not in body


def test_format_history_does_not_render_literal_none_for_null_role():
    """Same defensive shape as the content fix: a row with
    ``role=None`` used to render as ``None:`` in the file. Post-
    fix it falls through to the existing ``unknown`` placeholder."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    rows = [{"role": None, "content": "hi", "created_at": ts}]
    text, _ = format_history_as_text(rows)
    assert "None:" not in text
    # Falls through to the existing "unknown" placeholder.
    assert "Unknown:" in text


def test_format_history_does_not_leak_bytes_repr():
    """A row whose ``content`` is somehow ``bytes`` (a future
    binary-data column or a defensive coercion gone wrong) must
    not render Python's ``b'...'`` repr. The defensive coercion
    helper returns the empty string for non-str / non-numeric
    types so the file stays clean."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    rows = [{"role": "user", "content": b"hello", "created_at": ts}]
    text, _ = format_history_as_text(rows)
    assert "b'hello'" not in text
    assert 'b"hello"' not in text


def test_format_history_renders_numeric_content_as_string():
    """Numeric content is the one non-string type we *do* coerce
    via ``str(...)`` (a future ``role=42`` row reads as ``"42"``,
    matching the existing capitalised-fallback convention). This
    test pins that contract so a future change doesn't accidentally
    drop legitimate numeric content along with the ``None`` /
    bytes refusal."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    rows = [{"role": "user", "content": 12345, "created_at": ts}]
    text, _ = format_history_as_text(rows)
    assert "12345" in text


def test_format_history_refuses_bool_content_to_avoid_ambiguity():
    """``bool`` is a subclass of ``int`` in Python — a stray
    ``True`` / ``False`` in the content slot would otherwise
    render as the ambiguous ``"1"`` / ``"0"``. Coerce to empty
    string instead so the file doesn't lie."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    rows = [{"role": "user", "content": True, "created_at": ts}]
    text, _ = format_history_as_text(rows)
    # Slice off the header (everything up to and including the
    # row of em-dashes) so the assertions look only at the
    # rendered messages, not the ``Messages: 1`` summary line.
    body = text.split("—" * 40, 1)[1]
    assert "True" not in body
    # The body line for the (bool-content) row is
    # ``[<ts>] You:\n\n`` — after coercion to empty string the
    # ``True`` value never reaches the body, and the literal
    # token ``1`` (or ``0``) does not appear either.
    assert "True" not in body
    assert "False" not in body
    # No standalone "1" / "0" tokens in the body — the body
    # for a bool-content row is the role header followed by an
    # empty line.
    assert "\n1\n" not in body
    assert "\n0\n" not in body
