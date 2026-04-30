"""Tests for ``user_stats`` (Stage-15-Step-E #2, first slice)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from user_stats import DIGEST_TOP_MODELS, format_user_stats


# ---------------------------------------------------------------------
# format_user_stats
# ---------------------------------------------------------------------


def _summary(**overrides) -> dict:
    base = {
        "total_calls": 12,
        "total_prompt_tokens": 4500,
        "total_completion_tokens": 8200,
        "total_spent_usd": 0.123456,
        "calls_last_7d": 5,
        "spent_last_7d_usd": 0.05,
        "calls_last_30d": 12,
        "spent_last_30d_usd": 0.123456,
        "top_models": [
            {"model": "openai/gpt-4o", "count": 7, "cost_usd": 0.08},
            {"model": "anthropic/claude-3-5-sonnet", "count": 5, "cost_usd": 0.043},
        ],
        "first_call_at": "2026-01-01T00:00:00+00:00",
        "last_call_at": "2026-04-30T17:00:00+00:00",
    }
    base.update(overrides)
    return base


def test_format_user_stats_renders_full_digest():
    out = format_user_stats(_summary(), lang="en")
    assert "Your usage stats" in out
    assert "Total messages" in out
    assert "12" in out
    assert "$0.1235" in out  # 4-decimal cost
    # Token line.
    assert "4,500" in out
    assert "8,200" in out
    # 7d / 30d windows.
    assert "7d" in out
    assert "30d" in out
    # Top-models block.
    assert "Top models" in out
    assert "openai/gpt-4o" in out
    assert "anthropic/claude-3-5-sonnet" in out


def test_format_user_stats_empty_buffer_renders_empty_state():
    """Zero usage rows → friendly empty-state message, not a wall
    of zero-padded fields."""
    out = format_user_stats(
        _summary(
            total_calls=0,
            total_prompt_tokens=0,
            total_completion_tokens=0,
            total_spent_usd=0.0,
            calls_last_7d=0,
            spent_last_7d_usd=0.0,
            calls_last_30d=0,
            spent_last_30d_usd=0.0,
            top_models=[],
            first_call_at=None,
            last_call_at=None,
        ),
        lang="en",
    )
    assert "No AI calls yet" in out
    # Must NOT include the digest scaffolding.
    assert "Top models" not in out


def test_format_user_stats_persian_locale():
    out = format_user_stats(_summary(), lang="fa")
    # Persian header / labels.
    assert "آمار" in out
    # Numbers still render in ASCII (the formatter uses {:,} which
    # is locale-free; this is intentional to keep the output Telegram-
    # paste-safe).
    assert "12" in out


def test_format_user_stats_missing_keys_treated_as_zero():
    """Defensive: a partial dict (e.g. an upgrade-in-flight where a
    new field was added but the old DB query path was forgotten)
    must not crash the formatter."""
    minimal = {"total_calls": 3}
    out = format_user_stats(minimal, lang="en")
    # 3 calls renders the digest, not the empty state.
    assert "Total messages" in out
    assert "3" in out
    # Missing fields render as 0 / empty.
    assert "0,000" not in out  # no malformed comma-separated zeros
    assert "Top models" not in out  # empty top_models list → header skipped


def test_format_user_stats_nan_cost_renders_as_zero():
    """A corrupted ``total_spent_usd=NaN`` row must not leak ``$nan``
    into the output (same NaN-defence pattern as wallet_display)."""
    out = format_user_stats(
        _summary(total_spent_usd=float("nan")), lang="en"
    )
    assert "nan" not in out.lower()
    assert "$0.0000" in out


def test_format_user_stats_caps_top_models_at_digest_constant():
    """If the DB returns more than DIGEST_TOP_MODELS rows the
    formatter still renders no more than the constant — defensive
    cap below the DB-layer's own ``top_n_models`` knob."""
    many = [
        {"model": f"m{i}", "count": 100 - i, "cost_usd": 0.01}
        for i in range(10)
    ]
    out = format_user_stats(_summary(top_models=many), lang="en")
    # Count rendered model lines (each starts with "  N. `…`").
    rendered = [line for line in out.splitlines() if "`m" in line]
    assert len(rendered) <= DIGEST_TOP_MODELS


def test_format_user_stats_unknown_model_renders_placeholder():
    """A row with a missing / None ``model`` field must render
    a stable placeholder rather than ``None``."""
    out = format_user_stats(
        _summary(top_models=[{"model": None, "count": 3, "cost_usd": 0.01}]),
        lang="en",
    )
    assert "(unknown)" in out
    assert "None" not in out


# ---------------------------------------------------------------------
# Handler-level tests
# ---------------------------------------------------------------------


def _make_message(*, user_id: int = 12345):
    msg = SimpleNamespace(
        text="/stats",
        from_user=SimpleNamespace(id=user_id, username="tester"),
        answer=AsyncMock(),
        chat=SimpleNamespace(id=user_id),
    )
    return msg


def _make_callback(*, user_id: int = 12345):
    msg = SimpleNamespace(
        edit_text=AsyncMock(),
        chat=SimpleNamespace(id=user_id),
    )
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, username="tester"),
        message=msg,
        answer=AsyncMock(),
        data="hub_stats",
    )


def _make_state():
    return SimpleNamespace(
        clear=AsyncMock(),
        set_state=AsyncMock(),
        set_data=AsyncMock(),
        get_data=AsyncMock(return_value={}),
    )


@pytest.mark.asyncio
async def test_cmd_stats_renders_digest():
    from handlers import cmd_stats

    msg = _make_message()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary",
        new=AsyncMock(return_value=_summary()),
    ):
        await cmd_stats(msg, state)

    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    assert "Your usage stats" in sent
    assert "openai/gpt-4o" in sent


@pytest.mark.asyncio
async def test_cmd_stats_handles_missing_from_user():
    """A channel-post or anonymous message has from_user=None;
    the handler must short-circuit, not crash on attribute access."""
    from handlers import cmd_stats

    msg = SimpleNamespace(
        text="/stats", from_user=None, answer=AsyncMock(),
    )
    state = _make_state()
    await cmd_stats(msg, state)
    msg.answer.assert_not_called()


@pytest.mark.asyncio
async def test_hub_stats_handler_renders_via_edit_text():
    from handlers import hub_stats_handler

    cb = _make_callback()
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary",
        new=AsyncMock(return_value=_summary()),
    ):
        await hub_stats_handler(cb, state)

    cb.message.edit_text.assert_awaited_once()
    sent = cb.message.edit_text.await_args.args[0]
    assert "Your usage stats" in sent
    cb.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_hub_stats_handler_swallows_message_not_modified():
    """Double-tap: the user fires ``hub_stats`` twice and the second
    edit raises TelegramBadRequest('message is not modified'). The
    handler must swallow it (toast was already shown) and not bubble
    the exception up to the dispatcher's error log."""
    from aiogram.exceptions import TelegramBadRequest

    from handlers import hub_stats_handler

    cb = _make_callback()
    cb.message.edit_text.side_effect = TelegramBadRequest(
        method=SimpleNamespace(__name__="editMessageText"),
        message="message is not modified",
    )
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="en")
    ), patch(
        "handlers.db.get_user_spending_summary",
        new=AsyncMock(return_value=_summary()),
    ):
        # No exception bubbles up.
        await hub_stats_handler(cb, state)
    cb.answer.assert_awaited_once()


# ---------------------------------------------------------------------
# Wallet keyboard exposes the stats button
# ---------------------------------------------------------------------


def test_wallet_keyboard_has_stats_button():
    """The "📊 My usage stats" button must be on the wallet
    keyboard so the feature is discoverable without a slash
    command."""
    from handlers import _build_wallet_keyboard

    kb = _build_wallet_keyboard("en")
    flat = [b for row in kb.as_markup().inline_keyboard for b in row]
    callbacks = [b.callback_data for b in flat]
    assert "hub_stats" in callbacks
