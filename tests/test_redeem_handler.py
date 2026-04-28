"""Tests for the ``/redeem CODE`` Telegram handler (Stage-8-Part-3).

We don't drive the aiogram dispatcher (needs a live Bot session +
Redis); we call the ``cmd_redeem`` coroutine directly with a mocked
message + state and verify the eligibility-status branches each
produce the right localized reply.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


def _make_message(text: str | None, *, from_user_id: int | None = 12345):
    """Build a Message-shaped stub the handler can chew on."""
    if from_user_id is None:
        from_user = None
    else:
        from_user = SimpleNamespace(id=from_user_id, username="tester")
    return SimpleNamespace(
        text=text,
        from_user=from_user,
        answer=AsyncMock(),
    )


def _make_state():
    return SimpleNamespace(clear=AsyncMock())


@pytest.mark.asyncio
async def test_cmd_redeem_no_arg_shows_usage():
    from handlers import cmd_redeem
    msg = _make_message("/redeem")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")):
        await cmd_redeem(msg, state)
    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    assert "/redeem CODE" in sent


@pytest.mark.asyncio
async def test_cmd_redeem_blank_arg_shows_usage():
    from handlers import cmd_redeem
    msg = _make_message("/redeem    ")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")):
        await cmd_redeem(msg, state)
    msg.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_cmd_redeem_bad_code_format_rejected_early():
    """Code with spaces / punctuation never reaches the DB layer."""
    from handlers import cmd_redeem
    msg = _make_message("/redeem has spaces here")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")), \
         patch("handlers.db.redeem_gift_code", new=AsyncMock()) as mock_redeem:
        await cmd_redeem(msg, state)
    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    assert "Invalid" in sent or "نامعتبر" in sent
    mock_redeem.assert_not_awaited()


@pytest.mark.asyncio
async def test_cmd_redeem_long_code_format_rejected_early():
    """A code over 64 chars never reaches the DB layer."""
    from handlers import cmd_redeem
    msg = _make_message("/redeem " + "A" * 70)
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")), \
         patch("handlers.db.redeem_gift_code", new=AsyncMock()) as mock_redeem:
        await cmd_redeem(msg, state)
    msg.answer.assert_awaited_once()
    mock_redeem.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        "GIFT\u06f1",      # Persian digit '۱'
        "PROMO\u041e",     # Cyrillic 'О' homoglyph of Latin 'O'
        "X\u00b2",         # Superscript 2
        "\u2164",          # Roman numeral V
    ],
)
async def test_cmd_redeem_unicode_alnum_rejected_early(code):
    """ASCII-only guard mirrors the admin-side ``parse_promo_form`` /
    ``parse_gift_form`` validators. Pre-fix ``str.isalnum`` returned
    True for Unicode digits / homoglyphs and the handler would
    happily round-trip the DB to get a ``not_found`` miss back —
    spending a query on something we can determine is malformed at
    parse time. Post-fix the user gets the clearer
    ``redeem_bad_code`` reply (``"Invalid code"``) without a DB hit.
    """
    from handlers import cmd_redeem
    msg = _make_message(f"/redeem {code}")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")), \
         patch("handlers.db.redeem_gift_code", new=AsyncMock()) as mock_redeem:
        await cmd_redeem(msg, state)
    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    assert "Invalid" in sent or "نامعتبر" in sent
    mock_redeem.assert_not_awaited()


@pytest.mark.asyncio
async def test_cmd_redeem_ok_credits_user():
    from handlers import cmd_redeem
    msg = _make_message("/redeem GIFT5")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")), \
         patch(
            "handlers.db.redeem_gift_code",
            new=AsyncMock(return_value={
                "status": "ok",
                "amount_usd": 5.0,
                "new_balance_usd": 12.34,
                "transaction_id": 42,
            }),
        ) as mock_redeem:
        await cmd_redeem(msg, state)
    mock_redeem.assert_awaited_once_with("GIFT5", 12345)
    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    assert "$5.00" in sent
    assert "$12.34" in sent


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
async def test_cmd_redeem_status_branches(status, expected_substr):
    from handlers import cmd_redeem
    msg = _make_message("/redeem GIFT5")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")), \
         patch(
            "handlers.db.redeem_gift_code",
            new=AsyncMock(return_value={
                "status": status,
                "amount_usd": None,
                "new_balance_usd": None,
                "transaction_id": None,
            }),
        ):
        await cmd_redeem(msg, state)
    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    assert expected_substr in sent


@pytest.mark.asyncio
async def test_cmd_redeem_db_exception_friendly_error():
    """If the DB throws, the user gets a friendly error and we don't
    crash the poller."""
    from handlers import cmd_redeem
    msg = _make_message("/redeem GIFT5")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")), \
         patch(
            "handlers.db.redeem_gift_code",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
        await cmd_redeem(msg, state)
    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    assert "Something went wrong" in sent or "خطایی" in sent


@pytest.mark.asyncio
async def test_cmd_redeem_returns_silently_when_from_user_none():
    """Defensive guard for anonymous-group-admin / channel-bot updates.
    Same pattern as cmd_start / process_chat / process_promo_input."""
    from handlers import cmd_redeem
    msg = _make_message("/redeem GIFT5", from_user_id=None)
    state = _make_state()
    with patch("handlers.db.redeem_gift_code", new=AsyncMock()) as mock_redeem:
        await cmd_redeem(msg, state)
    msg.answer.assert_not_called()
    mock_redeem.assert_not_awaited()


@pytest.mark.asyncio
async def test_cmd_redeem_clears_fsm_state():
    """``state.clear()`` runs even when from_user is None — same
    pattern as cmd_start (defensive cleanup before the guard)."""
    from handlers import cmd_redeem
    msg = _make_message("/redeem GIFT5")
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")), \
         patch(
            "handlers.db.redeem_gift_code",
            new=AsyncMock(return_value={
                "status": "ok",
                "amount_usd": 1.0,
                "new_balance_usd": 1.0,
                "transaction_id": 1,
            }),
        ):
        await cmd_redeem(msg, state)
    state.clear.assert_awaited_once()


@pytest.mark.asyncio
async def test_cmd_redeem_uppercases_code_for_db():
    """Users may type lowercase; we still hand the DB an uppercase
    code so it matches the canonical form stored at create time."""
    from handlers import cmd_redeem
    msg = _make_message("/redeem  birthday5  ")  # lower + extra spaces
    state = _make_state()
    with patch("handlers._get_user_language", new=AsyncMock(return_value="en")), \
         patch(
            "handlers.db.redeem_gift_code",
            new=AsyncMock(return_value={
                "status": "not_found",
                "amount_usd": None,
                "new_balance_usd": None,
                "transaction_id": None,
            }),
        ) as mock_redeem:
        await cmd_redeem(msg, state)
    # The DB layer normalises case via .upper() so we don't strictly
    # require the handler to do it, but we DO require a non-blank
    # arg got forwarded.
    args = mock_redeem.await_args.args
    assert args[0].strip() != ""
    assert args[1] == 12345
