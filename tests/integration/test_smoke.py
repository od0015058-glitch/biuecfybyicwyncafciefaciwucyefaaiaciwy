"""Live-bot smoke tests (Stage-15-Step-E #6 — first slice).

These tests **do not run in CI by default**. The ``integration_secrets``
session-scoped fixture skips the entire suite if any of
``TG_API_ID`` / ``TG_API_HASH`` / ``TG_TEST_SESSION_STRING`` /
``TG_TEST_BOT_USERNAME`` is unset — see ``conftest.py`` for full setup.

To run locally::

    export TG_API_ID=...
    export TG_API_HASH=...
    export TG_TEST_SESSION_STRING=...
    export TG_TEST_BOT_USERNAME=mybot_test
    pytest tests/integration -v

Each test sends a single message to the bot and asserts on the reply.
The assertions look for *language-agnostic* tokens (currency symbols,
emoji, ``$``, the bot's username) rather than full Persian / English
strings so a small wording change in ``strings.py`` doesn't ripple
into the integration suite.
"""

from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_start_returns_greeting(send_and_wait):
    """``/start`` must elicit a reply within the timeout window.

    The check is intentionally minimal — confirms the bot is alive,
    long-polling, and able to reach Telegram. Wording is locale-
    dependent so we only assert non-empty body.
    """
    reply = await send_and_wait("/start")
    assert reply.text and len(reply.text.strip()) > 0, (
        f"/start returned an empty reply: {reply!r}"
    )


@pytest.mark.asyncio
async def test_start_hub_arrives_after_greeting(send_and_wait):
    """``/start`` posts a greeting *and* the hub menu.

    The hub is the second message and carries an inline keyboard.
    ``send_and_wait`` returns the *first* reply by default; we wait
    for the second by predicate-filtering on ``reply_markup``.
    """
    hub = await send_and_wait(
        "/start",
        predicate=lambda m: bool(getattr(m, "reply_markup", None)),
    )
    assert hub.reply_markup is not None, (
        f"/start hub message has no reply_markup: {hub!r}"
    )


@pytest.mark.asyncio
async def test_balance_command_returns_dollar_amount(send_and_wait):
    """``/balance`` (or whatever surfaces the wallet) must include
    a ``$`` symbol — the wallet template is ``${balance:.2f}``."""
    # Some operators wire ``/balance`` directly; others surface the
    # wallet via ``/start`` → "💰 Wallet" button. Trying the direct
    # command first lets the test fail fast if neither route works.
    reply = await send_and_wait("/balance")
    assert "$" in (reply.text or ""), (
        "/balance reply did not include a '$' "
        f"({reply.text!r})"
    )


@pytest.mark.asyncio
async def test_unknown_command_does_not_crash_bot(send_and_wait):
    """A garbage command must not crash the long-poller.

    We don't assert on the exact reply (the bot might silently
    ignore unknown commands) — only that the bot is *still
    responsive afterwards* by issuing a follow-up ``/start`` and
    expecting a reply. This is the only test in the suite that
    sends two commands; it doubles as a smoke-test of the
    poller's resilience.
    """
    # We allow any message back — including no message at all
    # within a short timeout. The real assertion is the followup.
    try:
        await send_and_wait("/this_is_not_a_real_command", timeout_seconds=3)
    except asyncio.TimeoutError:
        # Bot ignored it (no reply within 3s) — that's fine; carry on.
        pass

    follow_up = await send_and_wait("/start")
    assert follow_up.text, (
        "bot stopped responding after an unknown command — "
        "long-poller may be wedged"
    )
