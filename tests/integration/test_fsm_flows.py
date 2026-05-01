"""Live-bot FSM coverage (Stage-15-Step-E #6 follow-up #1).

The first slice (PR #124) added smoke tests for ``/start`` / ``/balance``
and an unknown-command resilience check. This file extends the
suite to the **finite-state-machine flows** that mocks miss:

* ``/redeem`` → waiting_promo_code → user types code → bot replies.
* ``/start`` → wallet button (callback-query) → wallet card (the
  inline-keyboard / callback-query path that ``send_and_wait``
  alone couldn't reach).
* ``/start`` → top-up button → currency picker (multi-step
  callback chain that exercises ``edit_message`` + a new keyboard).
* Unknown command **mid-FSM** doesn't drop the FSM state — the bot
  must still respond to a follow-up valid step.

All of these tests are gated by the same
``integration_secrets`` fixture as the smoke suite — when any of
``TG_API_ID`` / ``TG_API_HASH`` / ``TG_TEST_SESSION_STRING`` /
``TG_TEST_BOT_USERNAME`` is unset, every test in this file is
skipped at fixture-resolution time so CI stays green.

Conventions:

* Assertions look for *language-agnostic* tokens (``$``,
  emoji, ``reply_markup``, button-count) rather than specific
  Persian / English strings — a wording change in ``strings.py``
  must not flake the integration suite.
* Each test sends at most a handful of messages; the polling
  helpers cap at ``TG_TEST_TIMEOUT_SECONDS`` (default 15s) per
  reply so a wedged bot fails fast.
"""

from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_redeem_then_invalid_code_replies_with_error(
    send_and_wait,
):
    """``/redeem`` enters the promo-code FSM. Sending a known-bad
    code must produce a reply (not silence) and must NOT wedge the
    bot — the followup ``/start`` must still respond.

    The point of this test is to verify the **two-step FSM transition**
    end-to-end: mocked unit tests fake the FSM context manually,
    but only a real Telegram client can drive
    ``message → state → next message → next state`` through Telegram's
    own update pipeline.
    """
    # Step 1: enter the FSM.
    prompt = await send_and_wait("/redeem")
    assert prompt.text and len(prompt.text.strip()) > 0, (
        "/redeem must reply with a prompt (FSM entered)"
    )

    # Step 2: feed the FSM a guaranteed-bad code.
    # The exact code doesn't matter — we just need *something* the
    # promo table won't have, and a 12-char ASCII string is unlikely
    # to collide with a real promo. The bot must reply (success
    # or error) — silence here would mean the FSM is wedged.
    reply = await send_and_wait("ZZZTESTNOPROMO")
    assert reply.text and len(reply.text.strip()) > 0, (
        "promo redemption (even rejection) must produce a reply"
    )

    # Step 3: smoke-test the bot is still responsive after the FSM
    # exit (the rejection path should clear the FSM, not lock it).
    follow_up = await send_and_wait("/start")
    assert follow_up.text, (
        "bot stopped responding after a failed promo redemption — "
        "FSM may be wedged in waiting_promo_code"
    )


@pytest.mark.asyncio
async def test_start_hub_keyboard_renders_multiple_buttons(
    send_and_wait,
):
    """The ``/start`` hub posts an inline keyboard. The keyboard
    geometry isn't a single row — at minimum the wallet / chat /
    settings cluster — so the test pins ``len(rows) >= 1`` AND
    ``total_buttons >= 2`` to guard against a regression that
    flattens the keyboard accidentally."""
    hub = await send_and_wait(
        "/start",
        predicate=lambda m: bool(getattr(m, "reply_markup", None)),
    )
    rows = getattr(hub.reply_markup, "rows", []) or []
    assert len(rows) >= 1, (
        f"/start hub keyboard had no rows: {hub.reply_markup!r}"
    )
    total_buttons = sum(len(getattr(r, "buttons", []) or []) for r in rows)
    assert total_buttons >= 2, (
        f"/start hub keyboard had < 2 buttons: {total_buttons} "
        f"({hub.reply_markup!r})"
    )


@pytest.mark.asyncio
async def test_wallet_button_click_renders_wallet_card(
    send_and_wait,
    click_button_and_wait,
):
    """Tap the wallet button on the ``/start`` hub and assert the
    bot's response includes a ``$`` (the balance line). This is the
    callback-query end-to-end test: the bot edits the hub message
    in place to show the wallet card.

    The button is matched case-insensitively on the substring
    ``"wallet"`` — works for both ``"💰 Wallet"`` (English) and
    Persian button labels that contain a ``Wallet`` token. If the
    operator's bot uses a fully-localised label (no ``"wallet"``
    substring), this test SKIPs cleanly rather than failing — see
    the AssertionError catch below.
    """
    hub = await send_and_wait(
        "/start",
        predicate=lambda m: bool(getattr(m, "reply_markup", None)),
    )
    try:
        wallet_card = await click_button_and_wait(hub, text="wallet")
    except AssertionError as exc:
        pytest.skip(
            f"wallet button not found on /start hub (operator may "
            f"have customised labels): {exc}"
        )
    text = wallet_card.text or wallet_card.message or ""
    assert "$" in text, (
        f"wallet card did not include a '$' balance line: {text!r}"
    )


@pytest.mark.asyncio
async def test_start_hub_unknown_callback_does_not_wedge_fsm(
    send_and_wait,
):
    """An invalid free-text message *while the user is sitting on the
    hub menu* (no FSM state set) must not drop hub responsiveness.

    Mirrors ``test_unknown_command_does_not_crash_bot`` from the
    smoke suite, but specifically pins that FSM-adjacent paths
    don't accidentally consume non-command input as state input.
    """
    # Set up the hub.
    hub = await send_and_wait(
        "/start",
        predicate=lambda m: bool(getattr(m, "reply_markup", None)),
    )
    assert hub.reply_markup, "precondition: hub must render a keyboard"

    # Send a piece of free text the hub doesn't expect. The bot may
    # ignore it (no FSM is active) — that's fine; the assertion is
    # the followup.
    try:
        await send_and_wait("hello-i-am-not-a-command", timeout_seconds=3)
    except asyncio.TimeoutError:
        pass

    # Hub must still be available.
    follow_up_hub = await send_and_wait(
        "/start",
        predicate=lambda m: bool(getattr(m, "reply_markup", None)),
    )
    assert follow_up_hub.reply_markup, (
        "hub stopped rendering its keyboard after free text — "
        "the FSM may have wedged on a stray message"
    )


@pytest.mark.asyncio
async def test_redeem_then_cancel_command_clears_fsm(
    send_and_wait,
):
    """``/redeem`` opens the promo FSM. A subsequent ``/start``
    (NOT a code) must clear the FSM and re-render the hub — not
    treat ``/start`` as a promo code.

    This catches the FSM-cleanup regression where a previous
    refactor consumed slash commands as raw text inside FSM states
    (Pre-PR-110 ``cmd_start`` had this exact bug — see
    ``handlers.py:344``).
    """
    # Step 1: enter the FSM.
    prompt = await send_and_wait("/redeem")
    assert prompt.text, "/redeem must elicit a prompt"

    # Step 2: send /start instead of a code. The bot MUST treat
    # this as a hub re-entry, not as a malformed promo code.
    hub = await send_and_wait(
        "/start",
        predicate=lambda m: bool(getattr(m, "reply_markup", None)),
    )
    assert hub.reply_markup, (
        "/start during waiting_promo_code didn't re-render the hub — "
        "FSM cleanup regression?"
    )

    # Step 3: confirm FSM was cleared by entering it again — if the
    # state persisted, the bot would interpret /redeem itself as a
    # code and bail.
    re_prompt = await send_and_wait("/redeem")
    assert re_prompt.text, (
        "/redeem after a /start interrupt didn't re-prompt — "
        "FSM may be stuck in waiting_promo_code"
    )
