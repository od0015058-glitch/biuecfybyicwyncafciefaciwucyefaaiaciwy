"""Tests for Stage-13-Step-B: per-user in-flight chat slot.

Covers:

* :func:`rate_limit.try_claim_chat_slot` and
  :func:`rate_limit.release_chat_slot` — the slot primitives that
  enforce one in-flight OpenRouter request per user.
* The ``process_chat`` handler wiring — second prompt while first is
  in flight gets the ``ai_chat_busy`` flash, slot is released even
  when ``chat_with_model`` raises, both gates fire in the correct
  order (token bucket first, slot second).
* The bundled bug fix — ``ai_engine.chat_with_model`` returns
  ``ai_provider_unavailable`` for OpenRouter 200 responses with
  ``content: null`` (tool-call shape / safety-policy refusals), and
  ``process_chat`` rewrites empty / ``None`` returns to the same
  fallback so Telegram never sees an empty ``sendMessage`` body.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import rate_limit as rl
from rate_limit import (
    release_chat_slot,
    reset_chat_inflight_slots_for_tests,
    try_claim_chat_slot,
)


@pytest.fixture(autouse=True)
def _clean_inflight_slots():
    """Each test starts with an empty in-flight set so a leak from
    one test doesn't bleed into the next."""
    reset_chat_inflight_slots_for_tests()
    yield
    reset_chat_inflight_slots_for_tests()


# ---------------------------------------------------------------------
# Slot primitives
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_first_claim_succeeds():
    """A fresh user can claim the slot."""
    assert await try_claim_chat_slot(7777) is True


@pytest.mark.asyncio
async def test_second_claim_rejected_until_release():
    """While a slot is held, a second claim for the same user
    returns False; after release it becomes claimable again."""
    assert await try_claim_chat_slot(7777) is True
    assert await try_claim_chat_slot(7777) is False
    await release_chat_slot(7777)
    assert await try_claim_chat_slot(7777) is True


@pytest.mark.asyncio
async def test_release_is_idempotent():
    """Releasing a slot that was never claimed (or already released)
    is a no-op so a forgotten ``try`` block can't deadlock the user."""
    await release_chat_slot(7777)  # never claimed
    await release_chat_slot(7777)  # release of a non-claim
    assert await try_claim_chat_slot(7777) is True
    await release_chat_slot(7777)
    await release_chat_slot(7777)  # double release


@pytest.mark.asyncio
async def test_separate_users_have_separate_slots():
    """One user's in-flight request must not block another user's."""
    assert await try_claim_chat_slot(1) is True
    # User 2 has a fresh slot.
    assert await try_claim_chat_slot(2) is True
    # Both users are now busy.
    assert await try_claim_chat_slot(1) is False
    assert await try_claim_chat_slot(2) is False


@pytest.mark.asyncio
async def test_concurrent_claim_for_same_user_only_one_wins():
    """If two ``try_claim_chat_slot`` calls race on the same user,
    exactly one of them must succeed. The lock makes the test-and-add
    atomic so this is deterministic."""
    results = await asyncio.gather(
        try_claim_chat_slot(42), try_claim_chat_slot(42)
    )
    assert sorted(results) == [False, True]


@pytest.mark.asyncio
async def test_concurrent_claim_for_different_users_both_win():
    """Independent users hitting the slot simultaneously both succeed."""
    results = await asyncio.gather(
        try_claim_chat_slot(101),
        try_claim_chat_slot(202),
        try_claim_chat_slot(303),
    )
    assert results == [True, True, True]


@pytest.mark.asyncio
async def test_claim_evicts_when_capacity_exceeded(monkeypatch):
    """When the in-flight set hits ``_CHAT_INFLIGHT_MAX`` entries, a
    fresh claim FIFO-evicts the oldest stuck slot rather than refusing
    every new user. Defence against a slow leak from a forgotten
    release."""
    monkeypatch.setattr(rl, "_CHAT_INFLIGHT_MAX", 3)
    # Fill with three "stuck" users.
    assert await try_claim_chat_slot(1) is True
    assert await try_claim_chat_slot(2) is True
    assert await try_claim_chat_slot(3) is True
    # New user pushes over capacity → oldest (user 1) is evicted.
    assert await try_claim_chat_slot(4) is True
    # User 1's slot was forcibly released by eviction, so they can
    # claim again immediately.
    assert await try_claim_chat_slot(1) is True


@pytest.mark.asyncio
async def test_reset_for_tests_clears_all_slots():
    """The test-only reset helper must wipe every slot so test
    isolation works."""
    assert await try_claim_chat_slot(1) is True
    assert await try_claim_chat_slot(2) is True
    reset_chat_inflight_slots_for_tests()
    assert await try_claim_chat_slot(1) is True
    assert await try_claim_chat_slot(2) is True


# ---------------------------------------------------------------------
# process_chat wiring
# ---------------------------------------------------------------------


def _make_message(text: str = "hi", user_id: int = 555) -> MagicMock:
    """Build a minimal ``Message`` mock that ``process_chat`` accepts."""
    msg = MagicMock()
    msg.text = text
    msg.from_user = MagicMock()
    msg.from_user.id = user_id
    msg.chat = MagicMock()
    msg.chat.id = user_id
    msg.bot = MagicMock()
    msg.bot.send_chat_action = AsyncMock()
    msg.answer = AsyncMock()
    return msg


@pytest.mark.asyncio
async def test_process_chat_releases_slot_on_success():
    """The slot must be released after a normal ``chat_with_model``
    return so the user can send another prompt."""
    import handlers

    msg = _make_message()
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch(
            "handlers.chat_with_model", AsyncMock(return_value="ok reply")
        ),
        patch(
            "handlers._get_user_language", AsyncMock(return_value="en")
        ),
    ):
        await handlers.process_chat(msg)

    # Slot was claimed and released; a fresh claim now succeeds.
    assert await try_claim_chat_slot(555) is True


@pytest.mark.asyncio
async def test_process_chat_releases_slot_on_exception():
    """If ``chat_with_model`` raises, the slot must still be released
    (try/finally) so the user isn't permanently locked out."""
    import handlers

    msg = _make_message()
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch(
            "handlers.chat_with_model",
            AsyncMock(side_effect=RuntimeError("upstream blew up")),
        ),
        patch(
            "handlers._get_user_language", AsyncMock(return_value="en")
        ),
    ):
        with pytest.raises(RuntimeError):
            await handlers.process_chat(msg)

    assert await try_claim_chat_slot(555) is True


@pytest.mark.asyncio
async def test_process_chat_second_prompt_rejected_with_busy_flash():
    """While a slot is held, a second ``process_chat`` call for the
    same user replies with ``ai_chat_busy`` and DOES NOT call
    ``chat_with_model``."""
    import handlers

    # Pre-claim the slot to simulate a still-in-flight first call.
    assert await try_claim_chat_slot(555) is True

    msg = _make_message()
    busy_chat = AsyncMock()
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", busy_chat),
        patch(
            "handlers._get_user_language", AsyncMock(return_value="en")
        ),
    ):
        await handlers.process_chat(msg)

    busy_chat.assert_not_awaited()
    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    assert "still being processed" in sent.lower()


@pytest.mark.asyncio
async def test_process_chat_token_bucket_fires_before_inflight_slot():
    """Order of gates matters: the token bucket fires BEFORE the
    in-flight slot so a rate-limited prompt doesn't tie up the slot
    for the legitimate next prompt."""
    import handlers

    msg = _make_message()
    chat_call = AsyncMock(return_value="ok")
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=False)),
        patch("handlers.chat_with_model", chat_call),
        patch(
            "handlers._get_user_language", AsyncMock(return_value="en")
        ),
    ):
        await handlers.process_chat(msg)

    chat_call.assert_not_awaited()
    # The slot must NOT be held — the rate limiter rejected before
    # we tried to claim it.
    assert await try_claim_chat_slot(555) is True


@pytest.mark.asyncio
async def test_process_chat_busy_flash_is_per_user_not_global():
    """User A being busy must not flash user B with ``ai_chat_busy``."""
    import handlers

    # User A is in flight.
    assert await try_claim_chat_slot(101) is True

    # User B sends a prompt — should go through normally.
    msg_b = _make_message(user_id=202)
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch(
            "handlers.chat_with_model", AsyncMock(return_value="hello")
        ),
        patch(
            "handlers._get_user_language", AsyncMock(return_value="en")
        ),
    ):
        await handlers.process_chat(msg_b)

    sent = msg_b.answer.await_args.args[0]
    assert "still being processed" not in sent.lower()
    assert "hello" in sent


# ---------------------------------------------------------------------
# Bundled bug fix: empty / None reply_text
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_chat_none_reply_falls_back_to_provider_unavailable():
    """If ``chat_with_model`` returns ``None`` (defence in depth — the
    ai_engine layer also catches this now), the handler MUST send the
    ``ai_provider_unavailable`` text rather than forwarding ``None``
    to Telegram (which would crash with ``Bad Request: text is empty``)."""
    import handlers

    msg = _make_message()
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", AsyncMock(return_value=None)),
        patch(
            "handlers._get_user_language", AsyncMock(return_value="en")
        ),
    ):
        await handlers.process_chat(msg)

    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    # ``ai_provider_unavailable`` text varies by locale but must be
    # non-empty and contain the canonical "unavailable" / "try again"
    # idiom — easier to assert non-emptiness + that we did not pass
    # ``None`` to Telegram.
    assert sent, "must not pass empty/None body to message.answer"


@pytest.mark.asyncio
async def test_process_chat_empty_string_reply_falls_back():
    """Empty-string reply gets the same fallback so we never hand
    Telegram an empty ``sendMessage``."""
    import handlers

    msg = _make_message()
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", AsyncMock(return_value="")),
        patch(
            "handlers._get_user_language", AsyncMock(return_value="en")
        ),
    ):
        await handlers.process_chat(msg)

    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    assert sent, "must not pass empty body to message.answer"


@pytest.mark.asyncio
async def test_process_chat_normal_reply_is_passed_through():
    """Sanity: non-empty replies are NOT rewritten — they're sent as-is
    (chunked through ``_split_for_telegram`` for >4000-char bodies)."""
    import handlers

    msg = _make_message()
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch(
            "handlers.chat_with_model",
            AsyncMock(return_value="the meaning of life is 42"),
        ),
        patch(
            "handlers._get_user_language", AsyncMock(return_value="en")
        ),
    ):
        await handlers.process_chat(msg)

    sent = msg.answer.await_args.args[0]
    assert sent == "the meaning of life is 42"


# ---------------------------------------------------------------------
# ai_engine: content=null guard
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ai_engine_returns_unavailable_for_null_content():
    """A 200 OK with ``content: null`` (tool-call shape / safety
    refusal) must be treated as ``ai_provider_unavailable`` rather
    than forwarded as a literal ``None`` body."""
    import ai_engine

    fake_user = {
        "free_messages_left": 5,
        "balance_usd": 10.0,
        "active_model": "openai/gpt-3.5-turbo",
        "language_code": "en",
        "memory_enabled": False,
    }

    class _FakeResponse:
        status = 200

        async def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [],
                        }
                    }
                ],
                "usage": {"prompt_tokens": 1, "completion_tokens": 0},
            }

        async def text(self):
            return ""

    class _FakeCtx:
        async def __aenter__(self):
            return _FakeResponse()

        async def __aexit__(self, *exc):
            return False

    class _FakeSession:
        def __init__(self, *_a, **_kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        def post(self, *_a, **_kw):
            return _FakeCtx()

    with (
        patch.object(ai_engine, "OPENROUTER_API_KEY", "test-key"),
        patch.object(
            ai_engine.db, "get_user", AsyncMock(return_value=fake_user)
        ),
        patch.object(ai_engine.aiohttp, "ClientSession", _FakeSession),
    ):
        out = await ai_engine.chat_with_model(123, "hi")

    # We never want None / empty leaked.
    assert out, "ai_engine must never return None / empty"
    # And we want the canonical fallback string, not a raw "None".
    assert "None" not in out


@pytest.mark.asyncio
async def test_ai_engine_returns_unavailable_for_empty_string_content():
    """``content: ""`` is just as bad as ``null`` — Telegram still
    rejects an empty body. Same fallback path."""
    import ai_engine

    fake_user = {
        "free_messages_left": 5,
        "balance_usd": 10.0,
        "active_model": "openai/gpt-3.5-turbo",
        "language_code": "en",
        "memory_enabled": False,
    }

    class _FakeResponse:
        status = 200

        async def json(self):
            return {
                "choices": [{"message": {"role": "assistant", "content": ""}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 0},
            }

        async def text(self):
            return ""

    class _FakeCtx:
        async def __aenter__(self):
            return _FakeResponse()

        async def __aexit__(self, *exc):
            return False

    class _FakeSession:
        def __init__(self, *_a, **_kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        def post(self, *_a, **_kw):
            return _FakeCtx()

    with (
        patch.object(ai_engine, "OPENROUTER_API_KEY", "test-key"),
        patch.object(
            ai_engine.db, "get_user", AsyncMock(return_value=fake_user)
        ),
        patch.object(ai_engine.aiohttp, "ClientSession", _FakeSession),
    ):
        out = await ai_engine.chat_with_model(123, "hi")

    assert out, "empty content must surface a non-empty fallback"


# ---------------------------------------------------------------------
# Strings: ai_chat_busy localised in fa + en
# ---------------------------------------------------------------------


def test_ai_chat_busy_string_exists_fa():
    from strings import t

    out = t("fa", "ai_chat_busy")
    assert out and "{" not in out, "fa string must be present and rendered"


def test_ai_chat_busy_string_exists_en():
    from strings import t

    out = t("en", "ai_chat_busy")
    assert out and "{" not in out, "en string must be present and rendered"
