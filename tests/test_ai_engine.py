"""Tests for ``ai_engine.chat_with_model`` settlement logic.

The settlement path is money-touching: if the chooser between the
free-message branch and the paid-balance branch gets it wrong, the bot
either silently eats OpenRouter cost or double-charges the user. The
race-pin tests below cover the bug fixed in this PR (concurrent free
messages with stale ``free_messages_left``).

We mock the OpenRouter HTTP call out (we don't want the test suite
hitting a real API) and inject mock ``db`` methods, then assert which
settlement primitive was awaited and with which arguments.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import ai_engine


# Default OpenRouter response shape: 1 user prompt, 1 assistant
# completion, both 100 tokens, model is ``test/m1``.
def _ok_openrouter_body() -> dict:
    return {
        "choices": [
            {"message": {"content": "hello back"}},
        ],
        "usage": {
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150,
        },
    }


class _StubResponse:
    """Minimal awaitable async-context-manager mimicking aiohttp's
    ``ClientResponse`` for the two methods we use: ``json()`` and
    ``text()`` plus the ``status`` attribute."""

    def __init__(self, status: int = 200, body: dict | None = None):
        self.status = status
        self._body = body or _ok_openrouter_body()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def json(self):
        return self._body

    async def text(self):
        return repr(self._body)


class _StubSession:
    def __init__(self, response: _StubResponse):
        self._response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def post(self, *_args, **_kwargs):
        return self._response


def _patched_session(response: _StubResponse):
    """Build a ``patch.object(ai_engine.aiohttp, 'ClientSession', ...)``
    that returns the stub session unconditionally."""
    return patch.object(
        ai_engine.aiohttp,
        "ClientSession",
        MagicMock(return_value=_StubSession(response)),
    )


@pytest.fixture
def stub_db(monkeypatch):
    """Replace ``ai_engine.db`` with an ``AsyncMock`` we can configure
    per-test. ``decrement_free_message`` and ``deduct_balance`` get
    sensible defaults so tests only override the field they care about.
    """
    db = AsyncMock()
    db.get_user = AsyncMock(
        return_value={
            "free_messages_left": 5,
            "balance_usd": 10.0,
            "active_model": "test/m1",
            "language_code": "en",
            "memory_enabled": False,
        }
    )
    # Default: free decrement succeeds and returns the new counter.
    db.decrement_free_message = AsyncMock(return_value=4)
    db.deduct_balance = AsyncMock(return_value=True)
    db.log_usage = AsyncMock()
    db.append_conversation_message = AsyncMock()
    db.get_recent_messages = AsyncMock(return_value=[])
    monkeypatch.setattr(ai_engine, "db", db)
    return db


# ---------------------------------------------------------------------
# Free-message TOCTOU race regression pin.
# ---------------------------------------------------------------------
# Pre-fix: when ``decrement_free_message`` returned ``None`` (counter
# already at 0 because a concurrent prompt won the race), the settlement
# silently moved on without touching balance or writing a usage_log.
# So a user with ``free_messages_left=1`` could fire N concurrent
# prompts and get N free replies. The test below asserts that the
# fall-through to paid settlement now happens, charging the wallet
# exactly like a normal paid call.
# ---------------------------------------------------------------------
async def test_free_message_race_falls_back_to_paid_settlement(
    stub_db,
):
    """``decrement_free_message`` returns None (lost the race) ⇒ we
    must charge the wallet via ``deduct_balance`` and write a
    ``usage_logs`` row, NOT swallow the call silently."""
    # User snapshot says they have 1 free message, but decrement loses
    # the race and returns None — exactly the production scenario.
    stub_db.get_user.return_value = {
        "free_messages_left": 1,
        "balance_usd": 10.0,
        "active_model": "test/m1",
        "language_code": "en",
        "memory_enabled": False,
    }
    stub_db.decrement_free_message.return_value = None

    with _patched_session(_StubResponse()):
        reply = await ai_engine.chat_with_model(42, "hi")

    assert reply == "hello back"
    # Tried the free path first.
    stub_db.decrement_free_message.assert_awaited_once_with(42)
    # Lost the race ⇒ must have charged the wallet.
    stub_db.deduct_balance.assert_awaited_once()
    args, kwargs = stub_db.deduct_balance.await_args
    # Positional ``(telegram_id, cost_usd)``.
    assert args[0] == 42
    assert args[1] > 0
    # And recorded the usage in the cost ledger so audits reconcile.
    stub_db.log_usage.assert_awaited_once()
    log_args, _ = stub_db.log_usage.await_args
    assert log_args[0] == 42                # telegram_id
    assert log_args[1] == "test/m1"         # model
    assert log_args[2] == 100               # prompt tokens
    assert log_args[3] == 50                # completion tokens
    assert log_args[4] > 0                  # charged > 0


async def test_free_message_normal_path_does_not_charge_balance(
    stub_db,
):
    """When ``decrement_free_message`` succeeds (returns the new
    counter, possibly 0), we must NOT also charge the wallet — that
    would double-bill the user. Pin so a future refactor doesn't
    regress us into "free path falls through always"."""
    stub_db.decrement_free_message.return_value = 4

    with _patched_session(_StubResponse()):
        reply = await ai_engine.chat_with_model(42, "hi")

    assert reply == "hello back"
    stub_db.decrement_free_message.assert_awaited_once_with(42)
    stub_db.deduct_balance.assert_not_awaited()
    stub_db.log_usage.assert_not_awaited()


async def test_paid_user_path_charges_balance_and_logs_usage(stub_db):
    """User with no free messages and a positive balance: must take
    the paid branch directly (no free decrement) and write a
    usage_log with the cost. This is the existing happy path; pinned
    so the new race-fallback logic doesn't accidentally call
    ``decrement_free_message`` for already-paid users."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 10.0,
        "active_model": "test/m1",
        "language_code": "en",
        "memory_enabled": False,
    }

    with _patched_session(_StubResponse()):
        reply = await ai_engine.chat_with_model(42, "hi")

    assert reply == "hello back"
    stub_db.decrement_free_message.assert_not_awaited()
    stub_db.deduct_balance.assert_awaited_once()
    stub_db.log_usage.assert_awaited_once()


async def test_insufficient_balance_after_pre_check_logs_zero(stub_db):
    """When ``deduct_balance`` returns False (balance was sufficient
    at the pre-check but a concurrent debit drained it before
    settlement), we still write a usage_log row — with
    ``cost_deducted_usd=0`` — so SUM(cost) on usage_logs reconciles
    with actual balance changes. This is the existing behaviour;
    pinned alongside the new race-fallback so the two paths share
    the same regression test surface."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 10.0,
        "active_model": "test/m1",
        "language_code": "en",
        "memory_enabled": False,
    }
    stub_db.deduct_balance.return_value = False

    with _patched_session(_StubResponse()):
        reply = await ai_engine.chat_with_model(42, "hi")

    assert reply == "hello back"
    stub_db.log_usage.assert_awaited_once()
    log_args, _ = stub_db.log_usage.await_args
    # Cost recorded as 0 since deduction didn't happen.
    assert log_args[4] == 0.0


async def test_pre_check_blocks_when_no_free_and_low_balance(stub_db):
    """``free_messages_left=0`` AND ``balance < 0.05`` ⇒ short-circuit
    with the i18n string and don't even call OpenRouter. Pin
    because the race fallback could in principle bypass this if
    written carelessly."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 0.01,
        "active_model": "test/m1",
        "language_code": "en",
        "memory_enabled": False,
    }
    # Don't even patch ClientSession — if we got far enough to call
    # OpenRouter the test would crash on the missing patch.
    reply = await ai_engine.chat_with_model(42, "hi")
    # Falls through to the i18n insufficient-balance string.
    assert "balance" in reply.lower() or "موجودی" in reply
    stub_db.decrement_free_message.assert_not_awaited()
    stub_db.deduct_balance.assert_not_awaited()
    stub_db.log_usage.assert_not_awaited()
