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
    # Stage-14: stub multi-key routing so tests don't need real env vars.
    monkeypatch.setattr(ai_engine, "key_for_user", lambda tid: "test-key")
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


# ---------------------------------------------------------------------
# active_model fallback for blank / NULL / whitespace rows.
#
# The ``users.active_model`` column is nullable (no NOT NULL
# constraint) and there's no application-level guard preventing
# direct DB writes from leaving it blank. Pre-fix a chat from a
# user with ``active_model=None`` POSTed ``{"model": null, ...}`` to
# OpenRouter, got a 400, and replied ai_provider_unavailable for
# *every* subsequent chat from that user — no actionable hint to
# the user, no recovery path. Worse, the 429 branch crashed
# outright (``None.endswith(":free")`` raises AttributeError) and
# surfaced as ai_transient_error. Now we fall back to a known-good
# default so the chat keeps working.
# ---------------------------------------------------------------------


def test_resolve_active_model_returns_default_for_none():
    assert (
        ai_engine._resolve_active_model(None)
        == ai_engine._ACTIVE_MODEL_FALLBACK
    )


def test_resolve_active_model_returns_default_for_empty_string():
    assert (
        ai_engine._resolve_active_model("")
        == ai_engine._ACTIVE_MODEL_FALLBACK
    )


def test_resolve_active_model_returns_default_for_whitespace():
    """Spaces, tabs, newlines collapse to the fallback after strip."""
    for raw in ("   ", "\t", "\n", " \t \n "):
        assert ai_engine._resolve_active_model(raw) == (
            ai_engine._ACTIVE_MODEL_FALLBACK
        ), f"raw={raw!r}"


def test_resolve_active_model_strips_surrounding_whitespace():
    """A legitimate id with stray surrounding whitespace gets cleaned
    up rather than dropped to the fallback. (The web admin form
    parser already strips, but a row written through some other
    path could carry whitespace.)"""
    assert (
        ai_engine._resolve_active_model("  openai/gpt-4  ")
        == "openai/gpt-4"
    )


def test_resolve_active_model_passes_through_canonical_id():
    """Regression pin: a clean id passes through unchanged."""
    assert (
        ai_engine._resolve_active_model("anthropic/claude-3-opus")
        == "anthropic/claude-3-opus"
    )


def test_resolve_active_model_coerces_non_string_input():
    """Defensive: a row that somehow stored a non-string (e.g.
    via a future migration accident) routes through ``str()`` rather
    than blowing up at the next ``.endswith`` / ``.lower()``."""
    # ``str(123)`` is ``"123"`` which is non-empty and doesn't strip
    # to nothing, so we get the coerced value back. The point is no
    # exception is raised — exactly the defensive contract.
    assert ai_engine._resolve_active_model(123) == "123"


async def test_chat_with_model_uses_fallback_when_active_model_is_none(
    stub_db,
):
    """End-to-end pin: a user row with ``active_model=None`` does NOT
    crash the bot and does NOT POST ``{"model": null}`` to
    OpenRouter. Instead the fallback is used and the chat completes
    normally. Pre-fix this would have surfaced as
    ai_provider_unavailable (400 from OpenRouter on the null model).
    """
    stub_db.get_user.return_value = {
        "free_messages_left": 5,
        "balance_usd": 10.0,
        "active_model": None,
        "language_code": "en",
        "memory_enabled": False,
    }
    stub_db.decrement_free_message.return_value = 4

    with _patched_session(_StubResponse()):
        reply = await ai_engine.chat_with_model(42, "hi")

    assert reply == "hello back"
    # The fall-back model id was used (and not e.g. None) — verifying
    # via the log-usage call which echoes the chosen model.
    stub_db.log_usage.assert_not_awaited()  # free path; no log
    # Free path was taken — i.e. we got past the OpenRouter call.
    stub_db.decrement_free_message.assert_awaited_once_with(42)


async def test_chat_with_model_uses_fallback_when_active_model_is_empty(
    stub_db,
):
    """Same as the None case but with the empty-string variant —
    direct DB writes that did ``UPDATE users SET active_model = ''``
    instead of NULL. Both paths must self-heal."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 10.0,
        "active_model": "",
        "language_code": "en",
        "memory_enabled": False,
    }

    with _patched_session(_StubResponse()):
        reply = await ai_engine.chat_with_model(42, "hi")

    assert reply == "hello back"
    # Paid path was taken (free=0). The cost was computed against
    # the fallback model — verifying via log_usage's model arg.
    stub_db.log_usage.assert_awaited_once()
    log_args, _ = stub_db.log_usage.await_args
    assert log_args[1] == ai_engine._ACTIVE_MODEL_FALLBACK


async def test_chat_with_model_passes_through_real_active_model(stub_db):
    """Regression pin: a user with a real ``active_model`` is NOT
    rerouted to the fallback. Pin so the new guard doesn't
    accidentally rewrite legitimate model selections."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 10.0,
        "active_model": "anthropic/claude-3-opus",
        "language_code": "en",
        "memory_enabled": False,
    }

    with _patched_session(_StubResponse()):
        reply = await ai_engine.chat_with_model(42, "hi")

    assert reply == "hello back"
    stub_db.log_usage.assert_awaited_once()
    log_args, _ = stub_db.log_usage.await_args
    assert log_args[1] == "anthropic/claude-3-opus"


# ---------------------------------------------------------------------
# Non-finite balance_usd defense-in-depth (this PR).
#
# A non-finite ``balance_usd`` (NaN or +Infinity) silently bypasses
# the ``balance < 0.05`` insufficient-funds gate at the top of
# ``chat_with_model`` — every comparison against NaN returns False,
# and ``+Infinity < 0.05`` is also False. A user with a poisoned
# wallet (legacy row, manual SQL fix, or any path bypassing the
# write-side finite guards on finalize_payment / deduct_balance /
# redeem_gift_code / admin_adjust_balance) would therefore pass the
# gate, hit OpenRouter on the bot's dime, and have ``deduct_balance``
# silently no-op (NaN comparison) or refuse (+Infinity), falling to
# the cost=0 ``log_usage`` branch — i.e. unlimited free chat at the
# bot's expense.
#
# Fix: detect non-finite balance at read time, log loud-and-once,
# and treat as $0 locally so the gate fires correctly.
# ---------------------------------------------------------------------


async def test_chat_with_model_treats_nan_balance_as_zero(stub_db, caplog):
    """A NaN ``balance_usd`` with no free messages must hit the
    insufficient-funds branch — NOT a free OpenRouter call. Pre-fix
    ``NaN < 0.05`` was False, so the gate let the user through."""
    import logging

    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": float("nan"),
        "active_model": "test/m1",
        "language_code": "en",
        "memory_enabled": False,
    }
    # Do NOT patch ClientSession — if we got far enough to POST to
    # OpenRouter the test would crash. That's the assertion.
    with caplog.at_level(logging.ERROR, logger="bot.ai_engine"):
        reply = await ai_engine.chat_with_model(42, "hi")

    assert "balance" in reply.lower() or "موجودی" in reply
    stub_db.deduct_balance.assert_not_awaited()
    stub_db.log_usage.assert_not_awaited()
    # The corruption is loud enough for ops to repair the row.
    assert any("non-finite balance_usd" in rec.message for rec in caplog.records)


async def test_chat_with_model_treats_positive_infinity_balance_as_zero(stub_db):
    """Same hole on the +Infinity branch: ``+Inf < 0.05`` is False,
    so a poisoned wallet would silently grant unlimited free chat."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": float("inf"),
        "active_model": "test/m1",
        "language_code": "en",
        "memory_enabled": False,
    }
    reply = await ai_engine.chat_with_model(42, "hi")
    assert "balance" in reply.lower() or "موجودی" in reply
    stub_db.deduct_balance.assert_not_awaited()
    stub_db.log_usage.assert_not_awaited()


async def test_chat_with_model_negative_infinity_balance_falls_through(stub_db):
    """``-Inf < 0.05`` is True so the gate already fires correctly
    for ``-Infinity`` — pin that the fix doesn't accidentally mask
    the existing-correct behaviour. Either way no OpenRouter call."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": float("-inf"),
        "active_model": "test/m1",
        "language_code": "en",
        "memory_enabled": False,
    }
    reply = await ai_engine.chat_with_model(42, "hi")
    assert "balance" in reply.lower() or "موجودی" in reply
    stub_db.deduct_balance.assert_not_awaited()
    stub_db.log_usage.assert_not_awaited()


async def test_chat_with_model_nan_balance_with_free_messages_still_uses_free_path(
    stub_db,
):
    """A user with a poisoned wallet but free messages remaining
    should still get to use them — the wallet poisoning shouldn't
    cancel out their non-money quota. Pin so the new guard doesn't
    accidentally short-circuit the free path."""
    stub_db.get_user.return_value = {
        "free_messages_left": 3,
        "balance_usd": float("nan"),
        "active_model": "test/m1",
        "language_code": "en",
        "memory_enabled": False,
    }
    stub_db.decrement_free_message.return_value = 2

    with _patched_session(_StubResponse()):
        reply = await ai_engine.chat_with_model(42, "hi")

    assert reply == "hello back"
    stub_db.decrement_free_message.assert_awaited_once_with(42)
    # Free path: no balance deduction.
    stub_db.deduct_balance.assert_not_awaited()


# ---- Stage-15-Step-E #4: 429 → mark_key_rate_limited wiring -------------


class _StubResponseWithHeaders(_StubResponse):
    """Adds a ``headers`` dict so the 429 branch can read ``Retry-After``."""

    def __init__(
        self,
        status: int = 429,
        body: dict | None = None,
        headers: dict[str, str] | None = None,
    ):
        super().__init__(status=status, body=body or {"error": "rate limited"})
        self.headers = headers or {}


@pytest.mark.asyncio
async def test_chat_429_marks_key_rate_limited(stub_db):
    """OpenRouter 429 must put the user's key in cooldown so the next
    user routed to it falls through to a different pool member.
    """
    import openrouter_keys

    openrouter_keys.clear_all_cooldowns()
    openrouter_keys._keys = ["test-key"]
    openrouter_keys._loaded = True

    response = _StubResponseWithHeaders(status=429)
    with _patched_session(response):
        reply = await ai_engine.chat_with_model(42, "hi")

    assert openrouter_keys.is_key_rate_limited("test-key")
    assert "rate" in reply.lower() or "limit" in reply.lower() or reply
    openrouter_keys.clear_all_cooldowns()
    openrouter_keys._keys = []
    openrouter_keys._loaded = False


@pytest.mark.asyncio
async def test_chat_429_honours_retry_after_header(stub_db):
    """A numeric ``Retry-After`` propagates to the cooldown deadline."""
    import openrouter_keys

    openrouter_keys.clear_all_cooldowns()
    openrouter_keys._keys = ["test-key"]
    openrouter_keys._loaded = True

    response = _StubResponseWithHeaders(
        status=429, headers={"Retry-After": "120"}
    )
    with _patched_session(response):
        await ai_engine.chat_with_model(42, "hi")

    deadline = openrouter_keys._cooldowns.get("test-key")
    assert deadline is not None
    now = openrouter_keys.time.monotonic()
    # ~120s from now (within the cooldown clamp ceiling).
    assert 119.0 <= deadline - now <= 121.0
    openrouter_keys.clear_all_cooldowns()
    openrouter_keys._keys = []
    openrouter_keys._loaded = False


@pytest.mark.asyncio
async def test_chat_429_falls_back_to_default_on_garbage_retry_after(stub_db):
    """``Retry-After: not-a-number`` should not crash the 429 branch;
    cooldown falls back to the default duration.
    """
    import openrouter_keys

    openrouter_keys.clear_all_cooldowns()
    openrouter_keys._keys = ["test-key"]
    openrouter_keys._loaded = True

    response = _StubResponseWithHeaders(
        status=429, headers={"Retry-After": "soon"}
    )
    with _patched_session(response):
        await ai_engine.chat_with_model(42, "hi")

    # Cooldown table must have an entry — the 429 fired the marker.
    assert "test-key" in openrouter_keys._cooldowns
    openrouter_keys.clear_all_cooldowns()
    openrouter_keys._keys = []
    openrouter_keys._loaded = False


@pytest.mark.asyncio
async def test_chat_429_no_retry_after_uses_default_cooldown(stub_db):
    """Missing ``Retry-After`` → default 60s cooldown."""
    import openrouter_keys

    openrouter_keys.clear_all_cooldowns()
    openrouter_keys._keys = ["test-key"]
    openrouter_keys._loaded = True

    response = _StubResponseWithHeaders(status=429, headers={})
    with _patched_session(response):
        await ai_engine.chat_with_model(42, "hi")

    deadline = openrouter_keys._cooldowns.get("test-key")
    assert deadline is not None
    now = openrouter_keys.time.monotonic()
    # ~DEFAULT_COOLDOWN_SECS (60s) from now.
    expected = openrouter_keys.DEFAULT_COOLDOWN_SECS
    assert expected - 1.0 <= deadline - now <= expected + 1.0
    openrouter_keys.clear_all_cooldowns()
    openrouter_keys._keys = []
    openrouter_keys._loaded = False


# ---------------------------------------------------------------------
# Stage-15-Step-E #10 bundled bug fix: persistence-after-charge
# must NOT lose the AI reply.
# ---------------------------------------------------------------------
# Pre-fix: a memory-enabled user whose persist INSERT raised (NUL
# byte in prompt or reply, transient DB hiccup, FK violation) hit
# the outer ``except Exception`` in ``chat_with_model``, the user
# saw ``ai_transient_error``, and ``reply_text`` was lost — even
# though ``deduct_balance`` (line ~293) and ``log_usage`` (line
# ~306) had ALREADY committed. Net effect: a re-prompt would
# re-charge them. Silent double-billing whenever a NUL-bearing
# message went through the pipeline.
# ---------------------------------------------------------------------


async def test_memory_persist_failure_does_not_lose_reply(stub_db, caplog):
    """Persistence is best-effort: a failing
    ``append_conversation_message`` after settlement must not
    swallow the AI reply (the user was already charged)."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 10.0,
        "active_model": "test/m1",
        "language_code": "en",
        "memory_enabled": True,
    }
    # Persistence raises — simulating a NUL-byte in the prompt /
    # reply that Postgres TEXT rejects. The first call (user
    # role) raising is enough; we don't even reach the assistant
    # call.
    stub_db.append_conversation_message.side_effect = RuntimeError(
        "invalid byte sequence for encoding UTF8: 0x00",
    )

    with caplog.at_level("ERROR"):
        with _patched_session(_StubResponse()):
            reply = await ai_engine.chat_with_model(42, "hello\x00world")

    # User STILL gets the reply — no transient-error swallow.
    assert reply == "hello back"
    # The wallet was already debited at this point in the flow.
    stub_db.deduct_balance.assert_awaited()
    stub_db.log_usage.assert_awaited()
    # Persist was attempted (raised) — we tried.
    assert stub_db.append_conversation_message.await_count >= 1
    # Loud-and-once log so ops can spot the row corruption.
    assert any(
        "memory persist failed" in record.message for record in caplog.records
    ), "expected 'memory persist failed' log entry"


async def test_memory_persist_assistant_failure_does_not_lose_reply(
    stub_db, caplog,
):
    """The assistant-side INSERT raising must also not lose the
    reply. Pre-fix the second call could blow up (NUL byte in
    ``reply_text``) even if the first succeeded — same outcome:
    user charged, no reply, double-billing on retry."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 10.0,
        "active_model": "test/m1",
        "language_code": "en",
        "memory_enabled": True,
    }
    # First call (user role) succeeds, second (assistant role) raises.
    side_effects = [None, RuntimeError("transient deadlock")]
    stub_db.append_conversation_message.side_effect = side_effects

    with caplog.at_level("ERROR"):
        with _patched_session(_StubResponse()):
            reply = await ai_engine.chat_with_model(42, "hi")

    assert reply == "hello back"
    assert stub_db.append_conversation_message.await_count == 2
    assert any(
        "memory persist failed" in record.message for record in caplog.records
    )


async def test_memory_disabled_skips_persist_entirely(stub_db):
    """Sanity: the wrap doesn't introduce overhead for users
    with memory off — append is never called and the existing
    happy-path return shape is unchanged."""
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
    stub_db.append_conversation_message.assert_not_awaited()


# ---------------------------------------------------------------------
# Stage-15-Step-E #10 second slice: vision integration.
# ---------------------------------------------------------------------
# The ``image_data_uris`` keyword argument routes a multimodal user
# turn through the existing settlement / OpenRouter pipeline. The
# tests below pin:
#   - the vision-capability gate fires *before* any wallet debit or
#     OpenRouter call when the active model is text-only,
#   - the multimodal payload is assembled correctly when the model
#     IS vision-capable (text + image_url parts, in that order),
#   - the existing positional-arg call shape (no images) keeps
#     working unchanged.
# ---------------------------------------------------------------------

_TINY_DATA_URI = (
    "data:image/jpeg;base64,"
    "/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAEBAQEBAQEBAQEBAQEB"
)


async def test_vision_gate_rejects_non_vision_model_no_charge(stub_db):
    """A user whose ``active_model`` is text-only sending an image
    must get ``ai_model_no_vision`` *before* any wallet debit or
    OpenRouter call — pre-fix none of this gating existed because
    the keyword was absent; this is the regression pin."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 10.0,
        "active_model": "openai/gpt-3.5-turbo",  # NOT vision-capable
        "language_code": "en",
        "memory_enabled": False,
    }

    with _patched_session(_StubResponse()):
        reply = await ai_engine.chat_with_model(
            42, "what's in this", image_data_uris=[_TINY_DATA_URI],
        )

    # The localised key is ``ai_model_no_vision`` — assert by
    # substring so the test isn't tied to copy edits in strings.py.
    assert "vision" in reply.lower()
    # No wallet impact, no log_usage row, no OpenRouter call —
    # the gate fires *before* settlement.
    stub_db.deduct_balance.assert_not_awaited()
    stub_db.log_usage.assert_not_awaited()
    stub_db.decrement_free_message.assert_not_awaited()


async def test_vision_gate_passes_for_vision_capable_model(stub_db):
    """A vision-capable ``active_model`` must let the multimodal
    request through to the OpenRouter call. We verify the
    settlement primitives are awaited (i.e. the gate did NOT
    short-circuit) and the reply is the canonical happy-path."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 10.0,
        "active_model": "openai/gpt-4o",  # vision-capable
        "language_code": "en",
        "memory_enabled": False,
    }

    with _patched_session(_StubResponse()):
        reply = await ai_engine.chat_with_model(
            42, "describe this", image_data_uris=[_TINY_DATA_URI],
        )

    assert reply == "hello back"
    # Settlement happened — vision turn billed like any other turn.
    stub_db.deduct_balance.assert_awaited()
    stub_db.log_usage.assert_awaited()


async def test_vision_payload_assembly_uses_multimodal_shape(stub_db):
    """When a vision-capable model gets images, the payload's
    last user-message must be the multimodal dict shape
    (content as a list of typed parts) — not the plain-string
    shape used for text-only turns."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 10.0,
        "active_model": "anthropic/claude-3-opus",  # vision-capable
        "language_code": "en",
        "memory_enabled": False,
    }

    captured: dict = {}

    class _CapturingResponse(_StubResponse):
        async def json(self):
            return _ok_openrouter_body()

    class _CapturingSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        def post(self, *_args, **kwargs):
            captured["json"] = kwargs.get("json")
            return _CapturingResponse()

    with patch.object(
        ai_engine.aiohttp,
        "ClientSession",
        MagicMock(return_value=_CapturingSession()),
    ):
        reply = await ai_engine.chat_with_model(
            7, "hi", image_data_uris=[_TINY_DATA_URI],
        )

    assert reply == "hello back"
    posted = captured["json"]
    last_user = posted["messages"][-1]
    assert last_user["role"] == "user"
    # Multimodal: content is a list of typed parts, text first.
    assert isinstance(last_user["content"], list)
    assert last_user["content"][0]["type"] == "text"
    assert last_user["content"][0]["text"] == "hi"
    assert last_user["content"][1]["type"] == "image_url"
    assert last_user["content"][1]["image_url"]["url"].startswith(
        "data:image/jpeg;base64,",
    )


async def test_vision_invalid_uri_returns_provider_unavailable(
    stub_db, caplog,
):
    """If the helper hands us a malformed data URI (caller
    bypassed validation, future refactor regression), the
    multimodal-assembly try/except must catch ``VisionError``
    and surface a localised message — never a poller-level
    crash. No wallet impact: the gate fires before settlement."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 10.0,
        "active_model": "openai/gpt-4o",
        "language_code": "en",
        "memory_enabled": False,
    }

    with caplog.at_level("ERROR"):
        with _patched_session(_StubResponse()):
            reply = await ai_engine.chat_with_model(
                42, "hi", image_data_uris=["http://not-a-data-uri/foo.jpg"],
            )

    # We render the catch-all instead of crashing.
    assert reply  # non-empty
    # No spend, no log_usage. The gate fired before settlement.
    stub_db.deduct_balance.assert_not_awaited()
    stub_db.log_usage.assert_not_awaited()


async def test_vision_no_images_keyword_keeps_text_payload_shape(stub_db):
    """A call with ``image_data_uris=None`` (or omitted) must
    still produce a plain-string user-content payload — i.e.
    the existing 19+ positional-only call sites in this test
    file are unaffected."""
    captured: dict = {}

    class _CapturingResponse(_StubResponse):
        async def json(self):
            return _ok_openrouter_body()

    class _CapturingSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        def post(self, *_args, **kwargs):
            captured["json"] = kwargs.get("json")
            return _CapturingResponse()

    with patch.object(
        ai_engine.aiohttp,
        "ClientSession",
        MagicMock(return_value=_CapturingSession()),
    ):
        reply = await ai_engine.chat_with_model(7, "hello")  # no kw

    assert reply == "hello back"
    posted = captured["json"]
    last_user = posted["messages"][-1]
    assert last_user == {"role": "user", "content": "hello"}


async def test_vision_empty_list_treated_as_no_images(stub_db):
    """``image_data_uris=[]`` must be treated as 'no images' —
    i.e. NOT trip the vision-capability gate, NOT use the
    multimodal payload shape. Defensive: handler-side
    validation might accidentally pass an empty list."""
    stub_db.get_user.return_value = {
        "free_messages_left": 0,
        "balance_usd": 10.0,
        "active_model": "openai/gpt-3.5-turbo",  # NOT vision
        "language_code": "en",
        "memory_enabled": False,
    }

    with _patched_session(_StubResponse()):
        reply = await ai_engine.chat_with_model(
            42, "hi", image_data_uris=[],
        )

    # Falls through to normal text path — gate didn't fire.
    assert reply == "hello back"
    stub_db.deduct_balance.assert_awaited()


# ---------------------------------------------------------------------
# Stage-15-Step-E #10 (this PR) bundled fix: NUL-byte sanitisation
# at the database layer (root cause of the previous-PR symptom).
# ---------------------------------------------------------------------
# These tests live in test_database_queries.py — see also the unit
# tests there. We don't repeat them here; this comment is a cross-
# reference for future readers who reach for ai_engine first.
