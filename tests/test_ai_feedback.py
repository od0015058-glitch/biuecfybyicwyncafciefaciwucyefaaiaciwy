"""Tests for the Stage-16 row 19 AI-feedback module.

Three layers:

* Callback-data + keyboard helpers — pure functions, no I/O.
* Database methods (``record_usage_feedback`` /
  ``get_recent_feedback_rates``) — driven via the existing
  ``_PoolStub`` connection mock pattern from
  ``tests/test_database_queries.py``.
* Alert layer — :func:`run_dissatisfaction_check`,
  :func:`_filter_alertable`, latch behaviour, admin DM
  fan-out.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

import ai_feedback
import database as database_module


# ---------------------------------------------------------------------
# Pool / connection stubs (keep parity with test_database_queries.py)
# ---------------------------------------------------------------------


class _PoolStub:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        outer = self

        class _Ctx:
            async def __aenter__(self_inner):
                return outer.connection

            async def __aexit__(self_inner, exc_type, exc, tb):
                return False

        return _Ctx()


def _make_conn():
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=0)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value="UPDATE 0")
    return conn


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch):
    """Each test starts with a clean alert latch + clean env vars
    so a knob set in test A doesn't leak into test B.
    """
    ai_feedback.reset_for_tests()
    for k in (
        "AI_FEEDBACK_ENABLED",
        "AI_FEEDBACK_ALERT_ENABLED",
        "AI_FEEDBACK_DISSATISFACTION_THRESHOLD_RATIO",
        "AI_FEEDBACK_DISSATISFACTION_WINDOW_SECONDS",
        "AI_FEEDBACK_MIN_SAMPLES",
        "AI_FEEDBACK_LOOP_INTERVAL_SECONDS",
        "AI_FEEDBACK_ALERT_COOLDOWN_SECONDS",
    ):
        monkeypatch.delenv(k, raising=False)
    yield
    ai_feedback.reset_for_tests()


# =====================================================================
# Knob plumbing
# =====================================================================


def test_knobs_have_documented_defaults():
    assert ai_feedback.dissatisfaction_threshold_ratio() == 0.30
    assert ai_feedback.dissatisfaction_window_seconds() == 3600
    assert ai_feedback.min_samples() == 10
    assert ai_feedback.loop_interval_seconds() == 300
    assert ai_feedback.alert_cooldown_seconds() == 3600
    assert ai_feedback.is_enabled() is True
    assert ai_feedback.alert_enabled() is True


@pytest.mark.parametrize(
    "raw,expected",
    [("0", False), ("false", False), ("no", False), ("off", False),
     ("1", True), ("true", True), ("yes", True), ("on", True)],
)
def test_is_enabled_parses_typical_truthy_falsy(monkeypatch, raw, expected):
    monkeypatch.setenv("AI_FEEDBACK_ENABLED", raw)
    assert ai_feedback.is_enabled() is expected


def test_threshold_ratio_clamps_to_unit_interval(monkeypatch):
    monkeypatch.setenv("AI_FEEDBACK_DISSATISFACTION_THRESHOLD_RATIO", "1.5")
    assert ai_feedback.dissatisfaction_threshold_ratio() == 1.0
    monkeypatch.setenv("AI_FEEDBACK_DISSATISFACTION_THRESHOLD_RATIO", "-0.2")
    assert ai_feedback.dissatisfaction_threshold_ratio() == 0.0


def test_window_seconds_clamps_to_floor(monkeypatch):
    monkeypatch.setenv("AI_FEEDBACK_DISSATISFACTION_WINDOW_SECONDS", "5")
    # Floor is 60 — a 5 s window would chase tail noise.
    assert ai_feedback.dissatisfaction_window_seconds() == 60


def test_min_samples_clamps_to_one(monkeypatch):
    """A floor of 1 prevents ZeroDivision on the rate aggregate.
    Operators wanting the alert to be quiet should raise the
    *threshold*, not lower the minimum sample count.
    """
    monkeypatch.setenv("AI_FEEDBACK_MIN_SAMPLES", "0")
    assert ai_feedback.min_samples() == 1
    monkeypatch.setenv("AI_FEEDBACK_MIN_SAMPLES", "-3")
    assert ai_feedback.min_samples() == 1


def test_loop_interval_clamps_to_thirty_seconds(monkeypatch):
    """A 30 s floor keeps a misconfigured operator from hammering
    the DB on the dissatisfaction-rate aggregate every second.
    """
    monkeypatch.setenv("AI_FEEDBACK_LOOP_INTERVAL_SECONDS", "1")
    assert ai_feedback.loop_interval_seconds() == 30


def test_invalid_int_falls_back_to_default(monkeypatch, caplog):
    import logging

    monkeypatch.setenv("AI_FEEDBACK_MIN_SAMPLES", "not-an-int")
    with caplog.at_level(logging.WARNING, logger="bot.ai_feedback"):
        result = ai_feedback.min_samples()
    assert result == 10  # default
    assert any("invalid integer" in rec.message for rec in caplog.records)


# =====================================================================
# Callback-data + keyboard helpers
# =====================================================================


def test_build_feedback_keyboard_emits_two_buttons():
    kb = ai_feedback.build_feedback_keyboard(42, enabled=True)
    assert kb is not None
    rows = kb.inline_keyboard
    assert len(rows) == 1
    assert len(rows[0]) == 2
    assert rows[0][0].callback_data == "fbp:42"
    assert rows[0][1].callback_data == "fbn:42"
    # Emoji on the buttons themselves so even a non-localised
    # client renders the affordance.
    assert "👍" in rows[0][0].text
    assert "👎" in rows[0][1].text


def test_build_feedback_keyboard_returns_none_when_disabled():
    assert ai_feedback.build_feedback_keyboard(42, enabled=False) is None


def test_build_feedback_keyboard_returns_none_for_invalid_log_id():
    """Free-tier turn (no usage row was inserted) → no keyboard."""
    assert ai_feedback.build_feedback_keyboard(None, enabled=True) is None
    assert ai_feedback.build_feedback_keyboard(0, enabled=True) is None
    assert ai_feedback.build_feedback_keyboard(-7, enabled=True) is None


def test_build_feedback_keyboard_consults_env_when_enabled_is_none(monkeypatch):
    monkeypatch.setenv("AI_FEEDBACK_ENABLED", "false")
    assert ai_feedback.build_feedback_keyboard(99) is None
    monkeypatch.setenv("AI_FEEDBACK_ENABLED", "true")
    kb = ai_feedback.build_feedback_keyboard(99)
    assert kb is not None


def test_callback_data_round_trips_through_parse():
    kb = ai_feedback.build_feedback_keyboard(987654321, enabled=True)
    assert kb is not None
    pos = kb.inline_keyboard[0][0].callback_data
    neg = kb.inline_keyboard[0][1].callback_data
    assert ai_feedback.parse_feedback_callback(pos) == ("positive", 987654321)
    assert ai_feedback.parse_feedback_callback(neg) == ("negative", 987654321)


def test_callback_data_fits_telegram_64_byte_cap():
    """A 64-bit log_id is at most 19 digits (``2**63-1`` is 19
    chars). ``fbn:`` + 19 = 23 bytes — well under Telegram's 64.
    """
    kb = ai_feedback.build_feedback_keyboard(2**63 - 1, enabled=True)
    assert kb is not None
    for btn in kb.inline_keyboard[0]:
        assert len(btn.callback_data.encode("utf-8")) <= 64


@pytest.mark.parametrize(
    "data",
    [
        None, "", "fbp", "fbp:", "fbp:abc", "fbp:-5", "fbp:0",
        "wrong:42", "FBP:42",  # case-sensitive
    ],
)
def test_parse_feedback_callback_rejects_malformed(data):
    assert ai_feedback.parse_feedback_callback(data) is None


# =====================================================================
# Database.record_usage_feedback
# =====================================================================


@pytest.mark.asyncio
async def test_record_usage_feedback_rejects_bad_slug():
    db = database_module.Database()
    db.pool = _PoolStub(_make_conn())
    with pytest.raises(ValueError, match="positive.*negative"):
        await db.record_usage_feedback(
            log_id=1, telegram_id=42, feedback="meh"
        )


@pytest.mark.asyncio
async def test_record_usage_feedback_returns_true_on_one_row_updated():
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="UPDATE 1")
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    ok = await db.record_usage_feedback(
        log_id=42, telegram_id=99, feedback="positive"
    )
    assert ok is True
    conn.execute.assert_awaited_once()
    sql = conn.execute.call_args.args[0]
    assert "UPDATE usage_logs" in sql
    assert "feedback IS NULL" in sql  # first-tap-wins idempotency
    assert "telegram_id" in sql  # owner check


@pytest.mark.asyncio
async def test_record_usage_feedback_returns_false_when_no_row_matched():
    """Owner mismatch / already-rated / log_id deleted by retention."""
    conn = _make_conn()
    conn.execute = AsyncMock(return_value="UPDATE 0")
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    ok = await db.record_usage_feedback(
        log_id=42, telegram_id=99, feedback="negative"
    )
    assert ok is False


# =====================================================================
# Database.get_recent_feedback_rates
# =====================================================================


@pytest.mark.asyncio
async def test_get_recent_feedback_rates_aggregates_per_model():
    conn = _make_conn()
    conn.fetch = AsyncMock(
        return_value=[
            {"model": "openai/gpt-4o", "total": 20, "negative": 8},
            {"model": "anthropic/claude-3-haiku", "total": 50, "negative": 5},
        ]
    )
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    rates = await db.get_recent_feedback_rates(
        window_seconds=3600, min_samples=10
    )
    assert len(rates) == 2
    assert rates[0]["model"] == "openai/gpt-4o"
    assert rates[0]["negative_rate"] == pytest.approx(0.4)
    assert rates[1]["model"] == "anthropic/claude-3-haiku"
    assert rates[1]["negative_rate"] == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_get_recent_feedback_rates_clamps_invalid_inputs():
    """Window < 1 s and min_samples < 1 are clamped, not rejected,
    so a misconfigured caller doesn't crash the alert loop.
    """
    conn = _make_conn()
    db = database_module.Database()
    db.pool = _PoolStub(conn)
    # No exception raised.
    await db.get_recent_feedback_rates(window_seconds=0, min_samples=0)
    await db.get_recent_feedback_rates(window_seconds=-100, min_samples=-5)
    # 2 calls observed.
    assert conn.fetch.await_count == 2


# =====================================================================
# Alert filtering (sync, no DB)
# =====================================================================


def _row(model, total, negative):
    rate = (negative / total) if total else 0.0
    return {
        "model": model, "total": total, "negative": negative,
        "negative_rate": rate,
    }


def test_filter_alertable_drops_below_threshold():
    rows = [_row("a", 100, 10), _row("b", 100, 50)]
    alertable = ai_feedback._filter_alertable(
        rows, threshold_ratio=0.30, min_n=10, now=0.0, cooldown=3600
    )
    assert [r["model"] for r in alertable] == ["b"]


def test_filter_alertable_drops_below_min_samples():
    rows = [_row("only-3-samples", 3, 3), _row("plenty", 50, 20)]
    alertable = ai_feedback._filter_alertable(
        rows, threshold_ratio=0.30, min_n=10, now=0.0, cooldown=3600
    )
    assert [r["model"] for r in alertable] == ["plenty"]


def test_filter_alertable_skips_recently_alerted_models():
    """Latch behaviour: a model already alerted within ``cooldown``
    is excluded from the alertable list.
    """
    rows = [_row("recent", 50, 30)]
    ai_feedback._LAST_ALERT_AT["recent"] = 100.0
    alertable = ai_feedback._filter_alertable(
        rows, threshold_ratio=0.30, min_n=10, now=200.0, cooldown=3600
    )
    assert alertable == []


def test_filter_alertable_re_alerts_after_cooldown():
    rows = [_row("after-cooldown", 50, 30)]
    ai_feedback._LAST_ALERT_AT["after-cooldown"] = 100.0
    alertable = ai_feedback._filter_alertable(
        rows, threshold_ratio=0.30, min_n=10, now=4000.0, cooldown=3600
    )
    assert [r["model"] for r in alertable] == ["after-cooldown"]


# =====================================================================
# run_dissatisfaction_check
# =====================================================================


def _stub_db(rows):
    db = MagicMock()
    db.get_recent_feedback_rates = AsyncMock(return_value=rows)
    return db


@pytest.mark.asyncio
async def test_run_dissatisfaction_check_dms_admins_for_breaching_models(
    monkeypatch,
):
    monkeypatch.setattr(ai_feedback, "get_admin_user_ids", lambda: [111, 222])
    bot = MagicMock()
    bot.send_message = AsyncMock()
    db = _stub_db([_row("bad", 50, 30)])
    sent = await ai_feedback.run_dissatisfaction_check(bot, db=db, now=1000.0)
    assert sent == 2
    assert bot.send_message.await_count == 2
    # The body names the model and the rate.
    body = bot.send_message.await_args.args[1]
    assert "bad" in body
    assert "60%" in body or "60" in body
    # Latch advanced for that model so a second tick within the
    # cooldown is a no-op.
    assert "bad" in ai_feedback._LAST_ALERT_AT


@pytest.mark.asyncio
async def test_run_dissatisfaction_check_is_no_op_below_threshold(monkeypatch):
    monkeypatch.setattr(ai_feedback, "get_admin_user_ids", lambda: [111])
    bot = MagicMock()
    bot.send_message = AsyncMock()
    db = _stub_db([_row("calm", 50, 5)])  # 10% — below 30% threshold
    sent = await ai_feedback.run_dissatisfaction_check(bot, db=db, now=0.0)
    assert sent == 0
    bot.send_message.assert_not_awaited()
    # Latch NOT advanced — a future spike should still alert.
    assert "calm" not in ai_feedback._LAST_ALERT_AT


@pytest.mark.asyncio
async def test_run_dissatisfaction_check_dedupes_within_cooldown(monkeypatch):
    monkeypatch.setattr(ai_feedback, "get_admin_user_ids", lambda: [111])
    bot = MagicMock()
    bot.send_message = AsyncMock()
    db = _stub_db([_row("hot", 50, 30)])

    # Tick 1: alert fires.
    sent = await ai_feedback.run_dissatisfaction_check(bot, db=db, now=0.0)
    assert sent == 1

    # Tick 2 within the cooldown: no second alert, even though
    # the rate is still breaching.
    sent2 = await ai_feedback.run_dissatisfaction_check(bot, db=db, now=300.0)
    assert sent2 == 0
    assert bot.send_message.await_count == 1


@pytest.mark.asyncio
async def test_run_dissatisfaction_check_re_alerts_after_cooldown(monkeypatch):
    monkeypatch.setattr(ai_feedback, "get_admin_user_ids", lambda: [111])
    monkeypatch.setenv("AI_FEEDBACK_ALERT_COOLDOWN_SECONDS", "10")
    bot = MagicMock()
    bot.send_message = AsyncMock()
    db = _stub_db([_row("flaky", 50, 30)])

    await ai_feedback.run_dissatisfaction_check(bot, db=db, now=0.0)
    # Past the (10s) cooldown — re-alert.
    sent2 = await ai_feedback.run_dissatisfaction_check(bot, db=db, now=20.0)
    assert sent2 == 1
    assert bot.send_message.await_count == 2


@pytest.mark.asyncio
async def test_run_dissatisfaction_check_does_not_advance_latch_on_failed_dm(
    monkeypatch,
):
    """A Telegram failure on every admin DM means ``sent == 0``,
    which means the latch is NOT advanced — the next tick should
    re-attempt rather than silently dropping the alert.
    """
    from aiogram.exceptions import TelegramAPIError

    monkeypatch.setattr(ai_feedback, "get_admin_user_ids", lambda: [111])
    bot = MagicMock()
    bot.send_message = AsyncMock(side_effect=TelegramAPIError(method="x", message="boom"))
    db = _stub_db([_row("retry", 50, 30)])

    sent = await ai_feedback.run_dissatisfaction_check(bot, db=db, now=0.0)
    assert sent == 0
    assert "retry" not in ai_feedback._LAST_ALERT_AT


@pytest.mark.asyncio
async def test_run_dissatisfaction_check_skips_when_disabled(monkeypatch):
    monkeypatch.setenv("AI_FEEDBACK_ALERT_ENABLED", "false")
    monkeypatch.setattr(ai_feedback, "get_admin_user_ids", lambda: [111])
    bot = MagicMock()
    bot.send_message = AsyncMock()
    db = _stub_db([_row("loud", 50, 30)])
    sent = await ai_feedback.run_dissatisfaction_check(bot, db=db, now=0.0)
    assert sent == 0
    bot.send_message.assert_not_awaited()
    db.get_recent_feedback_rates.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_dissatisfaction_check_swallows_db_failure(monkeypatch, caplog):
    import logging

    monkeypatch.setattr(ai_feedback, "get_admin_user_ids", lambda: [111])
    bot = MagicMock()
    bot.send_message = AsyncMock()
    db = MagicMock()
    db.get_recent_feedback_rates = AsyncMock(side_effect=RuntimeError("DB down"))

    with caplog.at_level(logging.ERROR, logger="bot.ai_feedback"):
        sent = await ai_feedback.run_dissatisfaction_check(bot, db=db, now=0.0)
    assert sent == 0
    bot.send_message.assert_not_awaited()
    assert any(
        "get_recent_feedback_rates failed" in rec.message
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_run_dissatisfaction_check_handles_blocked_admin(monkeypatch):
    """One blocked admin must not stop another admin's DM.

    Per-admin fault isolation matches the abuse-detection alert
    fan-out.
    """
    from aiogram.exceptions import TelegramForbiddenError

    monkeypatch.setattr(ai_feedback, "get_admin_user_ids", lambda: [111, 222])
    bot = MagicMock()

    async def _send(admin_id, text, **_kw):
        if admin_id == 111:
            raise TelegramForbiddenError(method="x", message="blocked")

    bot.send_message = AsyncMock(side_effect=_send)
    db = _stub_db([_row("split", 50, 30)])
    sent = await ai_feedback.run_dissatisfaction_check(bot, db=db, now=0.0)
    # Admin 222 still received the DM; admin 111's block was
    # logged-and-swallowed.
    assert sent == 1


@pytest.mark.asyncio
async def test_run_dissatisfaction_check_warns_when_no_admins_configured(
    monkeypatch, caplog
):
    import logging

    monkeypatch.setattr(ai_feedback, "get_admin_user_ids", lambda: [])
    bot = MagicMock()
    bot.send_message = AsyncMock()
    db = _stub_db([_row("orphan", 50, 30)])

    with caplog.at_level(logging.WARNING, logger="bot.ai_feedback"):
        sent = await ai_feedback.run_dissatisfaction_check(
            bot, db=db, now=0.0
        )
    assert sent == 0
    assert any(
        "ADMIN_USER_IDS is empty" in rec.message for rec in caplog.records
    )


# =====================================================================
# start_dissatisfaction_alert_task — lifecycle smoke test
# =====================================================================


@pytest.mark.asyncio
async def test_start_dissatisfaction_alert_task_returns_cancellable_task():
    """The returned task must be cancellable without leaking a
    warning. Mirrors the contract pinned for
    :func:`pending_alert.start_pending_alert_task`.
    """
    bot = MagicMock()
    bot.send_message = AsyncMock()
    task = ai_feedback.start_dissatisfaction_alert_task(bot)
    try:
        # Let the loop reach its first ``asyncio.sleep`` so the
        # cancellation lands on a clean suspend point.
        await asyncio.sleep(0)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
