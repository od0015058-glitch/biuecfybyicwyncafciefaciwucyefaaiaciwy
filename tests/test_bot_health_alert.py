"""Tests for the Stage-15-Step-F follow-up bot-health alert loop.

These tests exercise the pure pass + the formatter directly without
spinning up the long-running ``_alert_loop`` (cancellation timing of
``asyncio.sleep`` would dominate the test runtime). The pass is the
real surface — the loop is just a forever-while-true wrapper.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

import bot_health_alert as bha
from bot_health import BotStatus, BotStatusLevel


# ---------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_module_state():
    """Each test starts from a clean module-level cache."""
    bha.reset_latest_state_for_tests()
    yield
    bha.reset_latest_state_for_tests()


def _make_bot() -> MagicMock:
    """Stub aiogram Bot with an awaitable send_message."""
    bot = MagicMock()
    bot.send_message = AsyncMock()
    return bot


def _patch_admins(monkeypatch, ids: tuple[int, ...]):
    monkeypatch.setattr(bha, "get_admin_user_ids", lambda: frozenset(ids))


def _patch_signals(
    monkeypatch,
    *,
    inflight: int = 0,
    drops_total: int = 0,
    loop_ticks: dict[str, float] | None = None,
    login_keys: int = 0,
    db_error: str | None = None,
    expected_loops: tuple[str, ...] = (),
):
    """Patch both ``_read_signals`` and ``metrics._LOOP_METRIC_NAMES``.

    Default ``expected_loops=()`` so a test that doesn't care about
    the DEGRADED-from-stale-loop signal doesn't accidentally trip it
    just because the production loop list contains names that have
    not ticked in the test process.
    """
    monkeypatch.setattr(
        bha,
        "_read_signals",
        lambda: (
            inflight,
            drops_total,
            loop_ticks or {},
            login_keys,
            db_error,
        ),
    )
    import metrics

    monkeypatch.setattr(metrics, "_LOOP_METRIC_NAMES", expected_loops)


# ---------------------------------------------------------------------
# env var parsing
# ---------------------------------------------------------------------


def test_get_interval_seconds_uses_default():
    assert bha.get_bot_health_alert_interval_seconds() == 60


def test_get_interval_seconds_honours_env_var(monkeypatch):
    monkeypatch.setenv("BOT_HEALTH_ALERT_INTERVAL_SECONDS", "30")
    assert bha.get_bot_health_alert_interval_seconds() == 30


def test_get_interval_seconds_clamps_below_minimum(monkeypatch):
    monkeypatch.setenv("BOT_HEALTH_ALERT_INTERVAL_SECONDS", "0")
    assert bha.get_bot_health_alert_interval_seconds() == 1


def test_get_interval_seconds_invalid_falls_back(monkeypatch, caplog):
    monkeypatch.setenv("BOT_HEALTH_ALERT_INTERVAL_SECONDS", "notanint")
    with caplog.at_level(logging.ERROR, logger="bot.bot_health_alert"):
        assert bha.get_bot_health_alert_interval_seconds() == 60
    assert any("not an integer" in r.message for r in caplog.records)


# ---------------------------------------------------------------------
# _format_alert_body
# ---------------------------------------------------------------------


def test_alert_body_renders_incident_with_signals():
    status = BotStatus(
        level=BotStatusLevel.UNDER_ATTACK,
        summary="Bot is under attack — 200 IPN drops in window",
        signals=("200 IPN drops in window", "extra signal"),
        score=4,
    )
    body = bha._format_alert_body(status)
    assert "Bot health alert" in body
    assert "under_attack" in body
    assert "200 IPN drops" in body
    assert "/admin/control" in body


def test_alert_body_truncates_signals_above_five():
    status = BotStatus(
        level=BotStatusLevel.DEGRADED,
        summary="Degraded — many stale loops",
        signals=tuple(f"loop_{i} stale" for i in range(10)),
        score=3,
    )
    body = bha._format_alert_body(status)
    # Sanity: the "and N more" tail appears, and we haven't dumped all
    # 10 lines into Telegram.
    assert "5 more" in body
    assert "loop_5" not in body


def test_alert_body_renders_recovery():
    status = BotStatus(
        level=BotStatusLevel.HEALTHY,
        summary="Bot is operating normally",
        signals=(),
        score=1,
    )
    body = bha._format_alert_body(
        status, recovered_from=BotStatusLevel.UNDER_ATTACK
    )
    assert "recovered" in body.lower()
    assert "under_attack" in body
    assert "healthy" in body


# ---------------------------------------------------------------------
# notify_admins_of_health_change
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_notify_skips_when_no_admins_configured(monkeypatch, caplog):
    bot = _make_bot()
    _patch_admins(monkeypatch, ())
    status = BotStatus(
        level=BotStatusLevel.DOWN, summary="DB down", signals=(),
        score=5,
    )
    with caplog.at_level(logging.WARNING, logger="bot.bot_health_alert"):
        sent = await bha.notify_admins_of_health_change(bot, status)
    assert sent == 0
    bot.send_message.assert_not_awaited()
    assert any("ADMIN_USER_IDS is empty" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_notify_sends_to_each_admin(monkeypatch):
    bot = _make_bot()
    _patch_admins(monkeypatch, (111, 222, 333))
    status = BotStatus(
        level=BotStatusLevel.UNDER_ATTACK,
        summary="Attack",
        signals=("200 drops",),
        score=4,
    )
    sent = await bha.notify_admins_of_health_change(bot, status)
    assert sent == 3
    assert bot.send_message.await_count == 3
    targeted = {c.args[0] for c in bot.send_message.await_args_list}
    assert targeted == {111, 222, 333}


@pytest.mark.asyncio
async def test_notify_isolates_blocked_admin(monkeypatch):
    bot = _make_bot()
    _patch_admins(monkeypatch, (111, 222))

    async def send(chat_id, text, **kwargs):
        if chat_id == 111:
            raise TelegramForbiddenError(
                method=MagicMock(), message="blocked"
            )
        return None

    bot.send_message = AsyncMock(side_effect=send)
    status = BotStatus(
        level=BotStatusLevel.DOWN, summary="x", signals=(), score=5,
    )
    sent = await bha.notify_admins_of_health_change(bot, status)
    assert sent == 1


@pytest.mark.asyncio
async def test_notify_isolates_telegram_api_error(monkeypatch, caplog):
    bot = _make_bot()
    _patch_admins(monkeypatch, (111, 222))

    async def send(chat_id, text, **kwargs):
        if chat_id == 111:
            raise TelegramAPIError(method=MagicMock(), message="boom")
        return None

    bot.send_message = AsyncMock(side_effect=send)
    status = BotStatus(
        level=BotStatusLevel.DOWN, summary="x", signals=(), score=5,
    )
    with caplog.at_level(logging.ERROR, logger="bot.bot_health_alert"):
        sent = await bha.notify_admins_of_health_change(bot, status)
    assert sent == 1
    assert any(
        "Failed to send" in r.message for r in caplog.records
    )


# ---------------------------------------------------------------------
# run_bot_health_alert_pass
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pass_does_not_dm_when_idle(monkeypatch):
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    _patch_signals(monkeypatch)
    state = bha.AlertLoopState()
    sent = await bha.run_bot_health_alert_pass(bot, state=state)
    assert sent == 0
    bot.send_message.assert_not_awaited()
    assert state.last_observed_level is BotStatusLevel.IDLE
    assert state.last_dispatched_level is None


@pytest.mark.asyncio
async def test_pass_dms_admins_on_first_under_attack(monkeypatch):
    """A fresh process whose first tick already sees a flood (e.g. a
    burst happened during the boot window) must DM admins immediately
    — the rate-window delta on the first tick is the absolute
    since-boot count, by design."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    _patch_signals(monkeypatch, drops_total=200)
    state = bha.AlertLoopState()
    sent = await bha.run_bot_health_alert_pass(bot, state=state)
    assert sent == 1
    bot.send_message.assert_awaited_once()
    body = bot.send_message.await_args.args[1]
    assert "under_attack" in body
    assert state.last_dispatched_level is BotStatusLevel.UNDER_ATTACK
    assert state.previous_ipn_drops_total == 200


@pytest.mark.asyncio
async def test_pass_does_not_dm_when_drops_total_grew_within_window(
    monkeypatch,
):
    """The rate-window classifier must distinguish 200 drops in one
    tick (UNDER_ATTACK) from 200 drops accumulated over 10 ticks
    (HEALTHY/IDLE). After the first tick primes the previous total,
    a second tick where the running total grew by less than the
    threshold must NOT escalate."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    state = bha.AlertLoopState(previous_ipn_drops_total=300)

    # Tick 1: drops_total grew from 300 → 320. delta=20 < threshold 100
    # → no DM, no escalation.
    _patch_signals(monkeypatch, drops_total=320)
    sent = await bha.run_bot_health_alert_pass(bot, state=state)
    assert sent == 0
    bot.send_message.assert_not_awaited()
    assert state.last_dispatched_level is None
    assert state.previous_ipn_drops_total == 320


@pytest.mark.asyncio
async def test_pass_does_not_re_dm_on_consecutive_same_level(monkeypatch):
    """An incident is one DM per level per hour-anchor — not one per
    tick."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    state = bha.AlertLoopState(previous_ipn_drops_total=0)

    # Tick 1: 200 drops in window → UNDER_ATTACK + DM.
    _patch_signals(monkeypatch, drops_total=200)
    await bha.run_bot_health_alert_pass(bot, state=state)
    assert bot.send_message.await_count == 1

    # Tick 2: another small bump but level is still UNDER_ATTACK
    # because state was reset and now drops_total grew by 200 again.
    # The hour-anchor key matches the previous → no duplicate DM.
    _patch_signals(monkeypatch, drops_total=400)
    await bha.run_bot_health_alert_pass(bot, state=state)
    assert bot.send_message.await_count == 1, (
        "Same-level same-anchor must not re-DM"
    )


@pytest.mark.asyncio
async def test_pass_re_dms_on_level_escalation(monkeypatch):
    """DEGRADED → DOWN must re-DM even within the same anchor."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    state = bha.AlertLoopState()

    # Tick 1: stale loop → DEGRADED + DM.
    _patch_signals(
        monkeypatch,
        loop_ticks={"fx_refresh": 1.0},  # ancient
        expected_loops=("fx_refresh",),
    )
    await bha.run_bot_health_alert_pass(bot, state=state)
    assert state.last_dispatched_level is BotStatusLevel.DEGRADED
    assert bot.send_message.await_count == 1

    # Tick 2: db_error fires → DOWN + DM (level escalation).
    _patch_signals(
        monkeypatch,
        loop_ticks={"fx_refresh": 1.0},
        db_error="boom",
        expected_loops=("fx_refresh",),
    )
    await bha.run_bot_health_alert_pass(bot, state=state)
    assert state.last_dispatched_level is BotStatusLevel.DOWN
    assert bot.send_message.await_count == 2


@pytest.mark.asyncio
async def test_pass_dms_on_recovery_back_to_healthy(monkeypatch):
    """A bad → good transition must DM 'recovered' once and clear
    the dispatched level."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    state = bha.AlertLoopState()

    # Tick 1: trip UNDER_ATTACK.
    _patch_signals(monkeypatch, drops_total=200)
    await bha.run_bot_health_alert_pass(bot, state=state)
    assert state.last_dispatched_level is BotStatusLevel.UNDER_ATTACK
    assert bot.send_message.await_count == 1

    # Tick 2: drops go quiet → HEALTHY → recovery DM.
    _patch_signals(monkeypatch, drops_total=200)  # delta=0
    await bha.run_bot_health_alert_pass(bot, state=state)
    assert bot.send_message.await_count == 2
    body = bot.send_message.await_args.args[1]
    assert "recovered" in body.lower()
    assert "under_attack" in body
    assert state.last_dispatched_level is None  # cleared

    # Tick 3: still quiet — must NOT spam recoveries.
    await bha.run_bot_health_alert_pass(bot, state=state)
    assert bot.send_message.await_count == 2


@pytest.mark.asyncio
async def test_pass_busy_does_not_alert_admins(monkeypatch):
    """BUSY is not in the bad-levels set — a heavy-traffic surge that
    the bot is correctly handling should NOT spam the operator."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    _patch_signals(monkeypatch, inflight=200)
    state = bha.AlertLoopState()
    sent = await bha.run_bot_health_alert_pass(bot, state=state)
    assert sent == 0
    bot.send_message.assert_not_awaited()
    assert state.last_observed_level is BotStatusLevel.BUSY


@pytest.mark.asyncio
async def test_pass_updates_module_level_latest_state(monkeypatch):
    """The panel reads ``latest_observed_recent_drops()`` to display
    the loop's view. The pass must populate it after every tick."""
    assert bha.latest_observed_status() is None
    assert bha.latest_observed_recent_drops() == 0

    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    _patch_signals(monkeypatch, drops_total=42)
    state = bha.AlertLoopState()
    await bha.run_bot_health_alert_pass(bot, state=state)
    cached = bha.latest_observed_status()
    assert cached is not None
    assert cached.level is state.last_observed_level
    # First-tick delta is the since-boot count.
    assert bha.latest_observed_recent_drops() == 42


@pytest.mark.asyncio
async def test_pass_negative_delta_clamps_to_zero(monkeypatch):
    """If the underlying counters are reset (process restart of a
    payments module via reload) ``previous`` may end up larger than
    ``current``. Guard against a negative delta tripping a phantom
    UNDER_ATTACK."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    state = bha.AlertLoopState(previous_ipn_drops_total=500)
    _patch_signals(monkeypatch, drops_total=10)
    await bha.run_bot_health_alert_pass(bot, state=state)
    assert state.last_observed_recent_drops == 0
    bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_pass_classifier_failure_does_not_crash_loop(monkeypatch):
    """A signal-collection helper raising must not propagate out of
    the pass — the loop's `try/except Exception` already handles it,
    but per-helper isolation makes the failure recoverable on the
    next tick. We pin this contract."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))

    def boom():
        raise RuntimeError("sim")

    monkeypatch.setattr(bha, "_read_signals", boom)
    state = bha.AlertLoopState()

    with pytest.raises(RuntimeError):
        await bha.run_bot_health_alert_pass(bot, state=state)


# ---------------------------------------------------------------------
# integration with web_admin / metrics
# ---------------------------------------------------------------------


def test_latest_observed_recent_drops_returns_zero_until_loop_ticks():
    assert bha.latest_observed_recent_drops() == 0


@pytest.mark.asyncio
async def test_latest_observed_recent_drops_returns_loop_value(monkeypatch):
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    _patch_signals(monkeypatch, drops_total=17)
    state = bha.AlertLoopState(previous_ipn_drops_total=10)
    await bha.run_bot_health_alert_pass(bot, state=state)
    assert bha.latest_observed_recent_drops() == 7
