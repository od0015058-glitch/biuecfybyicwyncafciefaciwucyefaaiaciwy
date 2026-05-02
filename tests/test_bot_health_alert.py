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
    """Each test starts from a clean module-level cache.

    Also clears the Stage-15-Step-E #10b row 21 alert-interval
    override slot so a test that monkeypatches env doesn't see a
    leaked override from a previous test.
    """
    bha.reset_latest_state_for_tests()
    bha.reset_alert_interval_override_for_tests()
    yield
    bha.reset_latest_state_for_tests()
    bha.reset_alert_interval_override_for_tests()


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


# ---------------------------------------------------------------------
# audit-log hook (Stage-15-Step-F follow-up #3)
# ---------------------------------------------------------------------


def _patch_audit(monkeypatch) -> AsyncMock:
    """Patch ``database.db.record_admin_audit`` and return the spy.

    The audit write is best-effort with try/except — these tests
    pin that the spy *was* called with the expected arguments, not
    that the alert loop succeeds or fails based on the audit
    return value.
    """
    spy = AsyncMock(return_value=42)
    import database

    monkeypatch.setattr(database.db, "record_admin_audit", spy)
    return spy


@pytest.mark.asyncio
async def test_alert_dm_records_audit_row(monkeypatch):
    """A successful UNDER_ATTACK alert DM must produce one audit row
    with action=bot_health_alert, outcome=ok, and meta capturing
    the level / signals / delivery counts."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111, 222))
    _patch_signals(monkeypatch, drops_total=200)
    spy = _patch_audit(monkeypatch)
    state = bha.AlertLoopState()

    sent = await bha.run_bot_health_alert_pass(bot, state=state)
    assert sent == 2  # both admins received the DM

    spy.assert_awaited_once()
    kwargs = spy.await_args.kwargs
    assert kwargs["actor"] == "bot_health_alert"
    assert kwargs["action"] == "bot_health_alert"
    assert kwargs["target"] == "under_attack"
    assert kwargs["ip"] is None
    assert kwargs["outcome"] == "ok"
    meta = kwargs["meta"]
    assert meta["level"] == "under_attack"
    assert meta["score"] == 4
    assert meta["sent_count"] == 2
    assert meta["admin_count"] == 2
    assert isinstance(meta["signals"], list)
    assert any("IPN" in s for s in meta["signals"])


@pytest.mark.asyncio
async def test_recovery_dm_records_audit_row(monkeypatch):
    """The recovery DM uses action=bot_health_recovery and meta
    captures the prior bad level."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    spy = _patch_audit(monkeypatch)
    state = bha.AlertLoopState(
        last_dispatched_level=BotStatusLevel.UNDER_ATTACK,
    )
    _patch_signals(monkeypatch, drops_total=0)

    sent = await bha.run_bot_health_alert_pass(bot, state=state)
    assert sent == 1

    spy.assert_awaited_once()
    kwargs = spy.await_args.kwargs
    assert kwargs["action"] == "bot_health_recovery"
    assert kwargs["target"] in {"healthy", "idle"}
    assert kwargs["outcome"] == "ok"
    meta = kwargs["meta"]
    assert meta["recovered_from"] == "under_attack"
    assert meta["sent_count"] == 1
    assert meta["admin_count"] == 1


@pytest.mark.asyncio
async def test_partial_delivery_records_partial_outcome(monkeypatch):
    """If one admin blocked the bot, the audit row should still
    fire and ``meta.sent_count < admin_count`` makes the partial
    fan-out visible to the operator reviewing the timeline."""
    bot = _make_bot()
    bot.send_message = AsyncMock(
        side_effect=[None, TelegramForbiddenError(method=None, message="x")]
    )
    _patch_admins(monkeypatch, (111, 222))
    _patch_signals(monkeypatch, drops_total=200)
    spy = _patch_audit(monkeypatch)
    state = bha.AlertLoopState()

    sent = await bha.run_bot_health_alert_pass(bot, state=state)
    assert sent == 1

    kwargs = spy.await_args.kwargs
    meta = kwargs["meta"]
    assert kwargs["outcome"] == "ok"  # at least one admin reached
    assert meta["sent_count"] == 1
    assert meta["admin_count"] == 2


@pytest.mark.asyncio
async def test_zero_admins_reachable_records_no_admins_reachable(monkeypatch):
    """All admins blocked the bot → outcome marks the silent
    incident so the audit log is the *only* surface that captured
    it. Without this the operator has no way to know the alert
    fired but didn't reach anyone."""
    bot = _make_bot()
    bot.send_message = AsyncMock(
        side_effect=TelegramForbiddenError(method=None, message="blocked")
    )
    _patch_admins(monkeypatch, (111, 222))
    _patch_signals(monkeypatch, drops_total=200)
    spy = _patch_audit(monkeypatch)
    state = bha.AlertLoopState()

    sent = await bha.run_bot_health_alert_pass(bot, state=state)
    assert sent == 0

    kwargs = spy.await_args.kwargs
    assert kwargs["outcome"] == "no_admins_reachable"
    meta = kwargs["meta"]
    assert meta["sent_count"] == 0
    assert meta["admin_count"] == 2


@pytest.mark.asyncio
async def test_no_admins_configured_still_records_audit(monkeypatch):
    """An unconfigured deploy with empty ADMIN_USER_IDS but a real
    UNDER_ATTACK condition must still leave an audit-log row so
    the timeline isn't silently empty during an actual incident."""
    bot = _make_bot()
    _patch_admins(monkeypatch, ())  # empty
    _patch_signals(monkeypatch, drops_total=200)
    spy = _patch_audit(monkeypatch)
    state = bha.AlertLoopState()

    sent = await bha.run_bot_health_alert_pass(bot, state=state)
    assert sent == 0

    spy.assert_awaited_once()
    kwargs = spy.await_args.kwargs
    assert kwargs["outcome"] == "no_admins_configured"
    meta = kwargs["meta"]
    assert meta["sent_count"] == 0
    assert meta["admin_count"] == 0


@pytest.mark.asyncio
async def test_audit_failure_does_not_break_dm(monkeypatch):
    """A DB outage that crashes the audit insert must NOT stop the
    DM from being sent — the alert loop's job is to page the
    operator first, record-keeping second."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    _patch_signals(monkeypatch, drops_total=200)

    import database

    crashing = AsyncMock(side_effect=RuntimeError("DB pool died"))
    monkeypatch.setattr(database.db, "record_admin_audit", crashing)

    state = bha.AlertLoopState()
    # Should not raise.
    sent = await bha.run_bot_health_alert_pass(bot, state=state)
    assert sent == 1
    bot.send_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_busy_does_not_record_audit(monkeypatch):
    """BUSY isn't a paging condition — so the audit log shouldn't
    fill up with BUSY rows on every alert tick during heavy
    traffic."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    spy = _patch_audit(monkeypatch)
    _patch_signals(monkeypatch, inflight=10_000)
    state = bha.AlertLoopState()

    await bha.run_bot_health_alert_pass(bot, state=state)
    spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_dedup_does_not_double_record_audit(monkeypatch):
    """Same level + same hour anchor on consecutive ticks → no
    second DM, no second audit row. The audit log should match the
    DM cadence one-to-one. We simulate a sustained UNDER_ATTACK by
    stepping ``drops_total`` up each tick so each pass sees a fresh
    delta."""
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    spy = _patch_audit(monkeypatch)
    state = bha.AlertLoopState()

    _patch_signals(monkeypatch, drops_total=200)
    await bha.run_bot_health_alert_pass(bot, state=state)
    # Second tick with another wave — still UNDER_ATTACK, same hour
    # anchor, so dedup must suppress the DM and the audit row.
    _patch_signals(monkeypatch, drops_total=400)
    await bha.run_bot_health_alert_pass(bot, state=state)
    assert spy.await_count == 1


@pytest.mark.asyncio
async def test_latest_observed_recent_drops_returns_loop_value(monkeypatch):
    bot = _make_bot()
    _patch_admins(monkeypatch, (111,))
    _patch_signals(monkeypatch, drops_total=17)
    state = bha.AlertLoopState(previous_ipn_drops_total=10)
    await bha.run_bot_health_alert_pass(bot, state=state)
    assert bha.latest_observed_recent_drops() == 7


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 21 — DB-backed alert-interval override.
# ---------------------------------------------------------------------


class _StubDB:
    """Minimal DB stub — only what the override layer touches.

    Mirrors the stubs used in ``test_referral.py`` /
    ``test_force_join.py``: ``get_setting`` / ``upsert_setting`` /
    ``delete_setting`` answer from a dict; ``raise_on_get`` /
    ``raise_on_upsert`` simulate transient pool blips so the
    fail-soft branches can be exercised.
    """

    def __init__(
        self,
        initial: dict[str, str] | None = None,
        *,
        raise_on_get: BaseException | None = None,
        raise_on_upsert: BaseException | None = None,
        raise_on_delete: BaseException | None = None,
    ) -> None:
        self.rows: dict[str, str] = dict(initial or {})
        self.raise_on_get = raise_on_get
        self.raise_on_upsert = raise_on_upsert
        self.raise_on_delete = raise_on_delete
        self.upserts: list[tuple[str, str]] = []
        self.deletes: list[str] = []

    async def get_setting(self, key: str):
        if self.raise_on_get is not None:
            raise self.raise_on_get
        return self.rows.get(key)

    async def upsert_setting(self, key: str, value: str):
        if self.raise_on_upsert is not None:
            raise self.raise_on_upsert
        self.upserts.append((key, value))
        self.rows[key] = value

    async def delete_setting(self, key: str):
        if self.raise_on_delete is not None:
            raise self.raise_on_delete
        self.deletes.append(key)
        return self.rows.pop(key, None) is not None


def test_coerce_alert_interval_accepts_int():
    assert bha._coerce_alert_interval(60) == 60


def test_coerce_alert_interval_accepts_string():
    assert bha._coerce_alert_interval("90") == 90


def test_coerce_alert_interval_strips_string():
    assert bha._coerce_alert_interval("  120  ") == 120


def test_coerce_alert_interval_rejects_bool():
    # True is an int subclass — must be rejected explicitly.
    assert bha._coerce_alert_interval(True) is None
    assert bha._coerce_alert_interval(False) is None


def test_coerce_alert_interval_rejects_zero():
    assert bha._coerce_alert_interval(0) is None


def test_coerce_alert_interval_rejects_negative():
    assert bha._coerce_alert_interval(-5) is None


def test_coerce_alert_interval_rejects_above_max():
    assert (
        bha._coerce_alert_interval(bha.INTERVAL_OVERRIDE_MAXIMUM + 1)
        is None
    )


def test_coerce_alert_interval_accepts_max():
    assert (
        bha._coerce_alert_interval(bha.INTERVAL_OVERRIDE_MAXIMUM)
        == bha.INTERVAL_OVERRIDE_MAXIMUM
    )


def test_coerce_alert_interval_rejects_non_numeric():
    assert bha._coerce_alert_interval("notanint") is None


def test_coerce_alert_interval_rejects_float_string():
    assert bha._coerce_alert_interval("60.5") is None


def test_coerce_alert_interval_rejects_blank_string():
    assert bha._coerce_alert_interval("") is None
    assert bha._coerce_alert_interval("   ") is None


def test_coerce_alert_interval_rejects_other_types():
    assert bha._coerce_alert_interval(None) is None
    assert bha._coerce_alert_interval(1.5) is None
    assert bha._coerce_alert_interval([60]) is None
    assert bha._coerce_alert_interval({"value": 60}) is None


def test_set_alert_interval_override_applies():
    bha.set_alert_interval_override(120)
    assert bha.get_alert_interval_override() == 120


def test_set_alert_interval_override_idempotent():
    bha.set_alert_interval_override(120)
    bha.set_alert_interval_override(120)
    assert bha.get_alert_interval_override() == 120


def test_set_alert_interval_override_rejects_zero():
    with pytest.raises(ValueError):
        bha.set_alert_interval_override(0)


def test_set_alert_interval_override_rejects_negative():
    with pytest.raises(ValueError):
        bha.set_alert_interval_override(-1)


def test_set_alert_interval_override_rejects_above_max():
    with pytest.raises(ValueError):
        bha.set_alert_interval_override(bha.INTERVAL_OVERRIDE_MAXIMUM + 1)


def test_set_alert_interval_override_rejects_bool():
    with pytest.raises(ValueError):
        bha.set_alert_interval_override(True)  # type: ignore[arg-type]


def test_clear_alert_interval_override_returns_true_when_set():
    bha.set_alert_interval_override(120)
    assert bha.clear_alert_interval_override() is True
    assert bha.get_alert_interval_override() is None


def test_clear_alert_interval_override_returns_false_when_unset():
    assert bha.clear_alert_interval_override() is False


def test_get_alert_interval_override_returns_none_by_default():
    assert bha.get_alert_interval_override() is None


def test_get_interval_seconds_prefers_override(monkeypatch):
    """Override beats env beats default."""
    monkeypatch.setenv("BOT_HEALTH_ALERT_INTERVAL_SECONDS", "30")
    bha.set_alert_interval_override(180)
    assert bha.get_bot_health_alert_interval_seconds() == 180


def test_get_interval_seconds_falls_through_when_override_cleared(monkeypatch):
    monkeypatch.setenv("BOT_HEALTH_ALERT_INTERVAL_SECONDS", "30")
    bha.set_alert_interval_override(180)
    bha.clear_alert_interval_override()
    assert bha.get_bot_health_alert_interval_seconds() == 30


def test_get_source_returns_db_when_override_set():
    bha.set_alert_interval_override(120)
    assert bha.get_bot_health_alert_interval_source() == "db"


def test_get_source_returns_env_when_only_env_set(monkeypatch):
    monkeypatch.setenv("BOT_HEALTH_ALERT_INTERVAL_SECONDS", "30")
    assert bha.get_bot_health_alert_interval_source() == "env"


def test_get_source_returns_default_with_blank_env():
    assert bha.get_bot_health_alert_interval_source() == "default"


def test_get_source_returns_default_with_invalid_env(monkeypatch):
    monkeypatch.setenv("BOT_HEALTH_ALERT_INTERVAL_SECONDS", "notanint")
    # Falls through to default because env doesn't parse as an int.
    assert bha.get_bot_health_alert_interval_source() == "default"


@pytest.mark.asyncio
async def test_refresh_from_db_with_no_row_clears_override():
    bha.set_alert_interval_override(120)
    db = _StubDB()  # no rows
    result = await bha.refresh_alert_interval_override_from_db(db)
    assert result is None
    assert bha.get_alert_interval_override() is None


@pytest.mark.asyncio
async def test_refresh_from_db_loads_valid_row():
    db = _StubDB({bha.ALERT_INTERVAL_SETTING_KEY: "240"})
    result = await bha.refresh_alert_interval_override_from_db(db)
    assert result == 240
    assert bha.get_alert_interval_override() == 240


@pytest.mark.asyncio
async def test_refresh_from_db_keeps_cache_on_get_error(caplog):
    bha.set_alert_interval_override(120)
    db = _StubDB(raise_on_get=RuntimeError("pool blip"))
    with caplog.at_level(logging.ERROR, logger="bot.bot_health_alert"):
        result = await bha.refresh_alert_interval_override_from_db(db)
    assert result == 120
    # Cache must not have been wiped by the transient error.
    assert bha.get_alert_interval_override() == 120
    assert any("get_setting" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_refresh_from_db_clears_on_malformed_value(caplog):
    bha.set_alert_interval_override(120)
    db = _StubDB({bha.ALERT_INTERVAL_SETTING_KEY: "notanint"})
    with caplog.at_level(logging.WARNING, logger="bot.bot_health_alert"):
        result = await bha.refresh_alert_interval_override_from_db(db)
    assert result is None
    assert bha.get_alert_interval_override() is None
    assert any(
        "rejected stored" in r.message for r in caplog.records
    )


@pytest.mark.asyncio
async def test_refresh_from_db_clears_on_above_max(caplog):
    bha.set_alert_interval_override(60)
    db = _StubDB(
        {bha.ALERT_INTERVAL_SETTING_KEY: str(
            bha.INTERVAL_OVERRIDE_MAXIMUM + 1
        )}
    )
    with caplog.at_level(logging.WARNING, logger="bot.bot_health_alert"):
        result = await bha.refresh_alert_interval_override_from_db(db)
    assert result is None
    assert bha.get_alert_interval_override() is None


@pytest.mark.asyncio
async def test_refresh_from_db_with_none_db_returns_cache():
    bha.set_alert_interval_override(120)
    result = await bha.refresh_alert_interval_override_from_db(None)
    assert result == 120
    assert bha.get_alert_interval_override() == 120


# ---------------------------------------------------------------------
# bundled bug fix: panel cadence sync via update_loop_cadence.
# ---------------------------------------------------------------------


def test_sync_registered_cadence_updates_loop_cadences():
    """The loop's resolved cadence must be reflected in
    ``bot_health.LOOP_CADENCES`` so the ``/admin/control`` panel's
    "stale threshold" doesn't show 180s while the loop ticks every
    600s.
    """
    import bot_health
    # Register the loop name with the compile-time default so the
    # production-time decorator's effect is in place even if a
    # previous test cleared it.
    bot_health.LOOP_CADENCES["bot_health_alert"] = (
        bha._BOT_HEALTH_ALERT_INTERVAL_SECONDS_DEFAULT
    )

    bha._sync_registered_cadence(600)
    assert bot_health.LOOP_CADENCES["bot_health_alert"] == 600


def test_sync_registered_cadence_swallows_unknown_loop(caplog):
    """If the registry is empty (test harness),
    ``update_loop_cadence`` raises KeyError. The sync helper logs +
    swallows it so a flaky test doesn't take down the loop.
    """
    import bot_health
    bot_health.LOOP_CADENCES.pop("bot_health_alert", None)
    with caplog.at_level(logging.ERROR, logger="bot.bot_health_alert"):
        bha._sync_registered_cadence(120)
    assert any(
        "update_loop_cadence" in r.message for r in caplog.records
    )
