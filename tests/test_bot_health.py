"""Stage-15-Step-F unit tests for the bot-health classifier and the
force-stop primitive."""

from __future__ import annotations

import signal

import pytest

import bot_health as bh


# ── compute_bot_status ─────────────────────────────────────────────


def test_idle_when_no_load_no_drops():
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=(),
        db_error=None,
        login_throttle_active_keys=0,
    )
    assert status.level is bh.BotStatusLevel.IDLE
    assert status.score == 0
    assert status.signals == ()
    assert "idle" in status.summary.lower()


def test_healthy_when_active_load_no_alarms():
    status = bh.compute_bot_status(
        inflight_count=3,
        ipn_drops_total=0,
        loop_ticks={"fx_refresh": 1_000_000.0},
        expected_loops=("fx_refresh",),
        db_error=None,
        login_throttle_active_keys=0,
        now=1_000_500.0,  # 500s after last tick — fresh
    )
    assert status.level is bh.BotStatusLevel.HEALTHY
    assert status.score == 1


def test_busy_when_inflight_above_default_threshold():
    status = bh.compute_bot_status(
        inflight_count=bh.DEFAULT_BUSY_INFLIGHT,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=(),
        db_error=None,
        login_throttle_active_keys=0,
    )
    assert status.level is bh.BotStatusLevel.BUSY
    assert status.score == 2
    assert any(
        f"{bh.DEFAULT_BUSY_INFLIGHT}" in s for s in status.signals
    )


def test_busy_threshold_overridable_via_env(monkeypatch):
    monkeypatch.setenv("BOT_HEALTH_BUSY_INFLIGHT", "5")
    # Just under the override threshold → still healthy.
    healthy = bh.compute_bot_status(
        inflight_count=4,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=(),
        db_error=None,
        login_throttle_active_keys=0,
    )
    assert healthy.level is bh.BotStatusLevel.HEALTHY
    # At the threshold → busy.
    busy = bh.compute_bot_status(
        inflight_count=5,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=(),
        db_error=None,
        login_throttle_active_keys=0,
    )
    assert busy.level is bh.BotStatusLevel.BUSY


def test_degraded_when_loop_stale():
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={"fx_refresh": 1.0},
        expected_loops=("fx_refresh",),
        db_error=None,
        login_throttle_active_keys=0,
        now=1.0 + bh.DEFAULT_LOOP_STALE_SECONDS + 60,
    )
    assert status.level is bh.BotStatusLevel.DEGRADED
    assert status.score == 3
    assert any("fx_refresh" in s for s in status.signals)


def test_degraded_when_loop_never_ticked():
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=("fx_refresh", "model_discovery"),
        db_error=None,
        login_throttle_active_keys=0,
    )
    assert status.level is bh.BotStatusLevel.DEGRADED
    # Both loops show up as stale.
    assert len(status.signals) == 2


def test_degraded_summary_truncates_long_signal_list():
    """Signals list should be complete but the inline summary is trimmed."""
    expected = tuple(f"loop_{i}" for i in range(10))
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=expected,
        db_error=None,
        login_throttle_active_keys=0,
    )
    assert status.level is bh.BotStatusLevel.DEGRADED
    assert len(status.signals) == 10
    assert "…" in status.summary  # head + ellipsis


def test_under_attack_on_ipn_drop_spike():
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        ipn_drops_recent=bh.DEFAULT_IPN_DROP_ATTACK_THRESHOLD,
        loop_ticks={},
        expected_loops=(),
        db_error=None,
        login_throttle_active_keys=0,
    )
    assert status.level is bh.BotStatusLevel.UNDER_ATTACK
    assert status.score == 4


def test_long_uptime_drops_total_alone_does_not_trip_attack():
    """Bug-fix regression: a long-running deploy that has accumulated
    well over the attack threshold of drops *since boot* must NOT be
    classified UNDER_ATTACK when no drops have occurred in the recent
    rate-window. Pre-fix, ``ipn_drops_total`` drove the comparison and
    a 6-month-old deploy with one bad-signature row a day would
    silently false-fire UNDER_ATTACK on the dashboard while nothing
    was actually happening."""
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=bh.DEFAULT_IPN_DROP_ATTACK_THRESHOLD * 10,
        ipn_drops_recent=0,
        loop_ticks={},
        expected_loops=(),
        db_error=None,
        login_throttle_active_keys=0,
    )
    assert status.level is bh.BotStatusLevel.HEALTHY
    # The since-boot count is still surfaced as informational so
    # the panel can show "N IPN drop(s) since boot" — just not
    # cause an attack alert.
    assert any("since boot" in s for s in status.signals)


def test_under_attack_on_login_throttle_saturation():
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=(),
        db_error=None,
        login_throttle_active_keys=bh.DEFAULT_LOGIN_THROTTLE_ATTACK_KEYS,
    )
    assert status.level is bh.BotStatusLevel.UNDER_ATTACK
    assert any("login-throttle" in s for s in status.signals)


def test_under_attack_combines_ipn_and_login_signals():
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        ipn_drops_recent=bh.DEFAULT_IPN_DROP_ATTACK_THRESHOLD + 50,
        loop_ticks={},
        expected_loops=(),
        db_error=None,
        login_throttle_active_keys=bh.DEFAULT_LOGIN_THROTTLE_ATTACK_KEYS + 5,
    )
    assert status.level is bh.BotStatusLevel.UNDER_ATTACK
    assert len(status.signals) == 2


def test_down_when_db_error_set_overrides_other_signals():
    """DB error is the most severe signal — wins over an in-flight chat
    burst and a drop spike."""
    status = bh.compute_bot_status(
        inflight_count=200,
        ipn_drops_total=10_000,
        ipn_drops_recent=10_000,
        loop_ticks={},
        expected_loops=("fx_refresh",),
        db_error="connection refused",
        login_throttle_active_keys=100,
    )
    assert status.level is bh.BotStatusLevel.DOWN
    assert status.score == 5
    assert "connection refused" in status.summary


def test_under_attack_outranks_degraded():
    """A stale loop AND a flood is reported as under_attack — highest
    severity wins so the operator's attention goes to the active
    threat first."""
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        ipn_drops_recent=bh.DEFAULT_IPN_DROP_ATTACK_THRESHOLD,
        loop_ticks={"fx_refresh": 1.0},
        expected_loops=("fx_refresh",),
        db_error=None,
        login_throttle_active_keys=0,
        now=10_000_000.0,  # loop is millennia stale
    )
    assert status.level is bh.BotStatusLevel.UNDER_ATTACK


def test_busy_outranks_healthy():
    status = bh.compute_bot_status(
        inflight_count=bh.DEFAULT_BUSY_INFLIGHT + 10,
        ipn_drops_total=5,
        loop_ticks={},
        expected_loops=(),
        db_error=None,
        login_throttle_active_keys=0,
    )
    assert status.level is bh.BotStatusLevel.BUSY


@pytest.mark.parametrize("invalid", ["abc", "-1", " "])
def test_env_threshold_invalid_falls_back_to_default(monkeypatch, invalid):
    monkeypatch.setenv("BOT_HEALTH_BUSY_INFLIGHT", invalid)
    # The default threshold (50) is still in effect → 49 is healthy.
    status = bh.compute_bot_status(
        inflight_count=49,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=(),
        db_error=None,
        login_throttle_active_keys=0,
    )
    assert status.level is bh.BotStatusLevel.HEALTHY


def test_status_score_helper_matches_status_score_field():
    for level in bh.BotStatusLevel:
        # Build a minimal status for *level* and verify status_score
        # agrees with the field — both must come from the same map.
        if level is bh.BotStatusLevel.DOWN:
            s = bh.compute_bot_status(
                inflight_count=0, ipn_drops_total=0, loop_ticks={},
                expected_loops=(), db_error="x",
                login_throttle_active_keys=0,
            )
        elif level is bh.BotStatusLevel.UNDER_ATTACK:
            s = bh.compute_bot_status(
                inflight_count=0,
                ipn_drops_total=0,
                ipn_drops_recent=bh.DEFAULT_IPN_DROP_ATTACK_THRESHOLD,
                loop_ticks={}, expected_loops=(), db_error=None,
                login_throttle_active_keys=0,
            )
        elif level is bh.BotStatusLevel.DEGRADED:
            s = bh.compute_bot_status(
                inflight_count=0, ipn_drops_total=0, loop_ticks={},
                expected_loops=("x",), db_error=None,
                login_throttle_active_keys=0,
            )
        elif level is bh.BotStatusLevel.BUSY:
            s = bh.compute_bot_status(
                inflight_count=bh.DEFAULT_BUSY_INFLIGHT,
                ipn_drops_total=0, loop_ticks={},
                expected_loops=(), db_error=None,
                login_throttle_active_keys=0,
            )
        elif level is bh.BotStatusLevel.HEALTHY:
            s = bh.compute_bot_status(
                inflight_count=1, ipn_drops_total=0, loop_ticks={},
                expected_loops=(), db_error=None,
                login_throttle_active_keys=0,
            )
        else:  # IDLE
            s = bh.compute_bot_status(
                inflight_count=0, ipn_drops_total=0, loop_ticks={},
                expected_loops=(), db_error=None,
                login_throttle_active_keys=0,
            )
        assert s.score == bh.status_score(s.level)


# ── request_force_stop ─────────────────────────────────────────────


def test_request_force_stop_calls_kill_with_default_sigterm():
    captured: list[tuple[int, int]] = []

    def fake_kill(pid: int, sig: int) -> None:
        captured.append((pid, sig))

    bh.request_force_stop(kill_fn=fake_kill, pid=12345)
    assert captured == [(12345, signal.SIGTERM)]


def test_request_force_stop_honours_explicit_signal():
    captured: list[tuple[int, int]] = []

    def fake_kill(pid: int, sig: int) -> None:
        captured.append((pid, sig))

    bh.request_force_stop(
        kill_fn=fake_kill, pid=98765, signal_number=signal.SIGINT
    )
    assert captured == [(98765, signal.SIGINT)]


def test_request_force_stop_defaults_pid_to_current_process(monkeypatch):
    """When pid is None, it falls back to ``os.getpid()``."""
    captured: list[tuple[int, int]] = []

    def fake_kill(pid: int, sig: int) -> None:
        captured.append((pid, sig))

    # We don't pass pid — the primitive must read os.getpid().
    bh.request_force_stop(kill_fn=fake_kill)
    assert len(captured) == 1
    assert captured[0][1] == signal.SIGTERM
    assert captured[0][0] > 0  # any real pid is positive


# ── BotStatus dataclass ────────────────────────────────────────────


def test_bot_status_severity_property_aliases_score():
    s = bh.compute_bot_status(
        inflight_count=0, ipn_drops_total=0, loop_ticks={},
        expected_loops=(), db_error=None,
        login_throttle_active_keys=0,
    )
    assert s.severity == s.score


def test_bot_status_is_immutable():
    s = bh.compute_bot_status(
        inflight_count=0, ipn_drops_total=0, loop_ticks={},
        expected_loops=(), db_error=None,
        login_throttle_active_keys=0,
    )
    with pytest.raises((AttributeError, Exception)):
        s.level = bh.BotStatusLevel.DOWN  # type: ignore[misc]
