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
    """A loop that has not ticked is DEGRADED *once the grace
    window from boot expires*.

    Per-loop grace = the loop's stale threshold. ``fx_refresh``
    cadence 600 s → threshold ~1260 s. ``model_discovery`` cadence
    21600 s → threshold ~43260 s. We mock a boot 50 000 s ago so
    both grace windows have elapsed.
    """
    now = 1_000_000.0
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=("fx_refresh", "model_discovery"),
        db_error=None,
        login_throttle_active_keys=0,
        now=now,
        process_start_epoch=now - 50_000.0,
    )
    assert status.level is bh.BotStatusLevel.DEGRADED
    # Both loops show up as stale.
    assert len(status.signals) == 2


def test_degraded_summary_truncates_long_signal_list():
    """Signals list should be complete but the inline summary is trimmed."""
    expected = tuple(f"loop_{i}" for i in range(10))
    now = 1_000_000.0
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=expected,
        db_error=None,
        login_throttle_active_keys=0,
        now=now,
        # Far past boot so even unknown loops (using the legacy
        # 1800 s default threshold) are out of grace.
        process_start_epoch=now - 100_000.0,
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


# ── Per-loop staleness thresholds + grace period ──────────────────


def test_fresh_boot_does_not_flag_long_cadence_loop_as_stale():
    """Bug-fix regression: the legacy single
    ``BOT_HEALTH_LOOP_STALE_SECONDS=1800`` threshold over-flagged
    long-cadence loops on a freshly-booted bot. ``catalog_refresh``
    has a 24h cadence by design — a bot that's been up for 30 min
    and hasn't yet hit its first catalog-refresh tick is *not*
    DEGRADED, the loop simply hasn't reached its first scheduled
    fire."""
    now = 1_000_000.0
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=("catalog_refresh",),
        db_error=None,
        login_throttle_active_keys=0,
        now=now,
        # Boot 30 min ago — well within the 24h grace window.
        process_start_epoch=now - 1_800.0,
    )
    assert status.level is bh.BotStatusLevel.IDLE
    assert status.signals == ()


def test_long_cadence_loop_stale_only_after_two_cadences():
    """``model_discovery`` has a 6h cadence (21600 s). The per-loop
    threshold is 2 × cadence + 60 s = 43260 s. Below that, a missed
    tick is *not* an alarm — the loop is just on its scheduled
    interval."""
    now = 1_000_000.0
    threshold = bh.LOOP_CADENCES["model_discovery"] * 2 + 60
    # Last tick was just under the threshold ago — fresh.
    fresh = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={"model_discovery": now - (threshold - 1)},
        expected_loops=("model_discovery",),
        db_error=None,
        login_throttle_active_keys=0,
        now=now,
        process_start_epoch=now - 100_000.0,  # past grace
    )
    assert fresh.level is not bh.BotStatusLevel.DEGRADED
    # One second past threshold — stale.
    stale = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={"model_discovery": now - (threshold + 1)},
        expected_loops=("model_discovery",),
        db_error=None,
        login_throttle_active_keys=0,
        now=now,
        process_start_epoch=now - 100_000.0,
    )
    assert stale.level is bh.BotStatusLevel.DEGRADED
    assert any("model_discovery" in s for s in stale.signals)


def test_short_cadence_loop_stale_at_short_threshold():
    """``bot_health_alert`` has a 60 s cadence. The per-loop
    threshold is 2 × 60 + 60 = 180 s. The pre-fix legacy threshold
    was 1800 s — meaning a 5-minute outage of the alert loop would
    have been silent on the panel."""
    now = 1_000_000.0
    # 200 s ago — past the 180 s threshold but well within the
    # legacy 1800 s threshold.
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={"bot_health_alert": now - 200.0},
        expected_loops=("bot_health_alert",),
        db_error=None,
        login_throttle_active_keys=0,
        now=now,
        process_start_epoch=now - 100_000.0,
    )
    assert status.level is bh.BotStatusLevel.DEGRADED
    assert any("bot_health_alert" in s for s in status.signals)


def test_explicit_env_override_beats_cadence_derived(monkeypatch):
    """An operator can pin a per-loop threshold via env if the
    cadence-derived default isn't right for their deploy."""
    monkeypatch.setenv("BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS", "300")
    now = 1_000_000.0
    # 400 s ago — past the override (300 s), well within the
    # cadence-derived threshold (1260 s).
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={"fx_refresh": now - 400.0},
        expected_loops=("fx_refresh",),
        db_error=None,
        login_throttle_active_keys=0,
        now=now,
        process_start_epoch=now - 100_000.0,
    )
    assert status.level is bh.BotStatusLevel.DEGRADED


def test_unknown_loop_uses_legacy_threshold(monkeypatch):
    """A loop name that isn't in ``LOOP_CADENCES`` falls back to
    the legacy single-knob ``BOT_HEALTH_LOOP_STALE_SECONDS`` so a
    future loop can be added without touching this module."""
    monkeypatch.setenv("BOT_HEALTH_LOOP_STALE_SECONDS", "100")
    now = 1_000_000.0
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={"future_unknown_loop": now - 200.0},
        expected_loops=("future_unknown_loop",),
        db_error=None,
        login_throttle_active_keys=0,
        now=now,
        process_start_epoch=now - 100_000.0,
    )
    assert status.level is bh.BotStatusLevel.DEGRADED


def test_explicit_env_override_zero_falls_through(monkeypatch):
    """A non-positive override value is rejected (mirrors the
    fail-safe convention in ``_env_int``); the cadence-derived
    threshold takes over."""
    monkeypatch.setenv("BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS", "0")
    now = 1_000_000.0
    threshold = bh.LOOP_CADENCES["fx_refresh"] * 2 + 60
    # Just under the cadence-derived threshold → fresh.
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={"fx_refresh": now - (threshold - 10)},
        expected_loops=("fx_refresh",),
        db_error=None,
        login_throttle_active_keys=0,
        now=now,
        process_start_epoch=now - 100_000.0,
    )
    assert status.level is not bh.BotStatusLevel.DEGRADED


def test_explicit_env_override_garbage_falls_through(monkeypatch):
    """Same as above for non-int garbage."""
    monkeypatch.setenv(
        "BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS", "not-an-int"
    )
    now = 1_000_000.0
    threshold = bh.LOOP_CADENCES["fx_refresh"] * 2 + 60
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={"fx_refresh": now - (threshold - 10)},
        expected_loops=("fx_refresh",),
        db_error=None,
        login_throttle_active_keys=0,
        now=now,
        process_start_epoch=now - 100_000.0,
    )
    assert status.level is not bh.BotStatusLevel.DEGRADED


def test_never_ticked_grace_window_is_per_loop():
    """A bot booted 1h ago with no fx_refresh tick is DEGRADED
    (fx_refresh threshold ~1260 s, uptime 3600 s > threshold) but
    a no model_discovery tick is *not* DEGRADED (model_discovery
    threshold ~43260 s, uptime 3600 s < threshold)."""
    now = 1_000_000.0
    status = bh.compute_bot_status(
        inflight_count=0,
        ipn_drops_total=0,
        loop_ticks={},
        expected_loops=("fx_refresh", "model_discovery"),
        db_error=None,
        login_throttle_active_keys=0,
        now=now,
        process_start_epoch=now - 3_600.0,
    )
    assert status.level is bh.BotStatusLevel.DEGRADED
    assert len(status.signals) == 1
    assert any("fx_refresh" in s for s in status.signals)
    assert not any("model_discovery" in s for s in status.signals)


def test_get_process_start_epoch_returns_module_load_time():
    """``get_process_start_epoch`` is a stable single-value
    accessor — repeated calls return the same value (the panel's
    uptime gauge and the classifier's grace check must agree)."""
    a = bh.get_process_start_epoch()
    b = bh.get_process_start_epoch()
    assert a == b
    assert a > 0  # captured at module load


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


# ── Cadence introspection accessors (Stage-15-Step-F follow-up #4) ─


def test_loop_cadence_seconds_returns_published_value():
    """Every entry in ``LOOP_CADENCES`` is reachable via the public
    accessor; unknown names round-trip to ``None`` (vs raising) so
    the panel's snapshot loop can ask about *every* loop in
    ``_LOOP_METRIC_NAMES`` without a try/except per name."""
    for name, expected in bh.LOOP_CADENCES.items():
        assert bh.loop_cadence_seconds(name) == expected
    assert bh.loop_cadence_seconds("nope") is None
    assert bh.loop_cadence_seconds("") is None


def test_loop_stale_threshold_seconds_uses_cadence_derived_default():
    """For known loops the public threshold is ``2 × cadence + 60``
    — same formula the classifier uses internally. Pinned per-loop
    so a refactor of ``_stale_threshold_seconds`` can't silently
    drift the panel away from the classifier."""
    margin = bh._STALE_THRESHOLD_MARGIN_SECONDS  # noqa: SLF001
    for name, cadence in bh.LOOP_CADENCES.items():
        assert (
            bh.loop_stale_threshold_seconds(name)
            == cadence * 2 + margin
        ), f"loop_stale_threshold_seconds({name!r}) drifted"


def test_loop_stale_threshold_seconds_unknown_falls_back_to_legacy(
    monkeypatch,
):
    """An unknown loop name falls back to the legacy single
    ``BOT_HEALTH_LOOP_STALE_SECONDS`` knob — important so a brand-new
    loop that opts in to ``_LOOP_METRIC_NAMES`` *before* its cadence
    is registered here doesn't crash the panel."""
    monkeypatch.setenv("BOT_HEALTH_LOOP_STALE_SECONDS", "777")
    assert bh.loop_stale_threshold_seconds("brand_new_loop") == 777


def test_loop_stale_threshold_seconds_legacy_default_when_env_unset(
    monkeypatch,
):
    """Unknown loop with no env override → ``DEFAULT_LOOP_STALE_SECONDS``."""
    monkeypatch.delenv("BOT_HEALTH_LOOP_STALE_SECONDS", raising=False)
    assert (
        bh.loop_stale_threshold_seconds("brand_new_loop")
        == bh.DEFAULT_LOOP_STALE_SECONDS
    )


def test_loop_stale_threshold_seconds_explicit_override_wins(
    monkeypatch,
):
    """A per-loop env override takes precedence over the cadence-
    derived default — same fail-safe contract as the private
    ``_stale_threshold_seconds`` (operators can pin a tighter or
    looser threshold for their deploy without redeploying code)."""
    monkeypatch.setenv("BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS", "111")
    assert bh.loop_stale_threshold_seconds("fx_refresh") == 111


def test_zarinpal_backfill_has_registered_cadence():
    """Bug-fix regression (Stage-15-Step-F follow-up #4): pre-fix
    ``zarinpal_backfill`` was in ``_LOOP_METRIC_NAMES`` (so it
    got a heartbeat gauge) but missing from ``LOOP_CADENCES``, so
    its 5-min-cadence loop fell back to the legacy 30-min stale
    threshold — six missed ticks before the panel even hinted at
    a problem. Pinning the cadence here means a future regression
    that drops the entry will fail this test loud-and-once."""
    from metrics import _LOOP_METRIC_NAMES  # noqa: SLF001

    assert "zarinpal_backfill" in _LOOP_METRIC_NAMES
    assert "zarinpal_backfill" in bh.LOOP_CADENCES
    assert bh.LOOP_CADENCES["zarinpal_backfill"] == 300
    # Threshold is the cadence-derived 2×300+60 = 660, NOT the
    # legacy 1800 the bug exposed.
    assert bh.loop_stale_threshold_seconds("zarinpal_backfill") == 660


def test_every_loop_in_metric_names_has_cadence_or_falls_back_safely():
    """A loop that's in ``_LOOP_METRIC_NAMES`` but not in
    ``LOOP_CADENCES`` falls back to the legacy single-knob default.
    That's intentional (forward-compat) but it means a fresh entry
    in ``_LOOP_METRIC_NAMES`` quietly inherits a 30-min threshold
    even if its real cadence is very different. This test pins the
    invariant that today every metric-named loop has an explicit
    cadence — flag at PR-review time if a follow-up adds a new loop
    without also adding a ``LOOP_CADENCES`` entry."""
    from metrics import _LOOP_METRIC_NAMES  # noqa: SLF001

    missing = sorted(set(_LOOP_METRIC_NAMES) - set(bh.LOOP_CADENCES))
    assert missing == [], (
        f"loops in _LOOP_METRIC_NAMES but missing LOOP_CADENCES "
        f"entries: {missing}"
    )
