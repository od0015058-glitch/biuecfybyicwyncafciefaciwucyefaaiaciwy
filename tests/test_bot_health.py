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


# ── Stage-15-Step-F follow-up: DB-backed threshold overrides ──────


@pytest.fixture(autouse=False)
def _clear_overrides():
    """Reset the module-level override cache before/after each test
    so a leak across tests doesn't show up as a phantom flake."""
    bh._THRESHOLD_OVERRIDES.clear()
    yield
    bh._THRESHOLD_OVERRIDES.clear()


def test_zero_busy_inflight_env_falls_back_to_default(
    monkeypatch, _clear_overrides,
):
    """Bug fix: ``BOT_HEALTH_BUSY_INFLIGHT=0`` previously made every
    chat slot trip BUSY. The new ``minimum=1`` floor forces it to
    fall through to ``DEFAULT_BUSY_INFLIGHT``."""
    monkeypatch.setenv("BOT_HEALTH_BUSY_INFLIGHT", "0")
    s = bh.compute_bot_status(
        inflight_count=1, ipn_drops_total=0, loop_ticks={},
        expected_loops=(), db_error=None,
        login_throttle_active_keys=0,
    )
    # 1 in-flight is NOT >= the default 50, so HEALTHY (not BUSY).
    assert s.level is bh.BotStatusLevel.HEALTHY


def test_zero_ipn_attack_threshold_env_falls_back(
    monkeypatch, _clear_overrides,
):
    """Bug fix: ``BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD=0`` previously
    permanently flagged UNDER_ATTACK on a healthy bot because
    ``ipn_drops_recent >= 0`` is always true."""
    monkeypatch.setenv("BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD", "0")
    s = bh.compute_bot_status(
        inflight_count=0, ipn_drops_total=0, loop_ticks={},
        expected_loops=(), db_error=None,
        login_throttle_active_keys=0,
    )
    assert s.level is bh.BotStatusLevel.IDLE


def test_zero_login_throttle_env_falls_back(
    monkeypatch, _clear_overrides,
):
    monkeypatch.setenv("BOT_HEALTH_LOGIN_THROTTLE_ATTACK_KEYS", "0")
    s = bh.compute_bot_status(
        inflight_count=0, ipn_drops_total=0, loop_ticks={},
        expected_loops=(), db_error=None,
        login_throttle_active_keys=0,
    )
    assert s.level is bh.BotStatusLevel.IDLE


def test_zero_loop_stale_env_falls_back(monkeypatch, _clear_overrides):
    """Bug fix: legacy single-knob ``BOT_HEALTH_LOOP_STALE_SECONDS=0``
    would make any positive delta trip DEGRADED on every loop."""
    monkeypatch.setenv("BOT_HEALTH_LOOP_STALE_SECONDS", "0")
    # Use a loop name not in LOOP_CADENCES so the legacy knob is
    # actually consulted.
    s = bh.compute_bot_status(
        inflight_count=0, ipn_drops_total=0,
        loop_ticks={"unknown_loop": 1.0},
        expected_loops=("unknown_loop",), db_error=None,
        login_throttle_active_keys=0,
        now=10.0, process_start_epoch=0.0,
    )
    # With minimum=1, threshold=DEFAULT (1800), 9s delta is fresh.
    assert s.level is bh.BotStatusLevel.IDLE


def test_set_threshold_override_refuses_zero(_clear_overrides):
    with pytest.raises(ValueError):
        bh.set_threshold_override("BOT_HEALTH_BUSY_INFLIGHT", 0)
    with pytest.raises(ValueError):
        bh.set_threshold_override("BOT_HEALTH_BUSY_INFLIGHT", -5)


def test_set_threshold_override_refuses_bool(_clear_overrides):
    """``isinstance(True, int)`` is True in Python — guard against a
    template / form bug accidentally storing a bool."""
    with pytest.raises(ValueError):
        bh.set_threshold_override("BOT_HEALTH_BUSY_INFLIGHT", True)


def test_set_threshold_override_takes_effect_immediately(
    monkeypatch, _clear_overrides,
):
    """Override beats env even when env is also set."""
    monkeypatch.setenv("BOT_HEALTH_BUSY_INFLIGHT", "100")
    bh.set_threshold_override("BOT_HEALTH_BUSY_INFLIGHT", 5)
    s = bh.compute_bot_status(
        inflight_count=5, ipn_drops_total=0, loop_ticks={},
        expected_loops=(), db_error=None,
        login_throttle_active_keys=0,
    )
    # 5 chat slots is >= override 5 → BUSY (would be HEALTHY without
    # the override since env says 100).
    assert s.level is bh.BotStatusLevel.BUSY


def test_clear_threshold_override_returns_to_env(
    monkeypatch, _clear_overrides,
):
    monkeypatch.setenv("BOT_HEALTH_BUSY_INFLIGHT", "100")
    bh.set_threshold_override("BOT_HEALTH_BUSY_INFLIGHT", 1)
    assert bh.clear_threshold_override("BOT_HEALTH_BUSY_INFLIGHT") is True
    # No longer overridden: env says 100, so 5 slots is HEALTHY.
    s = bh.compute_bot_status(
        inflight_count=5, ipn_drops_total=0, loop_ticks={},
        expected_loops=(), db_error=None,
        login_throttle_active_keys=0,
    )
    assert s.level is bh.BotStatusLevel.HEALTHY
    # Idempotent: clearing again returns False.
    assert bh.clear_threshold_override("BOT_HEALTH_BUSY_INFLIGHT") is False


def test_get_threshold_overrides_snapshot_returns_copy(_clear_overrides):
    bh.set_threshold_override("BOT_HEALTH_BUSY_INFLIGHT", 7)
    snap = bh.get_threshold_overrides_snapshot()
    snap["BOT_HEALTH_BUSY_INFLIGHT"] = 999
    # Mutating the snapshot does NOT mutate the cache.
    again = bh.get_threshold_overrides_snapshot()
    assert again["BOT_HEALTH_BUSY_INFLIGHT"] == 7


@pytest.mark.asyncio
async def test_refresh_overrides_from_db_applies_valid_rows(
    _clear_overrides,
):
    class _Db:
        async def list_settings_with_prefix(self, prefix):
            assert prefix == "BOT_HEALTH_"
            return {
                "BOT_HEALTH_BUSY_INFLIGHT": "10",
                "BOT_HEALTH_LOOP_STALE_SECONDS": "60",
                # Invalid: not int.
                "BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD": "abc",
                # Invalid: below minimum.
                "BOT_HEALTH_LOGIN_THROTTLE_ATTACK_KEYS": "0",
                # Unknown key — ignored.
                "BOT_HEALTH_FUTURE_KNOB": "42",
            }

    snap = await bh.refresh_threshold_overrides_from_db(_Db())
    assert snap == {
        "BOT_HEALTH_BUSY_INFLIGHT": 10,
        "BOT_HEALTH_LOOP_STALE_SECONDS": 60,
    }


@pytest.mark.asyncio
async def test_refresh_overrides_from_db_handles_db_error(
    _clear_overrides,
):
    """A transient DB error must NOT blank the cache — the previous
    overrides stay in place so an outage doesn't silently revert
    every threshold to env defaults."""
    bh.set_threshold_override("BOT_HEALTH_BUSY_INFLIGHT", 7)

    class _Db:
        async def list_settings_with_prefix(self, prefix):
            raise RuntimeError("connection reset")

    snap = await bh.refresh_threshold_overrides_from_db(_Db())
    assert snap == {"BOT_HEALTH_BUSY_INFLIGHT": 7}


@pytest.mark.asyncio
async def test_refresh_overrides_from_db_handles_none_db(_clear_overrides):
    snap = await bh.refresh_threshold_overrides_from_db(None)
    assert snap == {}


@pytest.mark.asyncio
async def test_refresh_overrides_from_db_handles_non_dict_return(
    _clear_overrides,
):
    bh.set_threshold_override("BOT_HEALTH_BUSY_INFLIGHT", 7)

    class _Db:
        async def list_settings_with_prefix(self, prefix):
            return "not a dict"

    snap = await bh.refresh_threshold_overrides_from_db(_Db())
    # Cache unchanged — fail-safe behaviour.
    assert snap == {"BOT_HEALTH_BUSY_INFLIGHT": 7}


# ── Stage-15-Step-F follow-up #5: register_loop decorator ─────────


@pytest.fixture()
def _isolated_loop_registry():
    """Snapshot + restore the loop registry around a test.

    :func:`bh.reset_loop_registry_for_tests` empties both
    ``LOOP_CADENCES`` and ``metrics._LOOP_METRIC_NAMES``, so this
    fixture saves the current state, hands the test a clean slate,
    and restores afterwards. Without this, a test that registers a
    fake loop name leaks into every later test that iterates the
    registry.
    """
    import metrics

    saved_cadences = dict(bh.LOOP_CADENCES)
    saved_runners = dict(bh.LOOP_RUNNERS)
    saved_names = metrics._LOOP_METRIC_NAMES
    bh.reset_loop_registry_for_tests()
    yield
    bh.LOOP_CADENCES.clear()
    bh.LOOP_CADENCES.update(saved_cadences)
    bh.LOOP_RUNNERS.clear()
    bh.LOOP_RUNNERS.update(saved_runners)
    metrics._LOOP_METRIC_NAMES = saved_names


def test_register_loop_populates_both_registries(_isolated_loop_registry):
    """A bare ``register_loop`` call adds the name to both
    ``LOOP_CADENCES`` and ``metrics._LOOP_METRIC_NAMES``."""
    import metrics

    assert "fake_loop" not in bh.LOOP_CADENCES
    assert "fake_loop" not in metrics._LOOP_METRIC_NAMES

    bh.register_loop("fake_loop", cadence_seconds=42)

    assert bh.LOOP_CADENCES["fake_loop"] == 42
    assert "fake_loop" in metrics._LOOP_METRIC_NAMES


def test_register_loop_decorator_returns_function_unchanged(
    _isolated_loop_registry,
):
    """Decorator usage: the wrapped function must come through
    unchanged — name, qualname, signature, and the function object
    itself."""
    import inspect

    async def _orig(arg):
        return arg

    decorated = bh.register_loop(
        "decorated_loop", cadence_seconds=7,
    )(_orig)

    assert bh.LOOP_CADENCES["decorated_loop"] == 7
    # Decorator is a true no-op on the function: same identity,
    # name, signature, and ``async def`` shape.
    assert decorated is _orig
    assert decorated.__name__ == "_orig"
    assert inspect.iscoroutinefunction(decorated)
    assert (
        list(inspect.signature(decorated).parameters)
        == list(inspect.signature(_orig).parameters)
    )


def test_register_loop_idempotent_on_same_args(_isolated_loop_registry):
    """Re-registering the same ``(name, cadence)`` pair is a no-op."""
    bh.register_loop("dup", cadence_seconds=10)
    bh.register_loop("dup", cadence_seconds=10)  # no error
    assert bh.LOOP_CADENCES["dup"] == 10
    import metrics
    # Not duplicated in the metric tuple.
    assert metrics._LOOP_METRIC_NAMES.count("dup") == 1


def test_register_loop_raises_on_cadence_mismatch(
    _isolated_loop_registry,
):
    """Two registrations of the same name with different cadences
    is a hard error — the decorator's whole point is to keep the
    cadence and the name in lockstep, and a silent override would
    paper over a real configuration bug."""
    bh.register_loop("mismatch", cadence_seconds=10)
    with pytest.raises(RuntimeError, match="cadence mismatch"):
        bh.register_loop("mismatch", cadence_seconds=11)


def test_register_loop_rejects_empty_name(_isolated_loop_registry):
    with pytest.raises(ValueError, match="non-empty string"):
        bh.register_loop("", cadence_seconds=10)


def test_register_loop_rejects_non_str_name(_isolated_loop_registry):
    with pytest.raises(ValueError, match="non-empty string"):
        bh.register_loop(None, cadence_seconds=10)  # type: ignore[arg-type]


def test_register_loop_rejects_non_positive_cadence(
    _isolated_loop_registry,
):
    with pytest.raises(ValueError, match="positive int"):
        bh.register_loop("bad", cadence_seconds=0)
    with pytest.raises(ValueError, match="positive int"):
        bh.register_loop("bad", cadence_seconds=-1)


def test_register_loop_rejects_bool_cadence(_isolated_loop_registry):
    """``True`` is technically a positive int in Python (``True == 1``)
    but accepting it would let a typo (``cadence_seconds=True``)
    through with a 1-second threshold. Reject explicitly."""
    with pytest.raises(ValueError, match="positive int"):
        bh.register_loop("bad", cadence_seconds=True)  # type: ignore[arg-type]


def test_register_loop_rejects_non_int_cadence(_isolated_loop_registry):
    with pytest.raises(ValueError, match="positive int"):
        bh.register_loop("bad", cadence_seconds=10.5)  # type: ignore[arg-type]


def test_register_loop_used_via_decorator_threshold_matches(
    _isolated_loop_registry,
):
    """End-to-end: registering via decorator wires up the cadence-
    derived stale threshold correctly. ``2 × N + 60`` per the
    convention documented in ``bot_health.py``."""

    @bh.register_loop("e2e", cadence_seconds=200)
    async def _loop_fn():
        return None

    assert bh.loop_stale_threshold_seconds("e2e") == 200 * 2 + 60


def test_all_eight_production_loops_registered():
    """Every shipped loop must register itself at module import.

    Catches a refactor that drops a ``@register_loop`` line — the
    test imports each module and pins the (name, cadence) pair.
    Conftest already imports each module so registrations have
    fired by the time this test runs."""
    import metrics

    expected = {
        "min_amount_refresh": 900,
        "fx_refresh": 600,
        "model_discovery": 21_600,
        "catalog_refresh": 86_400,
        "pending_alert": 1_800,
        "pending_reaper": 900,
        "bot_health_alert": 60,
        "zarinpal_backfill": 300,
    }
    for name, cadence in expected.items():
        assert bh.LOOP_CADENCES.get(name) == cadence, (
            f"{name}: registry has "
            f"{bh.LOOP_CADENCES.get(name)!r}, expected {cadence}"
        )
        assert name in metrics._LOOP_METRIC_NAMES


# ── Stage-15-Step-F follow-up #6: per-loop runner registration ───


def test_register_loop_accepts_optional_runner(_isolated_loop_registry):
    """Passing ``runner=`` registers the runner in
    :data:`bh.LOOP_RUNNERS` so the panel's tick-now button has
    something to invoke."""
    async def my_runner(_app):
        pass

    bh.register_loop("my_loop", cadence_seconds=60, runner=my_runner)

    assert bh.LOOP_RUNNERS.get("my_loop") is my_runner
    assert bh.loop_runner("my_loop") is my_runner


def test_register_loop_runner_omitted_means_no_runner(
    _isolated_loop_registry,
):
    """A loop registered without a runner gets cadence + metric
    plumbing but no entry in ``LOOP_RUNNERS`` — :func:`loop_runner`
    returns None so the panel hides the button for that loop."""
    bh.register_loop("loop_without_runner", cadence_seconds=60)

    assert "loop_without_runner" in bh.LOOP_CADENCES
    assert "loop_without_runner" not in bh.LOOP_RUNNERS
    assert bh.loop_runner("loop_without_runner") is None


def test_register_loop_rejects_non_callable_runner(_isolated_loop_registry):
    """Passing a non-callable ``runner=`` is rejected at registration
    time — the panel's POST handler shouldn't have to defend against
    a misconfigured registry entry."""
    import pytest

    with pytest.raises(ValueError, match="runner must be callable"):
        bh.register_loop(
            "bad_loop",
            cadence_seconds=60,
            runner="not_callable",  # type: ignore[arg-type]
        )

    # And the registration must have rolled back — neither cadence
    # nor metric name was added.
    assert "bad_loop" not in bh.LOOP_CADENCES


def test_register_loop_runner_can_be_swapped(_isolated_loop_registry):
    """Re-registering with a new runner overrides the previous one
    — useful for tests that want to inject a stub. Cadence is
    still pinned so a mismatched cadence still raises."""
    async def first_runner(_app):
        pass

    async def second_runner(_app):
        pass

    bh.register_loop("swap_test", cadence_seconds=60, runner=first_runner)
    assert bh.LOOP_RUNNERS["swap_test"] is first_runner

    bh.register_loop("swap_test", cadence_seconds=60, runner=second_runner)
    assert bh.LOOP_RUNNERS["swap_test"] is second_runner


def test_loop_runner_returns_none_for_unknown_name():
    """Looking up a name that was never registered must return None
    (not raise) — the panel iterates every registered loop and a
    typo in a route should 302 with a flash, not 500."""
    assert bh.loop_runner("definitely_not_a_real_loop") is None


def test_reset_loop_registry_for_tests_clears_runners(
    _isolated_loop_registry,
):
    """The reset helper must clear all three pieces of state
    (cadences, runners, metric names) — otherwise a test that swaps
    in a stub runner leaks across test boundaries."""
    async def stub_runner(_app):
        pass

    bh.register_loop("temp", cadence_seconds=60, runner=stub_runner)
    assert "temp" in bh.LOOP_RUNNERS

    bh.reset_loop_registry_for_tests()
    assert "temp" not in bh.LOOP_CADENCES
    assert "temp" not in bh.LOOP_RUNNERS


def test_all_eight_production_loops_have_runners():
    """Every shipped loop must register a tick-now runner so the
    panel can offer the "Tick now" button for all of them. A
    missing runner is the common bug the registry prevents:
    silently-no-op buttons in the panel."""
    expected_runner_loops = [
        "min_amount_refresh",
        "fx_refresh",
        "model_discovery",
        "catalog_refresh",
        "pending_alert",
        "pending_reaper",
        "bot_health_alert",
        "zarinpal_backfill",
    ]
    for name in expected_runner_loops:
        assert name in bh.LOOP_RUNNERS, (
            f"{name}: no tick-now runner registered — "
            f"the panel button will be hidden"
        )
        runner = bh.loop_runner(name)
        assert callable(runner), (
            f"{name}: runner is registered but not callable: "
            f"{runner!r}"
        )


# ── Stage-15-Step-E #10b row 21: update_loop_cadence ──────────────


def test_update_loop_cadence_updates_in_place(_isolated_loop_registry):
    """The new public helper must replace the cadence value in
    :data:`LOOP_CADENCES` without raising.
    """
    bh.register_loop("dyn", cadence_seconds=60)
    assert bh.update_loop_cadence("dyn", 120) == 120
    assert bh.LOOP_CADENCES["dyn"] == 120


def test_update_loop_cadence_idempotent_on_same_value(_isolated_loop_registry):
    bh.register_loop("dyn", cadence_seconds=60)
    bh.update_loop_cadence("dyn", 60)
    bh.update_loop_cadence("dyn", 60)
    assert bh.LOOP_CADENCES["dyn"] == 60


def test_update_loop_cadence_does_not_touch_runner_or_metric(
    _isolated_loop_registry,
):
    """Updating the cadence must not drop the runner or the metric
    name registration.
    """
    import metrics

    async def stub(_app):
        return None

    bh.register_loop("dyn", cadence_seconds=60, runner=stub)
    bh.update_loop_cadence("dyn", 600)
    assert bh.LOOP_RUNNERS["dyn"] is stub
    assert "dyn" in metrics._LOOP_METRIC_NAMES


def test_update_loop_cadence_rejects_unknown_name(_isolated_loop_registry):
    with pytest.raises(KeyError):
        bh.update_loop_cadence("never_registered", 60)


def test_update_loop_cadence_rejects_empty_name(_isolated_loop_registry):
    with pytest.raises(ValueError):
        bh.update_loop_cadence("", 60)


def test_update_loop_cadence_rejects_non_str_name(_isolated_loop_registry):
    with pytest.raises(ValueError):
        bh.update_loop_cadence(None, 60)  # type: ignore[arg-type]


def test_update_loop_cadence_rejects_zero(_isolated_loop_registry):
    bh.register_loop("dyn", cadence_seconds=60)
    with pytest.raises(ValueError):
        bh.update_loop_cadence("dyn", 0)


def test_update_loop_cadence_rejects_negative(_isolated_loop_registry):
    bh.register_loop("dyn", cadence_seconds=60)
    with pytest.raises(ValueError):
        bh.update_loop_cadence("dyn", -1)


def test_update_loop_cadence_rejects_bool(_isolated_loop_registry):
    bh.register_loop("dyn", cadence_seconds=60)
    with pytest.raises(ValueError):
        bh.update_loop_cadence("dyn", True)  # type: ignore[arg-type]


def test_update_loop_cadence_rejects_float(_isolated_loop_registry):
    bh.register_loop("dyn", cadence_seconds=60)
    with pytest.raises(ValueError):
        bh.update_loop_cadence("dyn", 60.5)  # type: ignore[arg-type]


def test_update_loop_cadence_changes_stale_threshold(_isolated_loop_registry):
    """The bundled bug fix: the panel's stale threshold (``2× cadence
    + margin``) must follow a cadence update so an operator who tunes
    the loop interval doesn't see the loop forever marked overdue.
    """
    bh.register_loop("dyn", cadence_seconds=60)
    initial = bh.loop_stale_threshold_seconds("dyn")
    bh.update_loop_cadence("dyn", 600)
    new_threshold = bh.loop_stale_threshold_seconds("dyn")
    assert new_threshold > initial
    # 2 × 600 + 60 (the margin) = 1260.
    assert new_threshold == 1260
