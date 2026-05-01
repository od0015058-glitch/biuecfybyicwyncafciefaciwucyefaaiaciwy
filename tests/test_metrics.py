"""Stage-15-Step-A: tests for the Prometheus ``/metrics`` endpoint.

Covered surface:

* ``parse_ip_allowlist`` — pure CIDR parser (well-formed,
  whitespace tolerance, malformed-entry skip, mixed v4 / v6).
* ``is_ip_allowed`` — empty allowlist locks everyone out, valid v4
  / v6 membership, missing / unparseable ``request.remote``,
  v4-vs-v6 mismatches.
* ``record_loop_tick`` / ``get_loop_last_tick`` — set + read,
  reset-for-tests.
* ``render_metrics`` — full output shape: HELP / TYPE preamble per
  metric, labelled IPN counter rendering, gauge rendering, NaN /
  Inf gauge defence, trailing newline.
* ``metrics_handler`` — 200 + ``text/plain`` from an allowed IP,
  403 from a denied IP, end-to-end aiohttp roundtrip via the
  ``aiohttp_client`` fixture.
* ``install_metrics_route`` — registers ``GET /metrics`` and stashes
  the parsed allowlist under ``app["_metrics_allowlist"]``.

All tests run synchronously where possible and use the standard
``pytest-aiohttp`` ``aiohttp_client`` fixture for the
HTTP-roundtrip check.
"""

from __future__ import annotations

import ipaddress
from unittest.mock import MagicMock

import pytest
from aiohttp import web

import metrics


# ── parse_ip_allowlist ─────────────────────────────────────────────


def test_parse_ip_allowlist_well_formed():
    out = metrics.parse_ip_allowlist("127.0.0.1, 10.0.0.0/8 ,::1")
    assert isinstance(out, tuple)
    assert ipaddress.ip_network("127.0.0.1/32") in out
    assert ipaddress.ip_network("10.0.0.0/8") in out
    assert ipaddress.ip_network("::1/128") in out


def test_parse_ip_allowlist_skips_blank_and_malformed(caplog):
    with caplog.at_level("WARNING", logger="bot.metrics"):
        out = metrics.parse_ip_allowlist("127.0.0.1,,not-an-ip,10.0.0.0/8")
    assert len(out) == 2
    assert any("not-an-ip" in r.message for r in caplog.records)


def test_parse_ip_allowlist_empty_string_returns_empty_tuple():
    assert metrics.parse_ip_allowlist("") == ()
    assert metrics.parse_ip_allowlist("   ") == ()


# ── is_ip_allowed ──────────────────────────────────────────────────


def _make_request(remote: str | None) -> web.Request:
    """Build a minimal stub request with a controlled ``remote``."""
    req = MagicMock(spec=web.Request)
    req.remote = remote
    return req


def test_is_ip_allowed_empty_allowlist_locks_everyone_out():
    """Empty allowlist must NEVER mean 'allow all'.

    Fail-closed default — a typoed env var that drops every entry
    must not silently expose ``/metrics`` publicly.
    """
    assert metrics.is_ip_allowed(_make_request("127.0.0.1"), ()) is False


def test_is_ip_allowed_loopback_v4_in_default_allowlist():
    allowlist = metrics.parse_ip_allowlist(metrics.DEFAULT_METRICS_ALLOWLIST)
    assert metrics.is_ip_allowed(_make_request("127.0.0.1"), allowlist) is True


def test_is_ip_allowed_loopback_v6_in_default_allowlist():
    allowlist = metrics.parse_ip_allowlist(metrics.DEFAULT_METRICS_ALLOWLIST)
    assert metrics.is_ip_allowed(_make_request("::1"), allowlist) is True


def test_is_ip_allowed_outside_allowlist():
    allowlist = metrics.parse_ip_allowlist("10.0.0.0/8")
    assert metrics.is_ip_allowed(_make_request("8.8.8.8"), allowlist) is False
    assert metrics.is_ip_allowed(_make_request("10.1.2.3"), allowlist) is True


def test_is_ip_allowed_missing_remote_rejected():
    allowlist = metrics.parse_ip_allowlist("127.0.0.1")
    assert metrics.is_ip_allowed(_make_request(None), allowlist) is False


def test_is_ip_allowed_unparseable_remote_rejected():
    allowlist = metrics.parse_ip_allowlist("127.0.0.1")
    assert metrics.is_ip_allowed(_make_request("not-an-ip"), allowlist) is False


def test_is_ip_allowed_v4_vs_v6_membership_does_not_crash():
    """A v4 address against a v6-only allowlist must reject cleanly.

    ``IPv4Address in IPv6Network`` would raise ``TypeError`` in
    older Python versions; modern stdlib returns ``False`` but we
    still defend by catching the exception so a future upgrade
    can't blow the gate open.
    """
    allowlist = metrics.parse_ip_allowlist("::1/128")
    assert metrics.is_ip_allowed(_make_request("127.0.0.1"), allowlist) is False


# ── loop tick registry ─────────────────────────────────────────────


def test_record_loop_tick_round_trips():
    metrics.reset_loop_ticks_for_tests()
    assert metrics.get_loop_last_tick("fx_refresh") is None

    metrics.record_loop_tick("fx_refresh", ts=1700000000.0)
    assert metrics.get_loop_last_tick("fx_refresh") == 1700000000.0

    # Re-recording overwrites with the latest value.
    metrics.record_loop_tick("fx_refresh", ts=1700000999.0)
    assert metrics.get_loop_last_tick("fx_refresh") == 1700000999.0

    metrics.reset_loop_ticks_for_tests()


def test_reset_loop_ticks_clears_all():
    metrics.record_loop_tick("a", ts=1.0)
    metrics.record_loop_tick("b", ts=2.0)
    metrics.reset_loop_ticks_for_tests()
    assert metrics.get_loop_last_tick("a") is None
    assert metrics.get_loop_last_tick("b") is None


# ── render_metrics ─────────────────────────────────────────────────


def _patch_collectors(monkeypatch, *, ipn_drops=None, tetrapay_drops=None,
                     zarinpal_drops=None,
                     inflight=0, disabled_models=(), disabled_gateways=(),
                     key_count=0):
    """Patch every collector ``render_metrics`` reads so the test
    output is deterministic.
    """
    import admin_toggles
    import openrouter_keys
    import payments
    import rate_limit
    import tetrapay
    import zarinpal

    monkeypatch.setattr(
        payments, "get_ipn_drop_counters", lambda: dict(ipn_drops or {})
    )
    monkeypatch.setattr(
        tetrapay,
        "get_tetrapay_drop_counters",
        lambda: dict(tetrapay_drops or {}),
    )
    monkeypatch.setattr(
        zarinpal,
        "get_zarinpal_drop_counters",
        lambda: dict(zarinpal_drops or {}),
    )
    monkeypatch.setattr(
        rate_limit, "chat_inflight_count", lambda: int(inflight)
    )
    monkeypatch.setattr(
        admin_toggles,
        "get_disabled_models",
        lambda: frozenset(disabled_models),
    )
    monkeypatch.setattr(
        admin_toggles,
        "get_disabled_gateways",
        lambda: frozenset(disabled_gateways),
    )
    monkeypatch.setattr(openrouter_keys, "key_count", lambda: int(key_count))


def test_render_metrics_smoke(monkeypatch):
    """Output contains every expected metric name + a trailing newline."""
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch)

    body = metrics.render_metrics()
    assert body.endswith("\n")

    # Every metric we promise to expose appears in the output.
    expected_names = [
        "meowassist_ipn_drops_total",
        "meowassist_tetrapay_drops_total",
        # Stage-15-Step-E #9 bundled fix: Zarinpal shipped its own
        # drop registry in Stage-15-Step-E #8 but the Prometheus
        # exposition was never extended. The smoke test now pins
        # the third labelled counter so a future regression that
        # silently drops the import is caught at test time.
        "meowassist_zarinpal_drops_total",
        "meowassist_min_amount_refresh_last_run_epoch",
        "meowassist_fx_refresh_last_run_epoch",
        "meowassist_model_discovery_last_run_epoch",
        "meowassist_catalog_refresh_last_run_epoch",
        "meowassist_pending_alert_last_run_epoch",
        "meowassist_pending_reaper_last_run_epoch",
        "meowassist_chat_inflight_active",
        "meowassist_disabled_models_count",
        "meowassist_disabled_gateways_count",
        "meowassist_openrouter_keys_count",
    ]
    for name in expected_names:
        assert f"# HELP {name} " in body, f"missing HELP for {name}"
        assert f"# TYPE {name} " in body, f"missing TYPE for {name}"


def test_render_metrics_labelled_counter_format(monkeypatch):
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(
        monkeypatch,
        ipn_drops={"bad_signature": 3, "bad_json": 1, "replay": 7},
    )

    body = metrics.render_metrics()
    assert 'meowassist_ipn_drops_total{reason="bad_signature"} 3' in body
    assert 'meowassist_ipn_drops_total{reason="bad_json"} 1' in body
    assert 'meowassist_ipn_drops_total{reason="replay"} 7' in body
    # Sorted-by-label rendering means "bad_json" precedes
    # "bad_signature" in the body.
    assert body.index('"bad_json"') < body.index('"bad_signature"')


def test_render_metrics_zarinpal_drops_renders_with_reason_label(monkeypatch):
    """Stage-15-Step-E #9 bundled fix: an operator alerting on
    ``meowassist_*_drops_total{reason="bad_signature"}`` was blind to
    Zarinpal verify failures because the exposition silently ignored
    the third gateway's drop registry. Pin the per-reason rows so a
    future regression that drops the import is caught at test time.
    """
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(
        monkeypatch,
        zarinpal_drops={
            "verify_failed": 4,
            "missing_authority": 1,
            "replay": 9,
        },
    )

    body = metrics.render_metrics()
    assert 'meowassist_zarinpal_drops_total{reason="verify_failed"} 4' in body
    assert 'meowassist_zarinpal_drops_total{reason="missing_authority"} 1' in body
    assert 'meowassist_zarinpal_drops_total{reason="replay"} 9' in body
    # Counter type declared once for the family.
    assert "# TYPE meowassist_zarinpal_drops_total counter" in body
    # Sort-by-label means missing_authority < replay < verify_failed.
    assert body.index('"missing_authority"') < body.index('"replay"')
    assert body.index('"replay"') < body.index('"verify_failed"')


def test_format_labelled_counter_escapes_quotes_backslash_newlines(monkeypatch):
    """Bundled bug fix (PR Stage-15-Step-D #2): label values with ``"``,
    ``\\`` or newline must be escaped per the Prometheus
    text-exposition spec, otherwise a single poisoned label breaks
    the parser and the entire scrape returns blank.
    """
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(
        monkeypatch,
        ipn_drops={
            'has"quote': 1,
            "has\\backslash": 2,
            "has\nnewline": 3,
        },
    )

    body = metrics.render_metrics()
    # Each escape must be present in its rendered form.
    assert 'meowassist_ipn_drops_total{reason="has\\"quote"} 1' in body
    assert 'meowassist_ipn_drops_total{reason="has\\\\backslash"} 2' in body
    assert 'meowassist_ipn_drops_total{reason="has\\nnewline"} 3' in body
    # The newline label MUST NOT split the line. Find the newline-row
    # and verify it lives on a single physical line.
    newline_rows = [
        l for l in body.splitlines()
        if l.startswith("meowassist_ipn_drops_total{") and "newline" in l
    ]
    assert len(newline_rows) == 1, (
        "label with raw newline must render on exactly one line, got: "
        f"{newline_rows!r}"
    )


def test_escape_label_value_unit():
    """``_escape_label_value`` is a pure function — exercise the
    three escape paths directly."""
    assert metrics._escape_label_value("plain") == "plain"
    assert metrics._escape_label_value('with"quote') == 'with\\"quote'
    assert metrics._escape_label_value("with\\backslash") == "with\\\\backslash"
    assert metrics._escape_label_value("line1\nline2") == "line1\\nline2"
    # All three at once — backslashes must be escaped first to avoid
    # double-escaping the quote-escape sequence.
    assert (
        metrics._escape_label_value('a\\b"c\nd')
        == 'a\\\\b\\"c\\nd'
    )


def test_escape_help_text_unit():
    """``_escape_help_text`` is a pure function — only ``\\`` and
    newline are escaped; quotes are NOT (HELP text is unquoted).

    Bundled bug fix (Stage-15-Step-E #1 PR): same defensive escape
    pattern as :func:`_escape_label_value`, applied to the HELP
    line. Today's callers all pass static ASCII English strings so
    escaping is a no-op, but a future caller passing arbitrary text
    (a translated message, a config-derived description, a Windows
    path with ``\\``) would otherwise split the scrape on the
    embedded newline — and Prometheus would parse the second half
    as a new metric line, returning bogus data or blanking the
    entire response.
    """
    assert metrics._escape_help_text("plain text") == "plain text"
    # Quotes pass through unchanged.
    assert metrics._escape_help_text('a "quoted" word') == 'a "quoted" word'
    # Backslashes get doubled.
    assert metrics._escape_help_text("c:\\path") == "c:\\\\path"
    # Newlines become \\n.
    assert (
        metrics._escape_help_text("line1\nline2")
        == "line1\\nline2"
    )
    # Backslash + newline ordering: escape backslashes first so the
    # ``\\n`` we emit for newline doesn't get re-escaped.
    assert (
        metrics._escape_help_text("c:\\path\nfile")
        == "c:\\\\path\\nfile"
    )


def test_format_help_and_type_escapes_help_text_with_newline():
    """``_format_help_and_type`` must escape newlines in the HELP
    text. A raw ``\\n`` would split the line and break parsing.
    """
    lines = metrics._format_help_and_type(
        "meowassist_x", "first half\nsecond half", "counter"
    )
    # Exactly two lines emitted (HELP + TYPE), no spurious split.
    assert len(lines) == 2
    assert lines[0] == "# HELP meowassist_x first half\\nsecond half"
    assert lines[1] == "# TYPE meowassist_x counter"


def test_format_help_and_type_escapes_help_text_with_backslash():
    """A Windows-style path or a raw regex in the HELP text must
    have its backslashes doubled."""
    lines = metrics._format_help_and_type(
        "meowassist_x", "regex \\d+ count", "gauge"
    )
    assert lines[0] == "# HELP meowassist_x regex \\\\d+ count"


def test_render_metrics_empty_counter_still_emits_preamble(monkeypatch):
    """A counter with zero rows still emits HELP/TYPE — consumers
    running ``rate(...)`` against the empty counter need to see
    the metric name to avoid a 'no data' gap."""
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch, ipn_drops={})

    body = metrics.render_metrics()
    assert "# TYPE meowassist_ipn_drops_total counter" in body


def test_render_metrics_loop_epoch_default_zero(monkeypatch):
    """A loop that never ticked must render epoch 0, not blank /
    NaN. Prometheus-side ``time() - last_run_epoch > N`` alerts
    treat 0 as 'infinitely stale' which is the desired semantic.
    """
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch)

    body = metrics.render_metrics()
    assert "meowassist_fx_refresh_last_run_epoch 0\n" in body
    assert "meowassist_pending_reaper_last_run_epoch 0\n" in body


def test_render_metrics_loop_epoch_after_tick(monkeypatch):
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch)
    metrics.record_loop_tick("fx_refresh", ts=1700000123.0)

    body = metrics.render_metrics()
    # Integer-valued floats render as ints (clean output, no
    # ``1700000123.0``).
    assert "meowassist_fx_refresh_last_run_epoch 1700000123\n" in body


def test_render_metrics_loop_epoch_preserves_subsecond_precision(monkeypatch):
    """Regression: ``time.time()`` returns a non-integer float in
    production, and ``f"{value:g}"`` truncates a ~1.78e9 Unix
    epoch to 6 significant digits (e.g. ``1.77756e+09``), erasing
    ~2 000 s of precision. That breaks the heartbeat gauges' whole
    purpose: a staleness alert tuned for a 15-minute window would
    misfire on the ``:g``-induced precision loss alone.

    Pin the rendered output to the round-trip-safe ``str(float)``
    representation so a future refactor can't reintroduce ``:g``.
    """
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch)
    metrics.record_loop_tick("fx_refresh", ts=1777562092.857)

    body = metrics.render_metrics()
    # The full timestamp must round-trip — no scientific notation
    # / ``e+09`` truncation, no integer-cast precision loss.
    assert "meowassist_fx_refresh_last_run_epoch 1777562092.857\n" in body
    assert "1.77756e+09" not in body
    assert "1.77756e09" not in body
    # Round-trip back to the original float to prove the rendered
    # value is precise enough for ``time() - x > N`` alerting.
    line = next(
        l for l in body.splitlines()
        if l.startswith("meowassist_fx_refresh_last_run_epoch ")
    )
    rendered_value = float(line.split(" ", 1)[1])
    assert abs(rendered_value - 1777562092.857) < 1e-3


def test_render_metrics_gauge_values(monkeypatch):
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(
        monkeypatch,
        inflight=42,
        disabled_models=("openai/gpt-4o", "anthropic/claude-3"),
        disabled_gateways=("btc",),
        key_count=5,
    )

    body = metrics.render_metrics()
    assert "meowassist_chat_inflight_active 42\n" in body
    assert "meowassist_disabled_models_count 2\n" in body
    assert "meowassist_disabled_gateways_count 1\n" in body
    assert "meowassist_openrouter_keys_count 5\n" in body


def test_render_metrics_nan_inf_gauges_render_as_zero(monkeypatch):
    """NaN / Inf would crash Prometheus' parser. We coerce to 0
    (mirrors ``wallet_display.format_balance_block``'s NaN defence)."""
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch)
    metrics.record_loop_tick("fx_refresh", ts=float("nan"))
    metrics.record_loop_tick("pending_reaper", ts=float("inf"))

    body = metrics.render_metrics()
    assert "meowassist_fx_refresh_last_run_epoch 0\n" in body
    assert "meowassist_pending_reaper_last_run_epoch 0\n" in body
    # Confirm no metric line (i.e. a non-comment, non-empty line)
    # leaked the literal ``nan`` / ``inf`` token — the HELP /
    # TYPE comment lines may contain "infinitely stale" prose
    # which is fine.
    # Pull just the rendered value off each metric line (everything
    # after the final whitespace) so we don't false-positive on
    # metric names like ``meowassist_chat_inflight_active`` that
    # contain "inf" as a substring.
    rendered_values = [
        line.rsplit(" ", 1)[-1]
        for line in body.splitlines()
        if line and not line.startswith("#")
    ]
    for value in rendered_values:
        assert value.lower() != "nan"
        assert value.lower() not in ("inf", "+inf", "-inf")


# ── HTTP roundtrip via aiohttp_client ──────────────────────────────


@pytest.fixture
def metrics_app(monkeypatch):
    """Build a minimal aiohttp app with ``/metrics`` mounted and the
    collectors stubbed out."""
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch, ipn_drops={"replay": 2})

    app = web.Application()
    metrics.install_metrics_route(app, allowlist_env="127.0.0.1,::1")
    return app


async def test_metrics_endpoint_serves_allowed_ip(aiohttp_client, metrics_app):
    client = await aiohttp_client(metrics_app)
    resp = await client.get("/metrics")
    assert resp.status == 200
    assert resp.headers["Content-Type"].startswith("text/plain")
    body = await resp.text()
    assert 'meowassist_ipn_drops_total{reason="replay"} 2' in body


async def test_metrics_endpoint_rejects_disallowed_ip(aiohttp_client, monkeypatch):
    """An empty allowlist (e.g. operator typoed every entry) locks
    every request out with a 403."""
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch)

    app = web.Application()
    metrics.install_metrics_route(app, allowlist_env="")
    client = await aiohttp_client(app)

    resp = await client.get("/metrics")
    assert resp.status == 403


async def test_metrics_endpoint_rejects_outside_subnet(aiohttp_client, monkeypatch):
    """An allowlist that doesn't include localhost rejects the
    ``aiohttp_client`` (which connects from 127.0.0.1)."""
    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch)

    app = web.Application()
    # Restrict to a /32 that the test client isn't using.
    metrics.install_metrics_route(app, allowlist_env="10.99.0.1/32")
    client = await aiohttp_client(app)

    resp = await client.get("/metrics")
    assert resp.status == 403


def test_install_metrics_route_stashes_parsed_allowlist():
    app = web.Application()
    metrics.install_metrics_route(app, allowlist_env="10.0.0.0/8,192.168.0.0/16")
    parsed = app[metrics.APP_KEY_ALLOWLIST]
    assert ipaddress.ip_network("10.0.0.0/8") in parsed
    assert ipaddress.ip_network("192.168.0.0/16") in parsed


# ── Stage-15-Step-E #4 follow-up: per-key OpenRouter exposition ────


def _setup_three_key_pool(monkeypatch):
    """Set up a deterministic 3-key pool for the per-key tests.

    Mirrors ``tests/test_openrouter_keys._setup_three_keys`` but
    keeps the test helper local so a future split between unit and
    integration metric tests doesn't break across modules.
    """
    import openrouter_keys

    monkeypatch.setenv("OPENROUTER_API_KEY_1", "km0")
    monkeypatch.setenv("OPENROUTER_API_KEY_2", "km1")
    monkeypatch.setenv("OPENROUTER_API_KEY_3", "km2")
    # Force a clean reload so the env vars take effect.
    openrouter_keys._keys = []
    openrouter_keys._loaded = False
    openrouter_keys.clear_all_cooldowns()
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys.load_keys()


def test_render_metrics_emits_per_key_429_counter(monkeypatch):
    """The per-key 429 counter family appears in the rendered body
    with one row per pool slot that recorded a 429."""
    import openrouter_keys

    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch)
    _setup_three_key_pool(monkeypatch)
    openrouter_keys.mark_key_rate_limited("km0")
    openrouter_keys.mark_key_rate_limited("km2")
    openrouter_keys.mark_key_rate_limited("km2")

    body = metrics.render_metrics()
    assert "# HELP meowassist_openrouter_key_429_total" in body
    assert "# TYPE meowassist_openrouter_key_429_total counter" in body
    assert 'meowassist_openrouter_key_429_total{index="0"} 1' in body
    assert 'meowassist_openrouter_key_429_total{index="2"} 2' in body


def test_render_metrics_emits_per_key_fallback_counter(monkeypatch):
    """The per-key fallback counter family appears in the rendered body
    with one row per pool slot that absorbed a fallback."""
    import openrouter_keys

    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch)
    _setup_three_key_pool(monkeypatch)
    # User id 1 → sticky idx 1 → "km1"; mark "km1" hot, then call
    # key_for_user(1) — picker walks forward to idx 2 ("km2").
    openrouter_keys.mark_key_rate_limited("km1")
    openrouter_keys.key_for_user(1)

    body = metrics.render_metrics()
    assert "# HELP meowassist_openrouter_key_fallback_total" in body
    assert "# TYPE meowassist_openrouter_key_fallback_total counter" in body
    assert 'meowassist_openrouter_key_fallback_total{index="2"} 1' in body


def test_render_metrics_emits_per_key_cooldown_remaining_gauge(monkeypatch):
    """The cooldown-remaining gauge family renders one row per pool key.

    Available slots render as 0 so a PromQL ``> 0`` filter cleanly
    catches the cooled keys; the cooled slot renders the
    remaining seconds (close to the configured Retry-After).
    """
    import openrouter_keys

    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch)
    _setup_three_key_pool(monkeypatch)
    openrouter_keys.mark_key_rate_limited("km1", retry_after_secs=42.0)

    body = metrics.render_metrics()
    assert (
        "# HELP meowassist_openrouter_key_cooldown_remaining_seconds"
        in body
    )
    assert (
        "# TYPE meowassist_openrouter_key_cooldown_remaining_seconds gauge"
        in body
    )
    assert (
        'meowassist_openrouter_key_cooldown_remaining_seconds'
        '{index="0"} 0\n'
        in body
    )
    assert (
        'meowassist_openrouter_key_cooldown_remaining_seconds'
        '{index="2"} 0\n'
        in body
    )
    # The cooled slot must render a finite seconds value > 0
    # (the test runs in microseconds, so the deadline barely
    # decremented from 42 — assert it's in the (0, 42] range).
    for line in body.splitlines():
        if (
            line.startswith(
                "meowassist_openrouter_key_cooldown_remaining_seconds"
            )
            and 'index="1"' in line
        ):
            value = float(line.rsplit(" ", 1)[-1])
            assert 0.0 < value <= 42.0
            break
    else:
        pytest.fail(
            "no cooldown_remaining_seconds row for index=1 found"
        )


def test_render_metrics_per_key_counters_zero_when_pool_empty(monkeypatch):
    """No keys configured → the labelled counter families still emit
    the HELP/TYPE preamble (Prometheus rate(...) queries shouldn't
    blow up against an absent counter) but no data rows."""
    import openrouter_keys

    metrics.reset_loop_ticks_for_tests()
    _patch_collectors(monkeypatch)
    # Wipe the pool so there are no keys at all.
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    for i in range(1, 11):
        monkeypatch.delenv(f"OPENROUTER_API_KEY_{i}", raising=False)
    openrouter_keys._keys = []
    openrouter_keys._loaded = False
    openrouter_keys.clear_all_cooldowns()
    openrouter_keys.reset_key_counters_for_tests()

    body = metrics.render_metrics()
    assert "# HELP meowassist_openrouter_key_429_total" in body
    assert "# TYPE meowassist_openrouter_key_429_total counter" in body
    # No data rows for an empty pool.
    assert "meowassist_openrouter_key_429_total{" not in body
    assert "meowassist_openrouter_key_fallback_total{" not in body
    # No cooldown gauge rows either.
    assert (
        'meowassist_openrouter_key_cooldown_remaining_seconds{'
        not in body
    )
