"""Prometheus ``/metrics`` exposition for the bot's aiohttp server.

Stage-15-Step-A. Mounts a ``GET /metrics`` route on the same aiohttp
application that already hosts ``/nowpayments-webhook``,
``/tetrapay-webhook`` and the ``/admin/`` panel — one process, one
port, one less thing to deploy. The endpoint is gated by a
CIDR allowlist (``METRICS_IP_ALLOWLIST``, default ``127.0.0.1,::1``)
so a leaked URL doesn't expose internal counters publicly.

Exposition format is the Prometheus text format (no third-party
``prometheus_client`` dependency — we render the minimal subset of
gauges + counters we need by hand). Spec:
https://prometheus.io/docs/instrumenting/exposition_formats/

Public surface
--------------
* :func:`record_loop_tick` — called by every background loop on each
  successful pass. Stores ``time.time()`` keyed by loop name.
* :func:`get_loop_last_tick` — read-back accessor for tests / the
  rendered ``last_run_epoch`` gauges.
* :func:`reset_loop_ticks_for_tests` — tests-only.
* :func:`parse_ip_allowlist` — pure CIDR parser, exported for tests.
* :func:`render_metrics` — return the full text-format body.
* :func:`metrics_handler` — ``aiohttp`` handler.
* :func:`install_metrics_route` — mount ``GET /metrics`` on an app
  and wire the IP-allowlist middleware.

Bug-fix bundled with the same PR (Stage-15-Step-D #1): the crypto
currency picker no longer surfaces NowPayments tickers when
``NOWPAYMENTS_API_KEY`` is unset — see ``handlers._active_pay_currencies``.
"""

from __future__ import annotations

import ipaddress
import logging
import os
import time
from typing import Iterable

from aiohttp import web

log = logging.getLogger("bot.metrics")


# ── Loop heartbeat registry ────────────────────────────────────────
#
# Each background loop calls ``record_loop_tick("<name>")`` at the
# end of a *successful* pass (NOT inside the broad-except / sleep
# branch — a stuck reaper that swallows exceptions every iteration
# would otherwise look healthy). The corresponding gauge is rendered
# as ``meowassist_<name>_last_run_epoch`` in the exposition.
#
# A loop that never ticked once (cold-start window, or task crashed
# at the first iteration) renders ``0`` — Prometheus' typical
# ``time() - last_run_epoch > N`` alert expression treats epoch 0 as
# "infinitely stale" which is exactly what we want.
_LOOP_LAST_TICK: dict[str, float] = {}


# Loop names → metric-name fragment. Kept as an ordered tuple so the
# rendered output is stable (helpful for grep-friendly diffs of
# ``curl /metrics`` output across deploys).
_LOOP_METRIC_NAMES: tuple[str, ...] = (
    "min_amount_refresh",
    "fx_refresh",
    "model_discovery",
    "catalog_refresh",
    "pending_alert",
    "pending_reaper",
)


def record_loop_tick(name: str, *, ts: float | None = None) -> None:
    """Record a successful tick for the named loop.

    ``ts`` defaults to ``time.time()``. Tests pass a frozen value to
    pin the rendered gauge.
    """
    _LOOP_LAST_TICK[name] = ts if ts is not None else time.time()


def get_loop_last_tick(name: str) -> float | None:
    """Return the last successful-tick epoch for *name*, or ``None``."""
    return _LOOP_LAST_TICK.get(name)


def reset_loop_ticks_for_tests() -> None:
    """Clear the loop-tick registry. Tests-only."""
    _LOOP_LAST_TICK.clear()


# ── IP allowlist ───────────────────────────────────────────────────
#
# A leaked ``/metrics`` URL is a real (if low-severity) infoleak —
# the IPN drop counters tell an attacker "the bad-signature counter
# climbed by N, so my forged callbacks landed", and the disabled-
# models / gateways counts hint at operator activity. So we gate on
# IP, not on the admin auth cookie (this endpoint is for internal
# scrapers, not browsers).
#
# Default is localhost only — a sidecar Prometheus / VictoriaMetrics
# pod scrapes ``http://127.0.0.1:8080/metrics`` and that's it.
# Operators with split-host setups override
# ``METRICS_IP_ALLOWLIST=10.0.0.0/8,192.168.0.0/16``.
DEFAULT_METRICS_ALLOWLIST = "127.0.0.1,::1"

# Type alias: a parsed allowlist is a tuple of ``IPv4Network`` /
# ``IPv6Network``. ``ip_address in network`` is the membership test.
_AllowlistT = tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]


# aiohttp 3.9+ wants typed ``AppKey`` for ``app[...]`` storage
# instead of bare string keys (otherwise it emits ``NotAppKeyWarning``
# which the test runner upgrades to a failing assertion via the
# ``filterwarnings = error`` line in ``pytest.ini``). Mirrors the
# ``APP_KEY_*`` pattern in ``web_admin.py``.
APP_KEY_ALLOWLIST: web.AppKey = web.AppKey("metrics_allowlist", tuple)


def parse_ip_allowlist(raw: str) -> _AllowlistT:
    """Parse a comma-separated list of IPs / CIDRs into networks.

    A bare IP (``127.0.0.1``) is treated as ``/32`` (or ``/128``).
    Whitespace around entries is stripped. Empty entries are
    skipped. Malformed entries are logged and dropped — we prefer
    fail-soft over failing to boot a deploy with a typoed env var.
    """
    out: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for piece in (raw or "").split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            net = ipaddress.ip_network(piece, strict=False)
        except ValueError:
            log.warning(
                "METRICS_IP_ALLOWLIST: ignoring malformed entry %r", piece
            )
            continue
        out.append(net)
    return tuple(out)


def _client_ip(request: web.Request) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """Return ``request.remote`` parsed, or ``None`` if missing/unparseable.

    We deliberately do NOT consult ``X-Forwarded-For`` here — a
    public-facing reverse proxy can be tricked into spoofing a header
    that bypasses the allowlist. The metrics endpoint is meant to be
    scraped from inside the docker network where ``request.remote``
    is the trustworthy source.
    """
    raw = request.remote
    if not raw:
        return None
    try:
        return ipaddress.ip_address(raw)
    except ValueError:
        return None


def is_ip_allowed(
    request: web.Request, allowlist: _AllowlistT
) -> bool:
    """Return True if the request's source IP falls in *allowlist*."""
    if not allowlist:
        # Empty allowlist locks everyone out — a deliberate choice
        # over "empty == allow all" so a misconfigured env var fails
        # closed instead of silently exposing the endpoint publicly.
        return False
    ip = _client_ip(request)
    if ip is None:
        return False
    for net in allowlist:
        # An IPv4 address against an IPv6 network (or vice versa)
        # raises TypeError on membership; skip rather than crash so
        # one mismatched entry doesn't block all matches.
        try:
            if ip in net:
                return True
        except TypeError:
            continue
    return False


# ── Exposition rendering ───────────────────────────────────────────


def _format_help_and_type(
    name: str, help_text: str, metric_type: str
) -> list[str]:
    """Return the ``# HELP`` and ``# TYPE`` lines for a metric."""
    return [
        f"# HELP {name} {help_text}",
        f"# TYPE {name} {metric_type}",
    ]


def _escape_label_value(value: str) -> str:
    """Escape a Prometheus label value per the text-exposition spec.

    Per https://prometheus.io/docs/instrumenting/exposition_formats/
    label values must escape ``\\`` as ``\\\\``, ``"`` as ``\\"`` and
    newline as ``\\n``. The current callers (``_IPN_DROP_COUNTERS``,
    ``_TETRAPAY_DROP_COUNTERS``) only emit ASCII-safe identifiers like
    ``"bad_signature"`` so escaping is a no-op for them — but a
    future caller passing an arbitrary key (e.g. an OpenRouter model
    id with a literal ``"`` from a malicious or buggy upstream
    response) would otherwise produce a ``meowassist_x{label="bad
    "value"} 1`` line that Prometheus' parser rejects with a
    ``unexpected character ...`` error and the whole scrape fails —
    blanking every metric in the response. A quick escape pass keeps
    the endpoint robust against that class of poisoning.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _format_labelled_counter(
    metric_name: str,
    help_text: str,
    label_key: str,
    counters: dict[str, int],
) -> list[str]:
    """Render a labelled counter family (one row per label value).

    A counter with no rows still emits the HELP/TYPE preamble — keeps
    Prometheus happy when an operator runs a ``rate(...)`` query
    against a counter that has not yet been incremented this process
    lifetime.

    Label values are escaped per the exposition spec
    (:func:`_escape_label_value`) so a future caller passing a label
    that contains ``"`` / ``\\`` / newline can't poison the rendered
    body and break the whole ``/metrics`` response.
    """
    lines = _format_help_and_type(metric_name, help_text, "counter")
    for label_value, count in sorted(counters.items()):
        escaped = _escape_label_value(label_value)
        # Counter values are non-negative integers in our case, but
        # we render via int() to defend against a future caller
        # passing a float.
        lines.append(
            f'{metric_name}{{{label_key}="{escaped}"}} {int(count)}'
        )
    return lines


def _format_gauge(metric_name: str, help_text: str, value: float) -> list[str]:
    """Render a single un-labelled gauge.

    Non-finite values (``NaN``, ``+/-inf``) render as ``0`` rather
    than tripping Prometheus' parser — this matches the same NaN
    defence the wallet display uses elsewhere in the codebase
    (see ``wallet_display.format_balance_block``).
    """
    if value != value or value in (float("inf"), float("-inf")):
        rendered = "0"
    elif float(value).is_integer():
        rendered = str(int(value))
    else:
        # ``str(float)`` returns the shortest round-trip-safe
        # repr (e.g. ``str(1777562092.857)`` → ``'1777562092.857'``).
        # We deliberately do NOT use ``f"{value:g}"`` here — Python's
        # ``g`` format defaults to 6 significant digits, which on a
        # current Unix epoch (~1.78e9) collapses to ``1.77756e+09``
        # and erases ~2 000 s of precision. That's a fatal bug for
        # the heartbeat gauges: a stuck-loop alert tuned for a
        # 15-minute staleness window would misfire on the
        # ``:g``-induced ~35-minute precision error alone.
        rendered = str(float(value))
    return [
        *_format_help_and_type(metric_name, help_text, "gauge"),
        f"{metric_name} {rendered}",
    ]


def render_metrics() -> str:
    """Return the full Prometheus exposition body as a string.

    Each metric is collected lazily inside this function so a
    failed import in one of the source modules doesn't break the
    whole endpoint — but we still let the import errors propagate
    if they happen so the operator sees them.

    The collected snapshot represents a *point-in-time* view: a
    counter incrementing while we render is consistent because
    ``dict()`` of the in-process counter is a shallow copy.
    """
    # Imported lazily so this module can be imported by tests that
    # don't have the rest of the bot's surface area available
    # (e.g. asyncpg pool, telegram tokens). Same pattern admin.py
    # uses for its strings imports.
    from admin_toggles import get_disabled_gateways, get_disabled_models
    from openrouter_keys import key_count
    from payments import get_ipn_drop_counters
    from rate_limit import chat_inflight_count
    from tetrapay import get_tetrapay_drop_counters

    parts: list[str] = []

    parts.extend(
        _format_labelled_counter(
            "meowassist_ipn_drops_total",
            "NowPayments IPN POSTs dropped, broken down by reason.",
            "reason",
            get_ipn_drop_counters(),
        )
    )

    parts.extend(
        _format_labelled_counter(
            "meowassist_tetrapay_drops_total",
            "TetraPay IPN POSTs dropped, broken down by reason.",
            "reason",
            get_tetrapay_drop_counters(),
        )
    )

    for loop_name in _LOOP_METRIC_NAMES:
        parts.extend(
            _format_gauge(
                f"meowassist_{loop_name}_last_run_epoch",
                (
                    f"Unix epoch of the last successful {loop_name} loop "
                    "iteration; 0 means the loop has not yet ticked."
                ),
                _LOOP_LAST_TICK.get(loop_name, 0.0),
            )
        )

    parts.extend(
        _format_gauge(
            "meowassist_chat_inflight_active",
            "Number of users currently holding an in-flight AI chat slot.",
            float(chat_inflight_count()),
        )
    )

    parts.extend(
        _format_gauge(
            "meowassist_disabled_models_count",
            "Number of OpenRouter models disabled by the admin panel.",
            float(len(get_disabled_models())),
        )
    )

    parts.extend(
        _format_gauge(
            "meowassist_disabled_gateways_count",
            "Number of payment gateways disabled by the admin panel.",
            float(len(get_disabled_gateways())),
        )
    )

    parts.extend(
        _format_gauge(
            "meowassist_openrouter_keys_count",
            "Number of OpenRouter API keys loaded into the round-robin pool.",
            float(key_count()),
        )
    )

    # Prometheus requires a trailing newline on the body.
    return "\n".join(parts) + "\n"


# ── aiohttp wiring ─────────────────────────────────────────────────


async def metrics_handler(request: web.Request) -> web.Response:
    """``GET /metrics`` handler. IP-allowlist gated."""
    allowlist: _AllowlistT = request.app.get(APP_KEY_ALLOWLIST, ())
    if not is_ip_allowed(request, allowlist):
        # 403 (NOT 401): this isn't a credentials problem the client
        # could fix by retrying with a header. The IP itself is the
        # gate.
        log.info(
            "metrics: rejected request from %r (not in allowlist)",
            request.remote,
        )
        return web.Response(status=403, text="forbidden\n")

    body = render_metrics()
    # Prometheus' default scrape config accepts the legacy
    # ``text/plain; version=0.0.4`` content type — we set it
    # explicitly so a non-Prometheus client that just ``curl``s the
    # endpoint still sees a sensible MIME type.
    return web.Response(
        status=200,
        text=body,
        content_type="text/plain",
        charset="utf-8",
    )


def install_metrics_route(
    app: web.Application,
    *,
    allowlist_env: str | None = None,
) -> None:
    """Register ``GET /metrics`` on *app* and stash the allowlist.

    ``allowlist_env`` lets tests inject a parsed allowlist directly;
    when omitted, the function reads ``METRICS_IP_ALLOWLIST`` from
    the environment (default ``127.0.0.1,::1``).
    """
    raw = (
        allowlist_env
        if allowlist_env is not None
        else os.getenv("METRICS_IP_ALLOWLIST", DEFAULT_METRICS_ALLOWLIST)
    )
    allowlist = parse_ip_allowlist(raw)
    if not allowlist:
        log.warning(
            "METRICS_IP_ALLOWLIST resolved to an empty allowlist; "
            "/metrics will refuse every request."
        )
    app[APP_KEY_ALLOWLIST] = allowlist
    app.router.add_get("/metrics", metrics_handler)
    log.info(
        "metrics: /metrics endpoint mounted (allowlist=%s)",
        ",".join(str(n) for n in allowlist) or "<empty>",
    )


__all__: Iterable[str] = (
    "DEFAULT_METRICS_ALLOWLIST",
    "get_loop_last_tick",
    "install_metrics_route",
    "is_ip_allowed",
    "metrics_handler",
    "parse_ip_allowlist",
    "record_loop_tick",
    "render_metrics",
    "reset_loop_ticks_for_tests",
)
