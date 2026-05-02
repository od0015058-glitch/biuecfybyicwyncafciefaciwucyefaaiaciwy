"""fx_rates: live USD ↔ Iranian Toman exchange-rate ticker.

Stage-11-Step-A. The wallet is denominated in USD (an explicit
invariant — a user's balance must not lose purchasing power when
the rial swings). For Iranian users we want to *display* the same
balance in Toman and, when they top up, accept Toman as an input
currency (Stage-11-Step-B) or route the payment through a Rial card
gateway (Stage-11-Step-C). Both paths depend on a live, reasonably
fresh USD→Toman rate.

This module owns that rate:

* In-memory cache `(toman_per_usd, fetched_at, source)`; every
  consumer reads it through :func:`get_usd_to_toman_snapshot`.
* Background refresher :func:`refresh_usd_to_toman_loop` that polls
  the configured source every ``FX_REFRESH_INTERVAL_SECONDS``
  (default 10 minutes). Main.py spawns + cancels the loop
  alongside the other refreshers.
* DB-backed persistence (``fx_rates_snapshot`` table, migration
  0010) so a process restart starts with the last known-good rate
  rather than a cold cache (no rate → can't render Toman figures).
* **Cache preservation on source outage** (same defensive pattern
  as ``payments.refresh_min_amounts_once``): if a refresh returns
  ``None`` but we had a prior value, keep the prior and bump the
  attempt timestamp. A silent collapse to ``None`` during a
  Nobitex outage would either crash the wallet UI or — worse —
  flip the bot into a "can't convert" state where user top-ups
  get stuck at the entry screen.
* Admin DM on >``FX_RATE_ALERT_THRESHOLD_PERCENT`` movement between
  successive refreshes (default 10%). The rial is volatile but a
  10%-in-10-min move is news.

**Why Nobitex as the default source.** We need USD→Toman, not
USD→Rial. The two Iranian FX standards are:

1. `bonbast.com` — parallel-market rate, text-scraped (no official
   API, structure can break). Quoted in both rials and tomans.
2. `api.nobitex.ir/market/stats?srcCurrency=usdt&dstCurrency=rls` —
   crypto exchange rate for USDT/IRR, public JSON, no auth, heavy
   daily volume. USDT tracks USD within a fraction of a percent
   and this endpoint is what Iranian crypto users already trade at.

Nobitex is quoted in *rials*, so divide by 10 to get tomans. The
endpoint is env-configurable (``FX_RATE_ENDPOINT``) and the parser
is selectable via ``FX_RATE_SOURCE`` (``nobitex`` / ``bonbast`` /
``custom_static`` for testing), with ``FX_RATE_JSON_PATH`` letting
an operator point at any public JSON endpoint that returns a
single number or a nested path.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Any

import aiohttp

from bot_health import register_loop

log = logging.getLogger("bot.fx_rates")


# ---------------------------------------------------------------------
# Env knobs
# ---------------------------------------------------------------------

_DEFAULT_INTERVAL_SECONDS = 10 * 60
_DEFAULT_ALERT_THRESHOLD_PCT = 10.0
_DEFAULT_SOURCE = "nobitex"
_DEFAULT_ENDPOINT = (
    "https://api.nobitex.ir/market/stats?srcCurrency=usdt&dstCurrency=rls"
)
# Nobitex returns prices in rials; 1 toman = 10 rials.
_RIALS_PER_TOMAN = 10
# Sanity bounds on any rate we cache. The toman has floated between
# ~40 000 and ~120 000 per USD over the last several years. A source
# that returns 0, 3, or 3_000_000_000 is broken and we must NOT
# overwrite a real cached value with it.
_MIN_PLAUSIBLE_TOMAN = 10_000.0
_MAX_PLAUSIBLE_TOMAN = 1_000_000.0


def _parse_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    """Tolerant integer env parser with a hard floor.

    Stage-15-Step-E #8 bundled bug fix: previously this helper had
    no ``minimum`` floor, so a misconfigured
    ``FX_REFRESH_INTERVAL_SECONDS=0`` (a typo for ``60``) would
    busy-loop the FX refresher hammering the upstream API every
    iteration as fast as the network allowed — likely getting the
    API key rate-limited or banned. A negative value would silently
    degrade ``asyncio.sleep`` to a yield.

    The ``minimum`` floor closes that gap. Default ``minimum=1``
    mirrors the canonical pattern in
    :func:`pending_expiration._read_int_env`. Callers that legitimately
    want to allow ``0`` (or below) can opt out by passing
    ``minimum=0`` (or a negative).
    """
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.warning("%s=%r not an int; using default %d", name, raw, default)
        return default
    if value < minimum:
        log.warning(
            "%s=%d is below the minimum %d (would busy-loop the "
            "refresher); clamping",
            name, value, minimum,
        )
        return minimum
    return value


def _parse_float_env(name: str, default: float) -> float:
    """Tolerant float env parser: blank / malformed / non-finite → ``default``.

    Same regression class as ``model_discovery._parse_float_env``:
    ``float("nan")`` / ``float("inf")`` parse successfully but
    silently disable the threshold check at the call site (every
    comparison against NaN is ``False``; nothing finite exceeds Inf).
    Reject non-finite values explicitly so a bogus
    ``FX_RATE_ALERT_THRESHOLD_PERCENT=nan`` falls back to the
    default rather than turning the FX rate-move alert system off.
    """
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        log.warning("%s=%r not a float; using default %.2f", name, raw, default)
        return default
    if not math.isfinite(value):
        log.warning(
            "%s=%r parsed as non-finite (%s); using default %.2f",
            name, raw, value, default,
        )
        return default
    return value


def _get_interval_seconds() -> int:
    return _parse_int_env("FX_REFRESH_INTERVAL_SECONDS", _DEFAULT_INTERVAL_SECONDS)


def _get_alert_threshold_pct() -> float:
    return _parse_float_env(
        "FX_RATE_ALERT_THRESHOLD_PERCENT", _DEFAULT_ALERT_THRESHOLD_PCT
    )


def _get_source() -> str:
    return os.getenv("FX_RATE_SOURCE", _DEFAULT_SOURCE).strip().lower()


def _get_endpoint() -> str:
    return os.getenv("FX_RATE_ENDPOINT", _DEFAULT_ENDPOINT).strip()


# ---------------------------------------------------------------------
# Snapshot + cache
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class FxRateSnapshot:
    """One observation of the USD→Toman rate."""

    toman_per_usd: float
    fetched_at: float  # time.time() at fetch moment
    source: str  # "nobitex" / "bonbast" / "custom_static" / "db"

    def age_seconds(self) -> float:
        return max(0.0, time.time() - self.fetched_at)

    def is_stale(self, max_age_seconds: float | None = None) -> bool:
        """Return True when the snapshot is older than 4× the refresh
        cadence (default 40 min at 10-min cadence). Callers that want
        to render a "(approx)" marker in the wallet UI use this."""
        if max_age_seconds is None:
            max_age_seconds = 4 * _get_interval_seconds()
        return self.age_seconds() > max_age_seconds


# Single-process cache. The DB persists across restarts; this one
# just avoids a round-trip on every usd→toman conversion.
_cache: FxRateSnapshot | None = None
_cache_lock = asyncio.Lock()


def _is_plausible(value: float) -> bool:
    """Guard against upstream returning 0, NaN, or absurd numbers."""
    if not isinstance(value, (int, float)):
        return False
    if value != value:  # NaN check (NaN != NaN)
        return False
    if value <= 0 or value == float("inf") or value == float("-inf"):
        return False
    return _MIN_PLAUSIBLE_TOMAN <= value <= _MAX_PLAUSIBLE_TOMAN


# ---------------------------------------------------------------------
# Source parsers
# ---------------------------------------------------------------------


def _parse_nobitex(payload: dict[str, Any]) -> float | None:
    """Nobitex ``/market/stats`` response has shape::

        {"stats": {"usdt-rls": {"latest": "875000", ...}, ...}, ...}

    Price is a string in rials. Divide by 10 for tomans. Tolerant to
    shape changes — any missing hop returns ``None`` which triggers
    the cache-preservation branch in the refresher.
    """
    stats = payload.get("stats")
    if not isinstance(stats, dict):
        return None
    market = stats.get("usdt-rls")
    if not isinstance(market, dict):
        return None
    raw = market.get("latest")
    if raw is None:
        return None
    try:
        rials = float(raw)
    except (TypeError, ValueError):
        return None
    if rials <= 0:
        return None
    return rials / _RIALS_PER_TOMAN


def _parse_bonbast(payload: dict[str, Any]) -> float | None:
    """Bonbast (or compatible) payload shape::

        {"usd_sell": "875000", ...}  # tomans per USD

    Not the default — exposed only for operator override via
    ``FX_RATE_SOURCE=bonbast`` + ``FX_RATE_ENDPOINT=<your mirror>``.
    """
    raw = payload.get("usd_sell") or payload.get("usd") or payload.get("price")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _parse_json_path(payload: Any, path: str) -> float | None:
    """Walk a ``"foo.bar.baz"`` dotted path into a nested JSON payload
    and return the terminal value as a float. Returns ``None`` if any
    hop is missing or the terminal value isn't parseable."""
    cursor: Any = payload
    for segment in path.split("."):
        if isinstance(cursor, dict):
            cursor = cursor.get(segment)
        elif isinstance(cursor, list):
            try:
                cursor = cursor[int(segment)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    if cursor is None:
        return None
    try:
        return float(cursor)
    except (TypeError, ValueError):
        return None


def _parse_payload(payload: Any, source: str) -> float | None:
    """Dispatch payload-parsing on the configured source."""
    if source == "nobitex" and isinstance(payload, dict):
        return _parse_nobitex(payload)
    if source == "bonbast" and isinstance(payload, dict):
        return _parse_bonbast(payload)
    if source == "custom_json_path":
        path = os.getenv("FX_RATE_JSON_PATH", "price").strip()
        return _parse_json_path(payload, path)
    if source == "custom_static":
        # Debug / staging hook — lets us point the bot at a known
        # rate without a live source. Value comes from env.
        static = _parse_float_env("FX_RATE_STATIC_VALUE", 0.0)
        return static if static > 0 else None
    return None


# ---------------------------------------------------------------------
# Fetch + cache update
# ---------------------------------------------------------------------


async def _fetch_one() -> float | None:
    """One HTTP round-trip to the configured source. Returns the
    parsed toman-per-USD rate or ``None`` on any failure (network,
    non-2xx, malformed body, implausible value)."""
    source = _get_source()
    endpoint = _get_endpoint()
    if source == "custom_static":
        # No HTTP needed for the static source — used for tests and
        # staged staging setups. Still goes through the plausibility
        # guard below so an operator-typo like ``FX_RATE_STATIC_VALUE=5e9``
        # can't poison the cache.
        static_rate = _parse_payload({}, source)
        return static_rate if static_rate is not None and _is_plausible(static_rate) else None

    timeout = aiohttp.ClientTimeout(total=20)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(endpoint) as response:
                response.raise_for_status()
                try:
                    payload = await response.json(content_type=None)
                except (aiohttp.ContentTypeError, json.JSONDecodeError):
                    body = await response.text()
                    log.warning(
                        "FX source %s returned non-JSON body (%d chars); "
                        "keeping prior cache",
                        source, len(body),
                    )
                    return None
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("FX fetch from %s failed; keeping prior cache", endpoint)
        return None

    rate = _parse_payload(payload, source)
    if rate is None or not _is_plausible(rate):
        log.warning(
            "FX source %s returned implausible rate %r; keeping prior cache",
            source, rate,
        )
        return None
    return rate


async def refresh_usd_to_toman_once(bot: Any = None) -> FxRateSnapshot | None:
    """One refresh. Writes through to the cache AND the
    ``fx_rates_snapshot`` DB table. If ``bot`` is supplied and the
    new value differs from the prior cached value by more than
    ``FX_RATE_ALERT_THRESHOLD_PERCENT``, DM every admin.

    Returns the new snapshot on success, ``None`` on any failure
    (and preserves the existing cache — see module docstring).
    """
    global _cache
    prior = _cache
    async with _cache_lock:
        new_rate = await _fetch_one()
        if new_rate is None:
            return None
        snapshot = FxRateSnapshot(
            toman_per_usd=new_rate,
            fetched_at=time.time(),
            source=_get_source(),
        )
        _cache = snapshot

    # Best-effort DB persist. If the DB write fails we still have the
    # in-memory cache, and the next refresh will try again — but log
    # it so the operator sees a persistent DB problem.
    try:
        from database import db
        await db.upsert_fx_snapshot(
            toman_per_usd=snapshot.toman_per_usd,
            source=snapshot.source,
        )
    except Exception:
        log.exception("Failed to persist FX snapshot to DB; cache is in-memory only")

    if bot is not None and prior is not None:
        threshold = _get_alert_threshold_pct()
        delta_pct = (snapshot.toman_per_usd - prior.toman_per_usd) / prior.toman_per_usd * 100.0
        if abs(delta_pct) >= threshold:
            await _notify_admins_of_rate_move(bot, prior, snapshot, delta_pct)

    return snapshot


async def _notify_admins_of_rate_move(
    bot: Any,
    prior: FxRateSnapshot,
    current: FxRateSnapshot,
    delta_pct: float,
) -> int:
    """DM every admin about a large rate move. Per-admin fault
    isolation (same pattern as ``model_discovery.notify_admins``)."""
    from admin import get_admin_user_ids
    from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

    admin_ids = get_admin_user_ids()
    if not admin_ids:
        log.warning(
            "USD→Toman rate moved %.1f%% but ADMIN_USER_IDS is empty; "
            "nothing to notify", delta_pct,
        )
        return 0
    arrow = "↑" if delta_pct > 0 else "↓"
    text = (
        f"💱 USD→Toman rate moved {arrow}{abs(delta_pct):.1f}% since last check.\n\n"
        f"    was:  {prior.toman_per_usd:,.0f} TMN / USD "
        f"({time.strftime('%H:%M UTC', time.gmtime(prior.fetched_at))})\n"
        f"    now:  {current.toman_per_usd:,.0f} TMN / USD\n\n"
        f"Source: {current.source}"
    )
    sent = 0
    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, text, disable_web_page_preview=True)
            sent += 1
        except TelegramForbiddenError:
            log.info("Admin %d blocked the bot; skipping FX alert", admin_id)
        except TelegramAPIError:
            log.exception("Failed to DM admin %d about FX move", admin_id)
    return sent


@register_loop("fx_refresh", cadence_seconds=_DEFAULT_INTERVAL_SECONDS)
async def refresh_usd_to_toman_loop(
    bot: Any = None,
    *,
    interval_seconds: int | None = None,
) -> None:
    """Forever-loop wrapper. Spawned from ``main.py`` alongside the
    other refreshers. First pass runs immediately so the cache is
    warm before the first user reaches the top-up screen; subsequent
    passes wait ``interval_seconds`` (default 10 min).

    Swallows every exception except ``CancelledError`` so a
    network / source hiccup doesn't permanently stop the refresher.
    """
    # ``is not None`` (not ``or``) so a test-time ``interval_seconds=0``
    # tight loop works. ``0 or X == X`` would silently fall through to
    # the 600s default and hang the tests.
    interval = interval_seconds if interval_seconds is not None else _get_interval_seconds()
    while True:
        try:
            await refresh_usd_to_toman_once(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("FX refresher iteration failed; retrying")
        else:
            # Stage-15-Step-A: heartbeat for
            # ``meowassist_fx_refresh_last_run_epoch``.
            from metrics import record_loop_tick

            record_loop_tick("fx_refresh")
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise


# ---------------------------------------------------------------------
# Public accessors (read-only)
# ---------------------------------------------------------------------


async def _warm_cache_from_db() -> FxRateSnapshot | None:
    """If the in-memory cache is empty (cold start), seed it from the
    last known DB snapshot so we don't render the wallet UI without
    a rate for the first 10 minutes after a deploy."""
    global _cache
    if _cache is not None:
        return _cache
    try:
        from database import db
        persisted = await db.get_fx_snapshot()
    except Exception:
        log.exception("Failed to load FX snapshot from DB")
        return None
    if persisted is None:
        return None
    rate, fetched_at = persisted
    if not _is_plausible(rate):
        log.warning(
            "Persisted FX rate %r is outside the plausible band; ignoring",
            rate,
        )
        return None
    async with _cache_lock:
        if _cache is None:
            _cache = FxRateSnapshot(
                toman_per_usd=float(rate),
                fetched_at=fetched_at.timestamp() if hasattr(fetched_at, "timestamp") else float(fetched_at),
                source="db",
            )
        return _cache


async def get_usd_to_toman_snapshot() -> FxRateSnapshot | None:
    """Public read accessor. Returns the current cached snapshot, or
    falls back to the last DB snapshot on a cold cache. Returns
    ``None`` only when we have literally never observed a rate
    (first deploy, refresher hasn't run yet)."""
    if _cache is None:
        return await _warm_cache_from_db()
    return _cache


async def convert_usd_to_toman(amount_usd: float) -> float | None:
    """Convert a USD amount to Toman using the current snapshot.

    Returns ``None`` when no rate is known — callers (Stage-11-Step-B
    top-up UI, Stage-11-Step-D wallet display) must degrade
    gracefully by hiding the Toman figure rather than rendering
    ``0 TMN``.
    """
    snap = await get_usd_to_toman_snapshot()
    if snap is None:
        return None
    return amount_usd * snap.toman_per_usd


async def convert_toman_to_usd(amount_toman: float) -> float | None:
    """Inverse of :func:`convert_usd_to_toman`. Used by the
    Stage-11-Step-B top-up entry to go from user-typed Toman to the
    USD-denominated invoice amount."""
    snap = await get_usd_to_toman_snapshot()
    if snap is None or snap.toman_per_usd <= 0:
        return None
    return amount_toman / snap.toman_per_usd


def _reset_cache_for_tests() -> None:
    """Test-only helper — avoids import-order coupling between tests
    that touch the cache."""
    global _cache
    _cache = None
