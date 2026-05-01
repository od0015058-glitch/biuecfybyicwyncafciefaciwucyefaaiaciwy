"""Tests for :mod:`fx_rates` — USD↔Toman live ticker.

Covers:

* Nobitex / bonbast / custom_json_path parser dispatch + shape tolerance.
* Cache preservation on fetch failure (parity with
  ``payments.refresh_min_amounts_once`` — without this, a silent
  collapse to ``None`` during a source outage would break the
  wallet UI and top-up flow).
* Plausibility bounds (an upstream returning 0 or 3e9 must NOT
  overwrite a real cached value).
* Admin DM fan-out on >threshold moves (per-admin fault isolation).
* ``get_usd_to_toman_snapshot`` cold-start fallback to DB.
* ``convert_*`` graceful-``None`` return when rate unknown.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

import fx_rates


@pytest.fixture(autouse=True)
def _reset_cache_per_test():
    fx_rates._reset_cache_for_tests()
    yield
    fx_rates._reset_cache_for_tests()


# ---------------------------------------------------------------------
# Parser dispatch
# ---------------------------------------------------------------------


def test_parse_nobitex_well_formed_rial_payload_converts_to_toman():
    payload = {"stats": {"usdt-rls": {"latest": "875000"}}, "global": {}}
    # 875 000 rials / 10 = 87 500 tomans per USD
    assert fx_rates._parse_nobitex(payload) == 87_500.0


def test_parse_nobitex_missing_market_returns_none():
    assert fx_rates._parse_nobitex({"stats": {}}) is None


def test_parse_nobitex_malformed_latest_returns_none():
    assert fx_rates._parse_nobitex(
        {"stats": {"usdt-rls": {"latest": "not-a-number"}}}
    ) is None


def test_parse_nobitex_zero_latest_returns_none():
    # A "zero rate" from the source is as bad as no rate — we must
    # not cache it; the cache-preservation branch kicks in instead.
    assert fx_rates._parse_nobitex(
        {"stats": {"usdt-rls": {"latest": "0"}}}
    ) is None


def test_parse_bonbast_usd_sell_field():
    assert fx_rates._parse_bonbast({"usd_sell": "95000"}) == 95_000.0


def test_parse_bonbast_missing_field_returns_none():
    assert fx_rates._parse_bonbast({"eur_sell": "100000"}) is None


def test_parse_json_path_walks_dotted_paths():
    payload = {"data": {"rate": {"value": 82_500}}}
    assert fx_rates._parse_json_path(payload, "data.rate.value") == 82_500.0


def test_parse_json_path_missing_hop_returns_none():
    assert fx_rates._parse_json_path({"data": {}}, "data.rate.value") is None


def test_parse_json_path_list_index():
    payload = {"rates": [{"v": 50_000}, {"v": 60_000}]}
    assert fx_rates._parse_json_path(payload, "rates.1.v") == 60_000.0


# ---------------------------------------------------------------------
# Plausibility guard
# ---------------------------------------------------------------------


@pytest.mark.parametrize("rate", [0, -5.0, float("nan"), float("inf"), 0.5, 5_000_000_000])
def test_is_plausible_rejects_garbage(rate):
    assert fx_rates._is_plausible(rate) is False


@pytest.mark.parametrize("rate", [50_000, 87_500.0, 500_000.0])
def test_is_plausible_accepts_real_rates(rate):
    assert fx_rates._is_plausible(rate) is True


# ---------------------------------------------------------------------
# Refresh: cache preservation
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_once_populates_cache_on_success(monkeypatch):
    monkeypatch.setattr(fx_rates, "_fetch_one", AsyncMock(return_value=87_500.0))
    with patch.object(fx_rates, "db", MagicMock(upsert_fx_snapshot=AsyncMock())) if False else patch(
        "database.db.upsert_fx_snapshot", AsyncMock()
    ):
        snap = await fx_rates.refresh_usd_to_toman_once(bot=None)
    assert snap is not None
    assert snap.toman_per_usd == 87_500.0
    cached = await fx_rates.get_usd_to_toman_snapshot()
    assert cached is not None and cached.toman_per_usd == 87_500.0


@pytest.mark.asyncio
async def test_refresh_once_preserves_cache_when_source_fails(monkeypatch):
    """If a prior good value is cached and the next fetch returns
    ``None`` (API outage / malformed body), the cache MUST NOT be
    cleared. A silent collapse would either crash the wallet UI or
    mislead users into thinking the bot has no rate."""
    with patch("database.db.upsert_fx_snapshot", AsyncMock()):
        monkeypatch.setattr(fx_rates, "_fetch_one", AsyncMock(return_value=87_500.0))
        await fx_rates.refresh_usd_to_toman_once(bot=None)

        # Now simulate an outage.
        monkeypatch.setattr(fx_rates, "_fetch_one", AsyncMock(return_value=None))
        out = await fx_rates.refresh_usd_to_toman_once(bot=None)
    assert out is None
    cached = await fx_rates.get_usd_to_toman_snapshot()
    assert cached is not None
    assert cached.toman_per_usd == 87_500.0


@pytest.mark.asyncio
async def test_refresh_once_db_write_failure_does_not_crash_refresh(monkeypatch):
    """A DB outage must NOT take out the in-memory cache update —
    we still want the running process to have a fresh rate, and the
    next refresh will try the DB write again."""
    monkeypatch.setattr(fx_rates, "_fetch_one", AsyncMock(return_value=95_000.0))
    with patch("database.db.upsert_fx_snapshot", AsyncMock(side_effect=RuntimeError("db down"))):
        snap = await fx_rates.refresh_usd_to_toman_once(bot=None)
    assert snap is not None and snap.toman_per_usd == 95_000.0
    cached = await fx_rates.get_usd_to_toman_snapshot()
    assert cached is not None and cached.toman_per_usd == 95_000.0


# ---------------------------------------------------------------------
# Admin DM on threshold crossing
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rate_move_above_threshold_notifies_admins(monkeypatch):
    monkeypatch.setenv("FX_RATE_ALERT_THRESHOLD_PERCENT", "10")
    monkeypatch.setattr(fx_rates, "_fetch_one", AsyncMock(return_value=87_500.0))
    bot = MagicMock()
    bot.send_message = AsyncMock()
    with patch("database.db.upsert_fx_snapshot", AsyncMock()), \
         patch("admin.get_admin_user_ids", return_value=[111, 222]):
        await fx_rates.refresh_usd_to_toman_once(bot=bot)  # seeds cache, no DM

        # 20% jump — well above 10% threshold.
        monkeypatch.setattr(fx_rates, "_fetch_one", AsyncMock(return_value=105_000.0))
        await fx_rates.refresh_usd_to_toman_once(bot=bot)

    assert bot.send_message.call_count == 2  # one per admin
    sent_to = {call.args[0] for call in bot.send_message.call_args_list}
    assert sent_to == {111, 222}
    body = bot.send_message.call_args.args[1]
    assert "↑20.0%" in body
    assert "87,500" in body and "105,000" in body


@pytest.mark.asyncio
async def test_rate_move_below_threshold_is_silent(monkeypatch):
    monkeypatch.setenv("FX_RATE_ALERT_THRESHOLD_PERCENT", "10")
    monkeypatch.setattr(fx_rates, "_fetch_one", AsyncMock(return_value=87_500.0))
    bot = MagicMock(send_message=AsyncMock())
    with patch("database.db.upsert_fx_snapshot", AsyncMock()), \
         patch("admin.get_admin_user_ids", return_value=[111]):
        await fx_rates.refresh_usd_to_toman_once(bot=bot)  # seeds cache

        # 5% move — below 10% threshold.
        monkeypatch.setattr(fx_rates, "_fetch_one", AsyncMock(return_value=91_875.0))
        await fx_rates.refresh_usd_to_toman_once(bot=bot)

    bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_admin_dm_fault_isolation_blocked_admin_does_not_break_fanout(monkeypatch):
    """If admin 111 blocked the bot, admin 222 still gets the alert."""
    monkeypatch.setenv("FX_RATE_ALERT_THRESHOLD_PERCENT", "10")
    monkeypatch.setattr(fx_rates, "_fetch_one", AsyncMock(return_value=87_500.0))

    bot = MagicMock()
    bot.send_message = AsyncMock(
        side_effect=[TelegramForbiddenError(method=None, message="blocked"), None]
    )
    with patch("database.db.upsert_fx_snapshot", AsyncMock()), \
         patch("admin.get_admin_user_ids", return_value=[111, 222]):
        await fx_rates.refresh_usd_to_toman_once(bot=bot)  # seeds

        monkeypatch.setattr(fx_rates, "_fetch_one", AsyncMock(return_value=105_000.0))
        await fx_rates.refresh_usd_to_toman_once(bot=bot)

    assert bot.send_message.call_count == 2  # tried both, one blocked


# ---------------------------------------------------------------------
# Public accessors
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_snapshot_cold_start_warms_from_db():
    """Fresh process: cache is empty. ``get_usd_to_toman_snapshot``
    must fall back to the DB-persisted snapshot so the wallet UI
    has a rate immediately on boot rather than waiting ~10 minutes."""
    stored_rate = 88_000.0
    stored_at = dt.datetime(2026, 4, 29, 12, 0, 0, tzinfo=dt.timezone.utc)
    with patch(
        "database.db.get_fx_snapshot",
        AsyncMock(return_value=(stored_rate, stored_at)),
    ):
        snap = await fx_rates.get_usd_to_toman_snapshot()
    assert snap is not None
    assert snap.toman_per_usd == stored_rate
    assert snap.source == "db"


@pytest.mark.asyncio
async def test_get_snapshot_cold_start_db_empty_returns_none():
    with patch("database.db.get_fx_snapshot", AsyncMock(return_value=None)):
        snap = await fx_rates.get_usd_to_toman_snapshot()
    assert snap is None


@pytest.mark.asyncio
async def test_convert_usd_to_toman_basic():
    fx_rates._cache = fx_rates.FxRateSnapshot(
        toman_per_usd=90_000.0, fetched_at=time.time(), source="nobitex"
    )
    assert await fx_rates.convert_usd_to_toman(2.0) == 180_000.0


@pytest.mark.asyncio
async def test_convert_toman_to_usd_basic():
    fx_rates._cache = fx_rates.FxRateSnapshot(
        toman_per_usd=100_000.0, fetched_at=time.time(), source="nobitex"
    )
    assert await fx_rates.convert_toman_to_usd(400_000.0) == 4.0


@pytest.mark.asyncio
async def test_convert_returns_none_when_no_rate_known():
    with patch("database.db.get_fx_snapshot", AsyncMock(return_value=None)):
        assert await fx_rates.convert_usd_to_toman(2.0) is None
        assert await fx_rates.convert_toman_to_usd(180_000) is None


def test_snapshot_is_stale_uses_configured_cadence(monkeypatch):
    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "600")  # 10-min cadence
    fresh = fx_rates.FxRateSnapshot(100_000, time.time(), "nobitex")
    old = fx_rates.FxRateSnapshot(100_000, time.time() - 10 * 3600, "nobitex")
    assert fresh.is_stale() is False
    assert old.is_stale() is True


# ---------------------------------------------------------------------
# Fetch error handling
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_one_returns_none_on_http_failure(monkeypatch):
    """A network error / 5xx / DNS blip must return None (NOT raise)
    so the caller can take the cache-preservation branch."""
    monkeypatch.setenv("FX_RATE_SOURCE", "nobitex")

    class _BrokenSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def get(self, url):
            raise RuntimeError("network is down")

    monkeypatch.setattr("aiohttp.ClientSession", lambda **kw: _BrokenSession())
    assert await fx_rates._fetch_one() is None


@pytest.mark.asyncio
async def test_fetch_one_static_source_returns_env_value(monkeypatch):
    monkeypatch.setenv("FX_RATE_SOURCE", "custom_static")
    monkeypatch.setenv("FX_RATE_STATIC_VALUE", "95000")
    assert await fx_rates._fetch_one() == 95_000.0


@pytest.mark.asyncio
async def test_fetch_one_rejects_implausible_value(monkeypatch):
    """If the source returns a value outside the plausibility band
    (e.g. 5 or 5e9), the cache must NOT be overwritten — the fetch
    returns None and the cache-preservation branch kicks in."""
    monkeypatch.setenv("FX_RATE_SOURCE", "custom_static")
    monkeypatch.setenv("FX_RATE_STATIC_VALUE", "5000000000")
    assert await fx_rates._fetch_one() is None


# ---------------------------------------------------------------------
# DB method shapes (catch SQL regressions)
# ---------------------------------------------------------------------


def test_upsert_fx_snapshot_uses_single_row_upsert_pattern():
    """Simple textual check: the SQL upserts into id=1 and uses
    ``ON CONFLICT (id) DO UPDATE``. Prevents a future refactor from
    quietly turning this into a multi-row append-only table, which
    would break ``get_fx_snapshot``'s "WHERE id = 1" read."""
    import database
    import inspect
    src = inspect.getsource(database.Database.upsert_fx_snapshot)
    assert "fx_rates_snapshot" in src
    assert "VALUES (1," in src
    assert "ON CONFLICT (id) DO UPDATE" in src


# ---------------------------------------------------------------------
# _parse_float_env — NaN / Inf guard (Stage-15-Step-E #6 bundled bug fix)
# ---------------------------------------------------------------------


class TestParseFloatEnvNonFiniteGuard:
    """Guard against ``FX_RATE_ALERT_THRESHOLD_PERCENT=nan`` (or
    ``inf``) silently disabling the rate-move alert.

    ``float("nan")`` parses successfully; before the fix the function
    returned the NaN through unchanged, and the call site
    (``abs(delta) >= NaN`` is always ``False``) silently dropped
    every alert. Same regression class fixed in
    ``model_discovery._parse_float_env``.
    """

    def test_nan_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("FX_TEST_NAN", "nan")
        assert fx_rates._parse_float_env("FX_TEST_NAN", 12.5) == 12.5

    def test_uppercase_nan_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("FX_TEST_NAN_U", "NaN")
        assert fx_rates._parse_float_env("FX_TEST_NAN_U", 7.0) == 7.0

    def test_inf_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("FX_TEST_INF", "inf")
        assert fx_rates._parse_float_env("FX_TEST_INF", 12.5) == 12.5

    def test_negative_inf_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("FX_TEST_NEG_INF", "-inf")
        assert fx_rates._parse_float_env("FX_TEST_NEG_INF", 12.5) == 12.5

    def test_finite_value_passes_through(self, monkeypatch):
        monkeypatch.setenv("FX_TEST_OK", "8.5")
        assert fx_rates._parse_float_env("FX_TEST_OK", 12.5) == 8.5

    def test_blank_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("FX_TEST_BLANK", "")
        assert fx_rates._parse_float_env("FX_TEST_BLANK", 12.5) == 12.5

    def test_garbage_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("FX_TEST_GARBAGE", "twelve")
        assert fx_rates._parse_float_env("FX_TEST_GARBAGE", 12.5) == 12.5

    def test_negative_finite_passes_through(self, monkeypatch):
        # Negative is a *semantic* concern at the call site (a
        # negative threshold turns "alert on big moves" into "alert
        # on every move"), not a parser concern. The parser is
        # generic; the call site is responsible for clamping. This
        # test pins the contract so a future tightening of the
        # parser is a deliberate decision rather than a stealth
        # behaviour change.
        monkeypatch.setenv("FX_TEST_NEG", "-5")
        assert fx_rates._parse_float_env("FX_TEST_NEG", 12.5) == -5.0


class TestParseFloatEnvNonFiniteGuardSilencesRateMoveAlert:
    """End-to-end pin: ``FX_RATE_ALERT_THRESHOLD_PERCENT=nan`` must
    fall back to the default (5%) — NOT silently disable the alert.

    The pre-fix behaviour was that the env value passed through as
    NaN, the threshold check ``abs(delta) >= NaN`` returned ``False``
    for every model on every poll, and admins were never DM'd about
    a rate move. With the guard in place a misconfigured env value
    falls back to the default and the alert system keeps working.
    """

    @pytest.mark.asyncio
    async def test_nan_threshold_env_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("FX_RATE_ALERT_THRESHOLD_PERCENT", "nan")
        monkeypatch.setattr(fx_rates, "_fetch_one", AsyncMock(return_value=87_500.0))
        bot = MagicMock(send_message=AsyncMock())
        with patch("database.db.upsert_fx_snapshot", AsyncMock()), \
             patch("admin.get_admin_user_ids", return_value=[111]):
            await fx_rates.refresh_usd_to_toman_once(bot=bot)  # seed cache
            # 20% jump — well above the *default* 5% threshold,
            # which is what we should fall back to when env=nan.
            monkeypatch.setattr(
                fx_rates, "_fetch_one", AsyncMock(return_value=105_000.0)
            )
            await fx_rates.refresh_usd_to_toman_once(bot=bot)
        # The DM fired → fallback worked → bug is fixed.
        bot.send_message.assert_called()
