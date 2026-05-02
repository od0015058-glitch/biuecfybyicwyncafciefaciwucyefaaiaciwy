"""Tests for ``fx_refresh_config`` — DB-backed override for
FX_REFRESH_INTERVAL_SECONDS.

Stage-15-Step-E #10b row 24.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import fx_refresh_config as frc


# ── fixtures ─────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    """Clear module-level cache before each test."""
    monkeypatch.setattr(frc, "_FX_REFRESH_INTERVAL_OVERRIDE", None)
    monkeypatch.delenv("FX_REFRESH_INTERVAL_SECONDS", raising=False)
    yield


# ── coercion ─────────────────────────────────────────────────────


class TestCoercion:
    @pytest.mark.parametrize("value,expected", [
        (60, 60),
        (600, 600),
        (3600, 3600),
        (86400, 86400),
        ("60", 60),
        ("600", 600),
        ("86400", 86400),
        (60.0, 60),
        (600.0, 600),
    ])
    def test_happy(self, value, expected):
        assert frc._coerce_fx_refresh_interval(value) == expected

    @pytest.mark.parametrize("value", [
        True,
        False,
        "abc",
        "",
        None,
        float("nan"),
        float("inf"),
        float("-inf"),
        59,         # below min
        86401,      # above max
        0,
        -1,
        60.5,       # non-integer float
        "60.5",     # non-integer float string
        "60abc",    # garbage suffix
        [60],       # list not supported
        {"value": 60},
    ])
    def test_rejection(self, value):
        assert frc._coerce_fx_refresh_interval(value) is None

    def test_whitespace_string_accepted(self):
        """Float() tolerates whitespace; coercer mirrors that
        because callers (web_admin) already do their own ``.strip()``
        on form values."""
        assert frc._coerce_fx_refresh_interval(" 60 ") == 60
        assert frc._coerce_fx_refresh_interval("\t600\n") == 600


# ── override accessors ───────────────────────────────────────────


class TestOverrideAccessors:
    def test_initial(self):
        assert frc.get_fx_refresh_interval_override() is None

    def test_set_and_get(self):
        frc.set_fx_refresh_interval_override(1800)
        assert frc.get_fx_refresh_interval_override() == 1800

    def test_set_rejects_bool(self):
        with pytest.raises(ValueError, match="not bool"):
            frc.set_fx_refresh_interval_override(True)
        with pytest.raises(ValueError, match="not bool"):
            frc.set_fx_refresh_interval_override(False)

    def test_set_rejects_below_min(self):
        with pytest.raises(ValueError):
            frc.set_fx_refresh_interval_override(59)

    def test_set_rejects_above_max(self):
        with pytest.raises(ValueError):
            frc.set_fx_refresh_interval_override(86401)

    def test_set_rejects_zero(self):
        with pytest.raises(ValueError):
            frc.set_fx_refresh_interval_override(0)

    def test_set_rejects_negative(self):
        with pytest.raises(ValueError):
            frc.set_fx_refresh_interval_override(-60)

    def test_set_rejects_non_integer_float(self):
        with pytest.raises(ValueError):
            frc.set_fx_refresh_interval_override(60.5)  # type: ignore[arg-type]

    def test_set_accepts_min(self):
        frc.set_fx_refresh_interval_override(60)
        assert frc.get_fx_refresh_interval_override() == 60

    def test_set_accepts_max(self):
        frc.set_fx_refresh_interval_override(86400)
        assert frc.get_fx_refresh_interval_override() == 86400

    def test_set_accepts_string_int(self):
        frc.set_fx_refresh_interval_override("3600")  # type: ignore[arg-type]
        assert frc.get_fx_refresh_interval_override() == 3600

    def test_clear_returns_true_when_active(self):
        frc.set_fx_refresh_interval_override(1800)
        assert frc.clear_fx_refresh_interval_override() is True

    def test_clear_returns_false_when_none(self):
        assert frc.clear_fx_refresh_interval_override() is False

    def test_clear_resets(self):
        frc.set_fx_refresh_interval_override(1800)
        frc.clear_fx_refresh_interval_override()
        assert frc.get_fx_refresh_interval_override() is None


# ── resolution ───────────────────────────────────────────────────


class TestResolution:
    def test_default(self):
        assert frc.get_fx_refresh_interval_seconds() == 600
        assert frc.get_fx_refresh_interval_source() == "default"

    def test_env_wins(self, monkeypatch):
        monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "3600")
        assert frc.get_fx_refresh_interval_seconds() == 3600
        assert frc.get_fx_refresh_interval_source() == "env"

    def test_override_wins(self, monkeypatch):
        monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "3600")
        frc.set_fx_refresh_interval_override(1800)
        assert frc.get_fx_refresh_interval_seconds() == 1800
        assert frc.get_fx_refresh_interval_source() == "db"

    def test_invalid_env_falls_through(self, monkeypatch):
        monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "garbage")
        assert frc.get_fx_refresh_interval_seconds() == 600
        assert frc.get_fx_refresh_interval_source() == "default"

    def test_below_min_env_falls_through(self, monkeypatch):
        monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "10")
        assert frc.get_fx_refresh_interval_seconds() == 600
        assert frc.get_fx_refresh_interval_source() == "default"

    def test_above_max_env_falls_through(self, monkeypatch):
        monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "999999")
        assert frc.get_fx_refresh_interval_seconds() == 600
        assert frc.get_fx_refresh_interval_source() == "default"

    def test_override_after_clear_falls_back_to_env(self, monkeypatch):
        monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "1200")
        frc.set_fx_refresh_interval_override(60)
        assert frc.get_fx_refresh_interval_seconds() == 60
        frc.clear_fx_refresh_interval_override()
        assert frc.get_fx_refresh_interval_seconds() == 1200
        assert frc.get_fx_refresh_interval_source() == "env"


# ── refresh from DB ──────────────────────────────────────────────


class TestRefreshFromDB:
    @pytest.mark.asyncio
    async def test_none_db(self):
        result = await frc.refresh_fx_refresh_interval_override_from_db(None)
        assert result is None

    @pytest.mark.asyncio
    async def test_none_db_keeps_existing_cache(self):
        frc.set_fx_refresh_interval_override(1800)
        result = await frc.refresh_fx_refresh_interval_override_from_db(None)
        assert result == 1800
        assert frc.get_fx_refresh_interval_override() == 1800

    @pytest.mark.asyncio
    async def test_no_row_clears(self):
        frc.set_fx_refresh_interval_override(1800)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value=None)
        result = await frc.refresh_fx_refresh_interval_override_from_db(
            db_mock
        )
        assert result is None
        assert frc.get_fx_refresh_interval_override() is None

    @pytest.mark.asyncio
    async def test_valid_row(self):
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="3600")
        result = await frc.refresh_fx_refresh_interval_override_from_db(
            db_mock
        )
        assert result == 3600
        assert frc.get_fx_refresh_interval_override() == 3600

    @pytest.mark.asyncio
    async def test_valid_int_row(self):
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value=7200)
        result = await frc.refresh_fx_refresh_interval_override_from_db(
            db_mock
        )
        assert result == 7200

    @pytest.mark.asyncio
    async def test_malformed_clears(self):
        frc.set_fx_refresh_interval_override(1800)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="garbage")
        result = await frc.refresh_fx_refresh_interval_override_from_db(
            db_mock
        )
        assert result is None
        assert frc.get_fx_refresh_interval_override() is None

    @pytest.mark.asyncio
    async def test_below_min_clears(self):
        frc.set_fx_refresh_interval_override(1800)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="10")
        result = await frc.refresh_fx_refresh_interval_override_from_db(
            db_mock
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_above_max_clears(self):
        frc.set_fx_refresh_interval_override(1800)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="999999")
        result = await frc.refresh_fx_refresh_interval_override_from_db(
            db_mock
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_db_error_keeps_cache(self):
        frc.set_fx_refresh_interval_override(1800)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(side_effect=RuntimeError("boom"))
        result = await frc.refresh_fx_refresh_interval_override_from_db(
            db_mock
        )
        assert result == 1800
        assert frc.get_fx_refresh_interval_override() == 1800

    @pytest.mark.asyncio
    async def test_db_error_with_no_prior_keeps_none(self):
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(side_effect=RuntimeError("boom"))
        result = await frc.refresh_fx_refresh_interval_override_from_db(
            db_mock
        )
        assert result is None


# ── human formatter ──────────────────────────────────────────────


class TestFormatInterval:
    @pytest.mark.parametrize("seconds,expected", [
        (30, "30s"),
        (59, "59s"),
        (60, "1m"),
        (90, "1m 30s"),
        (600, "10m"),
        (3600, "1h"),
        (5400, "1h 30m"),
        (7200, "2h"),
        (21600, "6h"),
        (86400, "24h"),
    ])
    def test_format(self, seconds, expected):
        assert frc.format_interval_human(seconds) == expected


# ── module constants ─────────────────────────────────────────────


class TestConstants:
    def test_default_is_ten_minutes(self):
        assert frc.DEFAULT_FX_REFRESH_INTERVAL_SECONDS == 600

    def test_minimum_is_one_minute(self):
        assert frc.FX_REFRESH_INTERVAL_MINIMUM == 60

    def test_maximum_is_one_day(self):
        assert frc.FX_REFRESH_INTERVAL_MAXIMUM == 86400

    def test_setting_key_matches_env_name(self):
        assert (
            frc.FX_REFRESH_INTERVAL_SETTING_KEY
            == "FX_REFRESH_INTERVAL_SECONDS"
        )

    def test_default_within_range(self):
        assert (
            frc.FX_REFRESH_INTERVAL_MINIMUM
            <= frc.DEFAULT_FX_REFRESH_INTERVAL_SECONDS
            <= frc.FX_REFRESH_INTERVAL_MAXIMUM
        )


# ── bundled bug fix: cadence sync ────────────────────────────────


class TestCadenceSync:
    """Verify the ``_sync_registered_cadence`` helper in fx_rates
    pushes the resolved cadence into ``bot_health.LOOP_CADENCES`` so
    the panel's stale-threshold tracks the loop's actual sleep
    duration. Stage-15-Step-E #10b row 24 bundled bug fix.
    """

    def test_helper_exists(self):
        from fx_rates import _sync_registered_cadence

        assert callable(_sync_registered_cadence)

    def test_sync_updates_loop_cadences(self):
        from fx_rates import _sync_registered_cadence
        from bot_health import LOOP_CADENCES, register_loop

        # Register the fx_refresh loop if not already (idempotent
        # within this test process).
        if "fx_refresh" not in LOOP_CADENCES:
            register_loop("fx_refresh", cadence_seconds=600)

        _sync_registered_cadence(1800)
        assert LOOP_CADENCES["fx_refresh"] == 1800

        # Restore default for downstream tests in the same process.
        _sync_registered_cadence(600)
        assert LOOP_CADENCES["fx_refresh"] == 600

    def test_sync_swallows_unknown_loop(self, monkeypatch, caplog):
        """If the registry is empty (test harness),
        ``update_loop_cadence`` raises KeyError. The sync helper
        logs + swallows it so a flaky harness doesn't take down
        the loop.
        """
        import bot_health
        from fx_rates import _sync_registered_cadence

        original = bot_health.LOOP_CADENCES.copy()
        try:
            bot_health.LOOP_CADENCES.clear()
            _sync_registered_cadence(1800)
            assert any(
                "update_loop_cadence" in r.message
                for r in caplog.records
            )
        finally:
            bot_health.LOOP_CADENCES.update(original)
