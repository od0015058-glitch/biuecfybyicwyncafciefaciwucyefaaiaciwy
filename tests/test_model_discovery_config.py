"""Tests for ``model_discovery_config`` — DB-backed override for
DISCOVERY_INTERVAL_SECONDS.

Stage-15-Step-E #10b row 23.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock

import pytest

import model_discovery_config as mdc


# ── fixtures ─────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    """Clear module-level cache before each test."""
    monkeypatch.setattr(mdc, "_DISCOVERY_INTERVAL_OVERRIDE", None)
    monkeypatch.delenv("DISCOVERY_INTERVAL_SECONDS", raising=False)
    yield


# ── coercion ─────────────────────────────────────────────────────


class TestCoercion:
    @pytest.mark.parametrize("value,expected", [
        (60, 60),
        (21600, 21600),
        (604800, 604800),
        (3600, 3600),
        ("3600", 3600),
        ("60", 60),
        (60.0, 60),
    ])
    def test_happy(self, value, expected):
        assert mdc._coerce_discovery_interval(value) == expected

    @pytest.mark.parametrize("value", [
        True,
        False,
        "abc",
        "",
        None,
        float("nan"),
        float("inf"),
        float("-inf"),
        59,       # below min
        604801,   # above max
        0,
        -1,
        60.5,     # non-integer float
    ])
    def test_rejection(self, value):
        assert mdc._coerce_discovery_interval(value) is None


# ── override accessors ───────────────────────────────────────────


class TestOverrideAccessors:
    def test_initial(self):
        assert mdc.get_discovery_interval_override() is None

    def test_set_and_get(self):
        mdc.set_discovery_interval_override(3600)
        assert mdc.get_discovery_interval_override() == 3600

    def test_set_rejects_bool(self):
        with pytest.raises(ValueError, match="not bool"):
            mdc.set_discovery_interval_override(True)

    def test_set_rejects_out_of_range(self):
        with pytest.raises(ValueError):
            mdc.set_discovery_interval_override(10)

    def test_clear_returns_true_when_active(self):
        mdc.set_discovery_interval_override(3600)
        assert mdc.clear_discovery_interval_override() is True

    def test_clear_returns_false_when_none(self):
        assert mdc.clear_discovery_interval_override() is False

    def test_clear_resets(self):
        mdc.set_discovery_interval_override(3600)
        mdc.clear_discovery_interval_override()
        assert mdc.get_discovery_interval_override() is None


# ── resolution ───────────────────────────────────────────────────


class TestResolution:
    def test_default(self):
        assert mdc.get_discovery_interval_seconds() == 21600
        assert mdc.get_discovery_interval_source() == "default"

    def test_env_wins(self, monkeypatch):
        monkeypatch.setenv("DISCOVERY_INTERVAL_SECONDS", "7200")
        assert mdc.get_discovery_interval_seconds() == 7200
        assert mdc.get_discovery_interval_source() == "env"

    def test_override_wins(self, monkeypatch):
        monkeypatch.setenv("DISCOVERY_INTERVAL_SECONDS", "7200")
        mdc.set_discovery_interval_override(3600)
        assert mdc.get_discovery_interval_seconds() == 3600
        assert mdc.get_discovery_interval_source() == "db"

    def test_invalid_env_falls_through(self, monkeypatch):
        monkeypatch.setenv("DISCOVERY_INTERVAL_SECONDS", "garbage")
        assert mdc.get_discovery_interval_seconds() == 21600
        assert mdc.get_discovery_interval_source() == "default"


# ── refresh from DB ──────────────────────────────────────────────


class TestRefreshFromDB:
    @pytest.mark.asyncio
    async def test_none_db(self):
        result = await mdc.refresh_discovery_interval_override_from_db(None)
        assert result is None

    @pytest.mark.asyncio
    async def test_no_row_clears(self):
        mdc.set_discovery_interval_override(3600)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value=None)
        result = await mdc.refresh_discovery_interval_override_from_db(db_mock)
        assert result is None
        assert mdc.get_discovery_interval_override() is None

    @pytest.mark.asyncio
    async def test_valid_row(self):
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="7200")
        result = await mdc.refresh_discovery_interval_override_from_db(db_mock)
        assert result == 7200
        assert mdc.get_discovery_interval_override() == 7200

    @pytest.mark.asyncio
    async def test_malformed_clears(self):
        mdc.set_discovery_interval_override(3600)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="garbage")
        result = await mdc.refresh_discovery_interval_override_from_db(db_mock)
        assert result is None
        assert mdc.get_discovery_interval_override() is None

    @pytest.mark.asyncio
    async def test_db_error_keeps_cache(self):
        mdc.set_discovery_interval_override(3600)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(side_effect=RuntimeError("boom"))
        result = await mdc.refresh_discovery_interval_override_from_db(db_mock)
        assert result == 3600
        assert mdc.get_discovery_interval_override() == 3600


# ── human formatter ──────────────────────────────────────────────


class TestFormatInterval:
    @pytest.mark.parametrize("seconds,expected", [
        (30, "30s"),
        (60, "1m"),
        (90, "1m 30s"),
        (3600, "1h"),
        (7200, "2h"),
        (5400, "1h 30m"),
        (21600, "6h"),
        (86400, "24h"),
        (604800, "168h"),
    ])
    def test_format(self, seconds, expected):
        assert mdc.format_interval_human(seconds) == expected


# ── delete_setting NUL strip (bundled bug fix) ───────────────────


class TestDeleteSettingNulStrip:
    """Verify the NUL-strip fix in ``Database.delete_setting``."""

    @pytest.mark.asyncio
    async def test_nul_in_key_stripped(self):
        """A key with embedded NUL bytes should be stripped before
        the DELETE, mirroring upsert_setting."""
        from database import Database

        db = Database.__new__(Database)
        mock_pool = AsyncMock()
        mock_pool.execute = AsyncMock(return_value="DELETE 1")
        db.pool = mock_pool

        result = await db.delete_setting("MY\x00KEY")
        assert result is True
        call_args = mock_pool.execute.call_args
        assert call_args is not None
        assert "\x00" not in call_args[0][1]
        assert call_args[0][1] == "MYKEY"

    @pytest.mark.asyncio
    async def test_all_nul_key_raises(self):
        """A key that is entirely NUL bytes should raise ValueError."""
        from database import Database

        db = Database.__new__(Database)
        db.pool = AsyncMock()

        with pytest.raises(ValueError, match="empty after NUL"):
            await db.delete_setting("\x00\x00")
