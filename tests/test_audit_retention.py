"""Tests for ``audit_retention`` — DB-backed override layer + reaper.

Stage-15-Step-E #10b row 20.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import audit_retention


# ── fixtures ─────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_module_caches(monkeypatch):
    """Clear module-level caches before each test."""
    monkeypatch.setattr(audit_retention, "_AUDIT_RETENTION_DAYS_OVERRIDE", None)
    monkeypatch.setattr(audit_retention, "_REAPER_TICKS", 0)
    monkeypatch.setattr(audit_retention, "_REAPER_TOTAL_DELETED", 0)
    monkeypatch.setattr(audit_retention, "_REAPER_LAST_RUN_EPOCH", 0.0)
    monkeypatch.delenv("AUDIT_RETENTION_DAYS", raising=False)
    monkeypatch.delenv("AUDIT_RETENTION_INTERVAL_HOURS", raising=False)
    monkeypatch.delenv("AUDIT_RETENTION_BATCH", raising=False)
    yield


# ── coercion ─────────────────────────────────────────────────────


class TestCoercion:
    """Test ``_coerce_audit_retention_days``."""

    @pytest.mark.parametrize("value,expected", [
        (7, 7),
        (90, 90),
        (3650, 3650),
        (30, 30),
        ("90", 90),
        ("7", 7),
        ("3650", 3650),
        (90.0, 90),
    ])
    def test_happy_path(self, value, expected):
        assert audit_retention._coerce_audit_retention_days(value) == expected

    @pytest.mark.parametrize("value", [
        True,
        False,
        "abc",
        "",
        None,
        float("nan"),
        float("inf"),
        float("-inf"),
        6,    # below min
        3651, # above max
        0,
        -1,
        90.5, # non-integer float
    ])
    def test_rejection(self, value):
        assert audit_retention._coerce_audit_retention_days(value) is None


# ── override set / clear / get ───────────────────────────────────


class TestOverrideAccessors:
    def test_initial_state(self):
        assert audit_retention.get_audit_retention_days_override() is None

    def test_set_and_get(self):
        audit_retention.set_audit_retention_days_override(30)
        assert audit_retention.get_audit_retention_days_override() == 30

    def test_set_rejects_bool(self):
        with pytest.raises(ValueError, match="not bool"):
            audit_retention.set_audit_retention_days_override(True)

    def test_set_rejects_out_of_range(self):
        with pytest.raises(ValueError):
            audit_retention.set_audit_retention_days_override(3)

    def test_clear_returns_true_when_active(self):
        audit_retention.set_audit_retention_days_override(60)
        assert audit_retention.clear_audit_retention_days_override() is True

    def test_clear_returns_false_when_already_none(self):
        assert audit_retention.clear_audit_retention_days_override() is False

    def test_clear_resets_to_none(self):
        audit_retention.set_audit_retention_days_override(60)
        audit_retention.clear_audit_retention_days_override()
        assert audit_retention.get_audit_retention_days_override() is None


# ── resolution: get_audit_retention_days / source ────────────────


class TestResolution:
    def test_default_when_nothing_set(self):
        assert audit_retention.get_audit_retention_days() == 90
        assert audit_retention.get_audit_retention_days_source() == "default"

    def test_env_wins_over_default(self, monkeypatch):
        monkeypatch.setenv("AUDIT_RETENTION_DAYS", "45")
        assert audit_retention.get_audit_retention_days() == 45
        assert audit_retention.get_audit_retention_days_source() == "env"

    def test_override_wins_over_env(self, monkeypatch):
        monkeypatch.setenv("AUDIT_RETENTION_DAYS", "45")
        audit_retention.set_audit_retention_days_override(120)
        assert audit_retention.get_audit_retention_days() == 120
        assert audit_retention.get_audit_retention_days_source() == "db"

    def test_invalid_env_falls_through_to_default(self, monkeypatch):
        monkeypatch.setenv("AUDIT_RETENTION_DAYS", "garbage")
        assert audit_retention.get_audit_retention_days() == 90
        assert audit_retention.get_audit_retention_days_source() == "default"


# ── refresh from DB ──────────────────────────────────────────────


class TestRefreshFromDB:
    @pytest.mark.asyncio
    async def test_none_db(self):
        result = await audit_retention.refresh_audit_retention_days_override_from_db(None)
        assert result is None

    @pytest.mark.asyncio
    async def test_no_row_clears_override(self):
        audit_retention.set_audit_retention_days_override(30)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value=None)
        result = await audit_retention.refresh_audit_retention_days_override_from_db(db_mock)
        assert result is None
        assert audit_retention.get_audit_retention_days_override() is None

    @pytest.mark.asyncio
    async def test_valid_row(self):
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="60")
        result = await audit_retention.refresh_audit_retention_days_override_from_db(db_mock)
        assert result == 60
        assert audit_retention.get_audit_retention_days_override() == 60

    @pytest.mark.asyncio
    async def test_malformed_row_clears(self):
        audit_retention.set_audit_retention_days_override(30)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(return_value="garbage")
        result = await audit_retention.refresh_audit_retention_days_override_from_db(db_mock)
        assert result is None
        assert audit_retention.get_audit_retention_days_override() is None

    @pytest.mark.asyncio
    async def test_db_error_keeps_previous_cache(self):
        audit_retention.set_audit_retention_days_override(30)
        db_mock = AsyncMock()
        db_mock.get_setting = AsyncMock(side_effect=RuntimeError("db down"))
        result = await audit_retention.refresh_audit_retention_days_override_from_db(db_mock)
        assert result == 30
        assert audit_retention.get_audit_retention_days_override() == 30


# ── reaper interval ──────────────────────────────────────────────


class TestReaperInterval:
    def test_default(self):
        assert audit_retention._get_retention_interval_hours() == 24

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("AUDIT_RETENTION_INTERVAL_HOURS", "6")
        assert audit_retention._get_retention_interval_hours() == 6

    def test_invalid_env(self, monkeypatch):
        monkeypatch.setenv("AUDIT_RETENTION_INTERVAL_HOURS", "bad")
        assert audit_retention._get_retention_interval_hours() == 24

    def test_zero_env_falls_back(self, monkeypatch):
        monkeypatch.setenv("AUDIT_RETENTION_INTERVAL_HOURS", "0")
        assert audit_retention._get_retention_interval_hours() == 24


# ── reaper batch ─────────────────────────────────────────────────


class TestReaperBatch:
    def test_default(self):
        assert audit_retention._get_retention_batch() == 5000

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("AUDIT_RETENTION_BATCH", "100")
        assert audit_retention._get_retention_batch() == 100

    def test_invalid_env(self, monkeypatch):
        monkeypatch.setenv("AUDIT_RETENTION_BATCH", "abc")
        assert audit_retention._get_retention_batch() == 5000


# ── reaper counters ──────────────────────────────────────────────


class TestReaperCounters:
    def test_initial(self):
        counters = audit_retention.get_reaper_counters()
        assert counters["ticks"] == 0
        assert counters["total_deleted"] == 0
        assert counters["last_run_epoch"] == 0.0


# ── _delete_old_audit_rows ───────────────────────────────────────


class TestDeleteOldRows:
    @pytest.mark.asyncio
    async def test_happy_path(self):
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="DELETE 42")
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(
            return_value=mock_conn
        )
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(
            return_value=False
        )
        db_mock = MagicMock()
        db_mock.pool = mock_pool
        deleted = await audit_retention._delete_old_audit_rows(db_mock, 90, 5000)
        assert deleted == 42

    @pytest.mark.asyncio
    async def test_zero_deleted(self):
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="DELETE 0")
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(
            return_value=mock_conn
        )
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(
            return_value=False
        )
        db_mock = MagicMock()
        db_mock.pool = mock_pool
        deleted = await audit_retention._delete_old_audit_rows(db_mock, 90, 5000)
        assert deleted == 0


# ── list_admin_audit_log limit cap (bundled bug fix) ─────────────


class TestAuditLogLimitCap:
    """Verify the defensive limit cap added to
    ``Database.list_admin_audit_log``."""

    @pytest.mark.asyncio
    async def test_limit_capped_to_10000(self):
        """A caller requesting more than 10 000 rows should be silently
        capped."""
        from database import Database

        db = Database.__new__(Database)
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(
            return_value=mock_conn
        )
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(
            return_value=False
        )
        db.pool = mock_pool

        await db.list_admin_audit_log(limit=999_999)
        call_args = mock_conn.fetch.call_args
        assert call_args is not None
        limit_arg = call_args[0][-1]
        assert limit_arg == 10_000

    @pytest.mark.asyncio
    async def test_limit_floored_to_1(self):
        """A zero or negative limit should be floored to 1."""
        from database import Database

        db = Database.__new__(Database)
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(
            return_value=mock_conn
        )
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(
            return_value=False
        )
        db.pool = mock_pool

        await db.list_admin_audit_log(limit=0)
        call_args = mock_conn.fetch.call_args
        assert call_args is not None
        limit_arg = call_args[0][-1]
        assert limit_arg == 1

    @pytest.mark.asyncio
    async def test_normal_limit_unchanged(self):
        """Normal limits under the cap pass through unchanged."""
        from database import Database

        db = Database.__new__(Database)
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(
            return_value=mock_conn
        )
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(
            return_value=False
        )
        db.pool = mock_pool

        await db.list_admin_audit_log(limit=200)
        call_args = mock_conn.fetch.call_args
        assert call_args is not None
        limit_arg = call_args[0][-1]
        assert limit_arg == 200
