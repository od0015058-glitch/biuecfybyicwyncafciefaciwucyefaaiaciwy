"""Stage-15-Step-E #10b row 8: ``MEMORY_CONTEXT_LIMIT`` +
``MEMORY_CONTENT_MAX_CHARS`` override layer.

Mirrors :mod:`tests.test_free_trial` in structure:

1. Pure coercion for both knobs.
2. Override set / clear / get / refresh-from-DB.
3. Public lookup (resolution order + source reporting).
"""

from __future__ import annotations

import math
from unittest.mock import AsyncMock

import pytest

import memory_config as _mc


# =====================================================================
# Autouse fixture: reset module-level caches between tests
# =====================================================================


@pytest.fixture(autouse=True)
def _reset_overrides(monkeypatch):
    """Clear both override caches so each test starts from a clean state."""
    monkeypatch.setattr(_mc, "_MEMORY_CONTEXT_LIMIT_OVERRIDE", None)
    monkeypatch.setattr(_mc, "_MEMORY_CONTENT_MAX_CHARS_OVERRIDE", None)
    monkeypatch.delenv("MEMORY_CONTEXT_LIMIT", raising=False)
    monkeypatch.delenv("MEMORY_CONTENT_MAX_CHARS", raising=False)


# =====================================================================
# MEMORY_CONTEXT_LIMIT — coercion
# =====================================================================


@pytest.mark.parametrize(
    "raw, expected",
    [
        (1, 1),
        (30, 30),
        (500, 500),
        ("1", 1),
        ("30", 30),
        ("500", 500),
        (30.0, 30),
        ("30.0", 30),
    ],
)
def test_coerce_context_limit_accepts_valid(raw, expected):
    assert _mc._coerce_memory_context_limit(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        True, False,
        0, -1, 501, 1000,
        "0", "-1", "501",
        15.5, "15.5",
        "abc", "", None,
        float("nan"), float("inf"),
    ],
)
def test_coerce_context_limit_rejects_invalid(raw):
    assert _mc._coerce_memory_context_limit(raw) is None


# =====================================================================
# MEMORY_CONTENT_MAX_CHARS — coercion
# =====================================================================


@pytest.mark.parametrize(
    "raw, expected",
    [
        (100, 100),
        (8000, 8000),
        (100_000, 100_000),
        ("100", 100),
        ("8000", 8000),
        ("100000", 100_000),
        (8000.0, 8000),
        ("8000.0", 8000),
    ],
)
def test_coerce_content_max_accepts_valid(raw, expected):
    assert _mc._coerce_memory_content_max_chars(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        True, False,
        0, 99, -1, 100_001,
        "0", "99", "-1", "100001",
        99.5, "99.5",
        "abc", "", None,
        float("nan"), float("inf"),
    ],
)
def test_coerce_content_max_rejects_invalid(raw):
    assert _mc._coerce_memory_content_max_chars(raw) is None


# =====================================================================
# Context-limit override set / clear / get
# =====================================================================


def test_set_context_limit_override_valid():
    _mc.set_memory_context_limit_override(50)
    assert _mc.get_memory_context_limit_override() == 50


def test_set_context_limit_override_rejects_bool():
    with pytest.raises(ValueError, match="not bool"):
        _mc.set_memory_context_limit_override(True)


def test_set_context_limit_override_rejects_out_of_range():
    with pytest.raises(ValueError):
        _mc.set_memory_context_limit_override(0)
    with pytest.raises(ValueError):
        _mc.set_memory_context_limit_override(501)


def test_clear_context_limit_override():
    _mc.set_memory_context_limit_override(50)
    assert _mc.clear_memory_context_limit_override() is True
    assert _mc.get_memory_context_limit_override() is None
    assert _mc.clear_memory_context_limit_override() is False


# =====================================================================
# Content-max override set / clear / get
# =====================================================================


def test_set_content_max_override_valid():
    _mc.set_memory_content_max_chars_override(16000)
    assert _mc.get_memory_content_max_chars_override() == 16000


def test_set_content_max_override_rejects_bool():
    with pytest.raises(ValueError, match="not bool"):
        _mc.set_memory_content_max_chars_override(True)


def test_set_content_max_override_rejects_out_of_range():
    with pytest.raises(ValueError):
        _mc.set_memory_content_max_chars_override(99)
    with pytest.raises(ValueError):
        _mc.set_memory_content_max_chars_override(100_001)


def test_clear_content_max_override():
    _mc.set_memory_content_max_chars_override(16000)
    assert _mc.clear_memory_content_max_chars_override() is True
    assert _mc.get_memory_content_max_chars_override() is None
    assert _mc.clear_memory_content_max_chars_override() is False


# =====================================================================
# Public lookup — resolution order
# =====================================================================


def test_get_context_limit_default(monkeypatch):
    monkeypatch.delenv("MEMORY_CONTEXT_LIMIT", raising=False)
    assert _mc.get_memory_context_limit() == 30
    assert _mc.get_memory_context_limit_source() == "default"


def test_get_context_limit_env(monkeypatch):
    monkeypatch.setenv("MEMORY_CONTEXT_LIMIT", "50")
    assert _mc.get_memory_context_limit() == 50
    assert _mc.get_memory_context_limit_source() == "env"


def test_get_context_limit_override_wins_over_env(monkeypatch):
    monkeypatch.setenv("MEMORY_CONTEXT_LIMIT", "50")
    _mc.set_memory_context_limit_override(100)
    assert _mc.get_memory_context_limit() == 100
    assert _mc.get_memory_context_limit_source() == "db"


def test_get_context_limit_invalid_env_falls_to_default(monkeypatch):
    monkeypatch.setenv("MEMORY_CONTEXT_LIMIT", "abc")
    assert _mc.get_memory_context_limit() == 30
    assert _mc.get_memory_context_limit_source() == "default"


def test_get_content_max_default(monkeypatch):
    monkeypatch.delenv("MEMORY_CONTENT_MAX_CHARS", raising=False)
    assert _mc.get_memory_content_max_chars() == 8000
    assert _mc.get_memory_content_max_chars_source() == "default"


def test_get_content_max_env(monkeypatch):
    monkeypatch.setenv("MEMORY_CONTENT_MAX_CHARS", "16000")
    assert _mc.get_memory_content_max_chars() == 16000
    assert _mc.get_memory_content_max_chars_source() == "env"


def test_get_content_max_override_wins_over_env(monkeypatch):
    monkeypatch.setenv("MEMORY_CONTENT_MAX_CHARS", "16000")
    _mc.set_memory_content_max_chars_override(32000)
    assert _mc.get_memory_content_max_chars() == 32000
    assert _mc.get_memory_content_max_chars_source() == "db"


# =====================================================================
# refresh_*_from_db
# =====================================================================


@pytest.mark.asyncio
async def test_refresh_context_limit_from_db_happy():
    db = AsyncMock()
    db.get_setting = AsyncMock(return_value="50")
    result = await _mc.refresh_memory_context_limit_override_from_db(db)
    assert result == 50
    assert _mc.get_memory_context_limit_override() == 50


@pytest.mark.asyncio
async def test_refresh_context_limit_from_db_none_clears():
    _mc.set_memory_context_limit_override(50)
    db = AsyncMock()
    db.get_setting = AsyncMock(return_value=None)
    result = await _mc.refresh_memory_context_limit_override_from_db(db)
    assert result is None
    assert _mc.get_memory_context_limit_override() is None


@pytest.mark.asyncio
async def test_refresh_context_limit_from_db_error_keeps_cache():
    _mc.set_memory_context_limit_override(50)
    db = AsyncMock()
    db.get_setting = AsyncMock(side_effect=RuntimeError("boom"))
    result = await _mc.refresh_memory_context_limit_override_from_db(db)
    assert result == 50
    assert _mc.get_memory_context_limit_override() == 50


@pytest.mark.asyncio
async def test_refresh_context_limit_from_db_malformed_clears():
    _mc.set_memory_context_limit_override(50)
    db = AsyncMock()
    db.get_setting = AsyncMock(return_value="garbage")
    result = await _mc.refresh_memory_context_limit_override_from_db(db)
    assert result is None
    assert _mc.get_memory_context_limit_override() is None


@pytest.mark.asyncio
async def test_refresh_context_limit_from_db_none_db():
    result = await _mc.refresh_memory_context_limit_override_from_db(None)
    assert result is None


@pytest.mark.asyncio
async def test_refresh_content_max_from_db_happy():
    db = AsyncMock()
    db.get_setting = AsyncMock(return_value="16000")
    result = await _mc.refresh_memory_content_max_chars_override_from_db(db)
    assert result == 16000
    assert _mc.get_memory_content_max_chars_override() == 16000


@pytest.mark.asyncio
async def test_refresh_content_max_from_db_none_clears():
    _mc.set_memory_content_max_chars_override(16000)
    db = AsyncMock()
    db.get_setting = AsyncMock(return_value=None)
    result = await _mc.refresh_memory_content_max_chars_override_from_db(db)
    assert result is None
    assert _mc.get_memory_content_max_chars_override() is None


@pytest.mark.asyncio
async def test_refresh_content_max_from_db_error_keeps_cache():
    _mc.set_memory_content_max_chars_override(16000)
    db = AsyncMock()
    db.get_setting = AsyncMock(side_effect=RuntimeError("boom"))
    result = await _mc.refresh_memory_content_max_chars_override_from_db(db)
    assert result == 16000
    assert _mc.get_memory_content_max_chars_override() == 16000
