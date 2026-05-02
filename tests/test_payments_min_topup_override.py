"""Unit tests for ``payments.get_min_topup_usd`` and the DB-backed
override layer (Stage-15-Step-E #10b row 4).

Mirrors :file:`tests/test_pricing.py`'s coverage of the COST_MARKUP
override layer — same shape, same fixtures, same edge cases, just
applied to the minimum-top-up-USD knob. Validates:

* Resolution order (override → env → default).
* Validation rejects on set + on refresh from DB.
* ``effective_min_usd`` honours the override across cached coin minimums.
* Source reporting (``db`` / ``env`` / ``default``).
* Refresh-from-DB happy path, db-error path, none-db path,
  malformed-stored-value path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

import payments


@pytest.fixture(autouse=True)
def _reset_min_topup_override_between_tests(monkeypatch):
    """Each test starts with a clean override + fresh per-currency cache."""
    payments.clear_min_topup_override()
    payments._min_amount_cache.clear()
    monkeypatch.delenv("MIN_TOPUP_USD", raising=False)
    yield
    payments.clear_min_topup_override()
    payments._min_amount_cache.clear()


# ---------------------------------------------------------------------
# Override set / clear / get
# ---------------------------------------------------------------------


def test_set_min_topup_override_changes_get_min_topup(monkeypatch):
    monkeypatch.setenv("MIN_TOPUP_USD", "5")
    assert payments.get_min_topup_usd() == 5.0
    payments.set_min_topup_override(7.5)
    assert payments.get_min_topup_usd() == 7.5
    assert payments.get_min_topup_override() == 7.5


def test_clear_min_topup_override_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("MIN_TOPUP_USD", "3")
    payments.set_min_topup_override(10.0)
    assert payments.get_min_topup_usd() == 10.0
    payments.clear_min_topup_override()
    assert payments.get_min_topup_usd() == 3.0
    assert payments.get_min_topup_override() is None


def test_clear_min_topup_override_falls_back_to_default(monkeypatch):
    monkeypatch.delenv("MIN_TOPUP_USD", raising=False)
    payments.set_min_topup_override(50.0)
    payments.clear_min_topup_override()
    assert payments.get_min_topup_usd() == payments.DEFAULT_MIN_TOPUP_USD


def test_clear_min_topup_override_returns_had_value():
    payments.set_min_topup_override(4.0)
    assert payments.clear_min_topup_override() is True
    assert payments.clear_min_topup_override() is False


@pytest.mark.parametrize(
    "bad_value",
    [
        -1.0,
        float("nan"),
        float("inf"),
        float("-inf"),
        payments.MIN_TOPUP_USD_MAXIMUM,
        payments.MIN_TOPUP_USD_MAXIMUM + 1.0,
    ],
)
def test_set_min_topup_override_rejects_bad_values(bad_value):
    with pytest.raises(ValueError):
        payments.set_min_topup_override(bad_value)


def test_set_min_topup_override_rejects_bool():
    """``isinstance(True, int) is True`` would otherwise sneak through
    as 1.0; explicit bool guard prevents the silent acceptance."""
    with pytest.raises(ValueError):
        payments.set_min_topup_override(True)
    with pytest.raises(ValueError):
        payments.set_min_topup_override(False)


def test_set_min_topup_override_rejects_non_numeric_string():
    with pytest.raises(ValueError):
        payments.set_min_topup_override("not a number")


def test_set_min_topup_override_accepts_string_numbers():
    """Operators may post strings from a form; coercion should still work."""
    payments.set_min_topup_override("4.5")
    assert payments.get_min_topup_usd() == 4.5


# ---------------------------------------------------------------------
# Source reporting
# ---------------------------------------------------------------------


def test_get_min_topup_source_default_when_unset(monkeypatch):
    monkeypatch.delenv("MIN_TOPUP_USD", raising=False)
    assert payments.get_min_topup_source() == "default"


def test_get_min_topup_source_env_when_env_set(monkeypatch):
    monkeypatch.setenv("MIN_TOPUP_USD", "5")
    assert payments.get_min_topup_source() == "env"


def test_get_min_topup_source_env_with_invalid_env_falls_back(monkeypatch):
    """A malformed env value (e.g. -1) fails coercion → source=default."""
    monkeypatch.setenv("MIN_TOPUP_USD", "-1")
    assert payments.get_min_topup_source() == "default"


def test_get_min_topup_source_db_when_override_set(monkeypatch):
    monkeypatch.setenv("MIN_TOPUP_USD", "5")
    payments.set_min_topup_override(7.0)
    assert payments.get_min_topup_source() == "db"


# ---------------------------------------------------------------------
# refresh_min_topup_override_from_db
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_min_topup_override_from_db_loads_value():
    db = MagicMock()
    db.get_setting = AsyncMock(return_value="6.25")
    loaded = await payments.refresh_min_topup_override_from_db(db)
    assert loaded == 6.25
    assert payments.get_min_topup_usd() == 6.25
    db.get_setting.assert_awaited_once_with(
        payments.MIN_TOPUP_SETTING_KEY,
    )


@pytest.mark.asyncio
async def test_refresh_min_topup_override_from_db_clears_when_row_missing():
    payments.set_min_topup_override(9.0)
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=None)
    loaded = await payments.refresh_min_topup_override_from_db(db)
    assert loaded is None
    assert payments.get_min_topup_override() is None


@pytest.mark.asyncio
async def test_refresh_min_topup_override_from_db_keeps_cache_on_error():
    """A transient DB blip must NOT clear an active override."""
    payments.set_min_topup_override(9.0)
    db = MagicMock()
    db.get_setting = AsyncMock(side_effect=RuntimeError("DB down"))
    loaded = await payments.refresh_min_topup_override_from_db(db)
    # Returns the previous in-memory value rather than crashing or
    # silently clearing it.
    assert loaded == 9.0
    assert payments.get_min_topup_override() == 9.0


@pytest.mark.asyncio
async def test_refresh_min_topup_override_from_db_handles_none_db():
    payments.set_min_topup_override(4.0)
    loaded = await payments.refresh_min_topup_override_from_db(None)
    # None db = no-op; the previous override stays.
    assert loaded == 4.0
    assert payments.get_min_topup_override() == 4.0


@pytest.mark.asyncio
async def test_refresh_min_topup_override_from_db_rejects_malformed():
    """A malformed DB row must clear the override rather than poison it."""
    payments.set_min_topup_override(9.0)
    db = MagicMock()
    db.get_setting = AsyncMock(return_value="not-a-number")
    loaded = await payments.refresh_min_topup_override_from_db(db)
    assert loaded is None
    assert payments.get_min_topup_override() is None


@pytest.mark.asyncio
async def test_refresh_min_topup_override_from_db_rejects_out_of_range():
    """A maliciously stored 99999 must NOT poison the cache."""
    db = MagicMock()
    db.get_setting = AsyncMock(return_value="99999")  # > MAX
    loaded = await payments.refresh_min_topup_override_from_db(db)
    assert loaded is None


# ---------------------------------------------------------------------
# effective_min_usd integration
# ---------------------------------------------------------------------


def test_effective_min_usd_uses_override(monkeypatch):
    """The override flips the floor for *every* coin at once."""
    monkeypatch.setenv("MIN_TOPUP_USD", "2")
    # Pre-cache a per-currency min that's lower than both the env
    # default and the override; the effective floor must always be
    # ``max(global, per_currency)``.
    payments._min_amount_cache["btc"] = (
        0.5, __import__("asyncio").get_event_loop().time() + 3600,
    )
    assert payments.effective_min_usd("btc") == 2.0
    payments.set_min_topup_override(10.0)
    assert payments.effective_min_usd("btc") == 10.0


def test_effective_min_usd_uses_override_when_per_currency_higher(monkeypatch):
    """When the per-currency min beats both the override and env, the
    per-currency number wins."""
    monkeypatch.setenv("MIN_TOPUP_USD", "2")
    payments._min_amount_cache["sol"] = (
        15.0, __import__("asyncio").get_event_loop().time() + 3600,
    )
    payments.set_min_topup_override(10.0)
    assert payments.effective_min_usd("sol") == 15.0
