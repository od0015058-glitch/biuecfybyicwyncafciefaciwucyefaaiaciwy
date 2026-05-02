"""Tests for the i18n_lock module (Stage-15-Step-E #10b row 22)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import i18n_lock


@pytest.fixture(autouse=True)
def _reset_lock_cache():
    """Each test starts and ends with a clean override cache."""
    i18n_lock.clear_i18n_lock_override()
    yield
    i18n_lock.clear_i18n_lock_override()


# ---------------------------------------------------------------------
# _coerce_i18n_lock
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Truthy strings, every accepted spelling.
        ("1", True),
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("t", True),
        ("yes", True),
        ("Y", True),
        ("on", True),
        ("ON", True),
        ("lock", True),
        ("locked", True),
        ("  1  ", True),  # whitespace stripped
        # Falsy strings, every accepted spelling.
        ("0", False),
        ("false", False),
        ("False", False),
        ("FALSE", False),
        ("f", False),
        ("no", False),
        ("N", False),
        ("off", False),
        ("OFF", False),
        ("unlock", False),
        ("unlocked", False),
        ("  0  ", False),
        # Bools.
        (True, True),
        (False, False),
        # Numbers.
        (1, True),
        (0, False),
        (1.0, True),
        (0.0, False),
        # Unrecognised → None.
        ("", None),
        ("   ", None),
        ("yes-please", None),
        ("maybe", None),
        ("garbage", None),
        ("2", None),
        ("-1", None),
        (None, None),
        # NaN / inf collapse to None.
        (float("nan"), None),
        (float("inf"), None),
        (float("-inf"), None),
        # Other types collapse to None.
        ([], None),
        ({}, None),
        (object(), None),
    ],
)
def test_coerce_handles_every_input_shape(raw, expected):
    assert i18n_lock._coerce_i18n_lock(raw) is expected


# ---------------------------------------------------------------------
# Override accessors
# ---------------------------------------------------------------------


def test_override_starts_unset():
    assert i18n_lock.get_i18n_lock_override() is None


def test_set_override_round_trip_true():
    i18n_lock.set_i18n_lock_override(True)
    assert i18n_lock.get_i18n_lock_override() is True


def test_set_override_round_trip_false():
    i18n_lock.set_i18n_lock_override(False)
    assert i18n_lock.get_i18n_lock_override() is False


def test_clear_override_returns_true_when_active():
    i18n_lock.set_i18n_lock_override(True)
    assert i18n_lock.clear_i18n_lock_override() is True
    assert i18n_lock.get_i18n_lock_override() is None


def test_clear_override_returns_false_when_unset():
    assert i18n_lock.clear_i18n_lock_override() is False


@pytest.mark.parametrize("bad", [1, 0, "true", "false", None, [], {}])
def test_set_override_rejects_non_bool(bad):
    with pytest.raises(ValueError, match="must be bool"):
        i18n_lock.set_i18n_lock_override(bad)


# ---------------------------------------------------------------------
# refresh_i18n_lock_override_from_db
# ---------------------------------------------------------------------


async def test_refresh_clears_when_no_db_row_present():
    db = AsyncMock()
    db.get_setting = AsyncMock(return_value=None)
    i18n_lock.set_i18n_lock_override(True)

    result = await i18n_lock.refresh_i18n_lock_override_from_db(db)

    assert result is None
    assert i18n_lock.get_i18n_lock_override() is None


async def test_refresh_loads_true_from_db():
    db = AsyncMock()
    db.get_setting = AsyncMock(return_value="1")

    result = await i18n_lock.refresh_i18n_lock_override_from_db(db)

    assert result is True
    assert i18n_lock.get_i18n_lock_override() is True


async def test_refresh_loads_false_from_db():
    db = AsyncMock()
    db.get_setting = AsyncMock(return_value="0")

    result = await i18n_lock.refresh_i18n_lock_override_from_db(db)

    assert result is False
    assert i18n_lock.get_i18n_lock_override() is False


async def test_refresh_keeps_cache_on_transient_db_error():
    """A flaky DB shouldn't silently unlock the editor — keep the
    previous cached value rather than collapsing to None."""
    db = AsyncMock()
    db.get_setting = AsyncMock(side_effect=RuntimeError("transient"))
    i18n_lock.set_i18n_lock_override(True)

    result = await i18n_lock.refresh_i18n_lock_override_from_db(db)

    assert result is True
    assert i18n_lock.get_i18n_lock_override() is True


async def test_refresh_clears_override_on_garbage_value():
    db = AsyncMock()
    db.get_setting = AsyncMock(return_value="garbage-value-from-old-tool")
    i18n_lock.set_i18n_lock_override(True)

    result = await i18n_lock.refresh_i18n_lock_override_from_db(db)

    assert result is None
    assert i18n_lock.get_i18n_lock_override() is None


async def test_refresh_with_db_none_is_a_no_op():
    i18n_lock.set_i18n_lock_override(True)
    result = await i18n_lock.refresh_i18n_lock_override_from_db(None)
    assert result is True


@pytest.mark.parametrize("token", ["true", "yes", "on", "lock"])
async def test_refresh_accepts_alternative_truthy_spellings(token):
    db = AsyncMock()
    db.get_setting = AsyncMock(return_value=token)
    result = await i18n_lock.refresh_i18n_lock_override_from_db(db)
    assert result is True


@pytest.mark.parametrize("token", ["false", "no", "off", "unlock"])
async def test_refresh_accepts_alternative_falsy_spellings(token):
    db = AsyncMock()
    db.get_setting = AsyncMock(return_value=token)
    result = await i18n_lock.refresh_i18n_lock_override_from_db(db)
    assert result is False


# ---------------------------------------------------------------------
# is_i18n_locked + get_i18n_lock_source
# ---------------------------------------------------------------------


def test_is_locked_default_when_neither_override_nor_env(monkeypatch):
    monkeypatch.delenv("I18N_LOCK", raising=False)
    assert i18n_lock.is_i18n_locked() is False
    assert i18n_lock.get_i18n_lock_source() == "default"


def test_is_locked_reads_env_var_when_set(monkeypatch):
    monkeypatch.setenv("I18N_LOCK", "1")
    assert i18n_lock.is_i18n_locked() is True
    assert i18n_lock.get_i18n_lock_source() == "env"


def test_is_locked_explicit_env_value_param(monkeypatch):
    """The env_value kwarg lets the caller thread the env reading
    instead of doing an os.getenv at call time."""
    monkeypatch.delenv("I18N_LOCK", raising=False)
    assert i18n_lock.is_i18n_locked(env_value="1") is True
    assert i18n_lock.is_i18n_locked(env_value="0") is False
    assert i18n_lock.get_i18n_lock_source(env_value="1") == "env"


def test_is_locked_garbage_env_falls_through_to_default(monkeypatch):
    monkeypatch.setenv("I18N_LOCK", "garbage")
    assert i18n_lock.is_i18n_locked() is False
    assert i18n_lock.get_i18n_lock_source() == "default"


def test_db_override_beats_env(monkeypatch):
    """A False override beats a truthy env value, and vice-versa,
    so the operator can record an explicit override that contradicts
    the env."""
    monkeypatch.setenv("I18N_LOCK", "1")
    i18n_lock.set_i18n_lock_override(False)
    assert i18n_lock.is_i18n_locked() is False
    assert i18n_lock.get_i18n_lock_source() == "db"


def test_db_override_true_with_no_env(monkeypatch):
    monkeypatch.delenv("I18N_LOCK", raising=False)
    i18n_lock.set_i18n_lock_override(True)
    assert i18n_lock.is_i18n_locked() is True
    assert i18n_lock.get_i18n_lock_source() == "db"


# ---------------------------------------------------------------------
# serialise_lock_for_db
# ---------------------------------------------------------------------


def test_serialise_for_db_canonical_forms():
    assert i18n_lock.serialise_lock_for_db(True) == "1"
    assert i18n_lock.serialise_lock_for_db(False) == "0"


@pytest.mark.parametrize("bad", [1, 0, "1", None])
def test_serialise_for_db_rejects_non_bool(bad):
    with pytest.raises(ValueError, match="expects bool"):
        i18n_lock.serialise_lock_for_db(bad)


def test_serialised_value_round_trips_through_coerce():
    """Whatever ``serialise_lock_for_db`` writes must round-trip
    cleanly through the coercer — otherwise ``refresh_…_from_db``
    would reject our own writes."""
    for v in (True, False):
        assert i18n_lock._coerce_i18n_lock(
            i18n_lock.serialise_lock_for_db(v)
        ) is v


# ---------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------


def test_setting_key_is_i18n_lock():
    """Pinned so a future rename of the key requires updating both
    the module and any external operator who set the row by hand."""
    assert i18n_lock.I18N_LOCK_SETTING_KEY == "I18N_LOCK"
