"""Stage-15-Step-E #10b row 6: ``FREE_MESSAGES_PER_USER``.

Three layers covered here:

1. **Pure coercion** — :func:`free_trial._coerce_free_messages_per_user`
   under happy-path ints, finite int-valued floats (``"15.0"``),
   non-integer floats (``"15.5"``), NaN / Inf, ``bool``,
   out-of-range values, and non-numeric junk.

2. **Override get/set/clear** —
   :func:`free_trial.set_free_messages_per_user_override` + ``get`` +
   ``clear`` + the integration with
   :func:`free_trial.refresh_free_messages_per_user_override_from_db`
   which reads ``system_settings.FREE_MESSAGES_PER_USER`` via the
   in-process DB.

3. **Public lookup** — :func:`free_trial.get_free_messages_per_user`
   resolution order (override → env → default) +
   :func:`free_trial.get_free_messages_per_user_source` returning
   ``"db"`` / ``"env"`` / ``"default"``.

The DB-layer integration with :meth:`Database.create_user` (i.e.
the new explicit ``$3 = free_messages_left`` parameter binding) is
covered indirectly here — the override resolution is what
``create_user`` consults, and the SQL is unit-tested via mock in
:mod:`tests.test_database_queries`.
"""

from __future__ import annotations

import math
from unittest.mock import AsyncMock

import pytest

import free_trial as _free_trial


# =====================================================================
# _coerce_free_messages_per_user — happy paths
# =====================================================================


@pytest.mark.parametrize(
    "raw, expected",
    [
        # Int passthrough at boundaries.
        (0, 0),
        (1, 1),
        (10, 10),
        (10_000, 10_000),
        # String → int via float-then-int.
        ("0", 0),
        ("42", 42),
        ("10000", 10_000),
        # int-valued floats / strings — the override storage layer
        # writes via ``str(int)`` but a legacy writer may have used
        # ``str(float)``, so we accept both.
        (15.0, 15),
        ("15.0", 15),
        ("3.0", 3),
    ],
)
def test_coerce_free_messages_per_user_accepts_valid(raw, expected):
    """Happy path: accept all int + int-valued-float forms in range."""
    assert _free_trial._coerce_free_messages_per_user(raw) == expected


# =====================================================================
# _coerce_free_messages_per_user — rejections
# =====================================================================


@pytest.mark.parametrize(
    "raw",
    [
        # bool — subclass of int, MUST be rejected explicitly so a
        # stored ``True`` doesn't slide through as ``1`` (silently
        # changing the trial allowance to 1 message).
        True,
        False,
        # Out-of-range.
        -1,
        -100,
        10_001,
        100_000,
        "10001",
        "-1",
        # Non-integer floats.
        15.5,
        "15.5",
        "0.5",
        # Non-numeric junk.
        "abc",
        "",
        "  ",
        "12abc",
        None,
        # NaN / Inf — non-finite floats should be rejected even
        # though they're nominally a "float".
        float("nan"),
        float("inf"),
        float("-inf"),
        "nan",
        "inf",
    ],
)
def test_coerce_free_messages_per_user_rejects_invalid(raw):
    """Defence-in-depth: every invalid input falls back to ``None``."""
    assert _free_trial._coerce_free_messages_per_user(raw) is None


def test_coerce_rejects_bool_explicitly():
    """Pin the bool-rejection behaviour so a future "simplify by
    removing the isinstance check" refactor can't sneak through. The
    reason this matters: in CPython, ``isinstance(True, int)`` is
    ``True``, so without the explicit ``isinstance(value, bool)``
    guard, ``True`` would coerce to ``1`` and a stored ``True`` row
    in ``system_settings`` (which would read back as the literal
    string ``"True"``, but a future caller with rich-typed JSONB
    could deliver an actual ``bool``) would silently change the
    trial allowance to 1 message.
    """
    assert _free_trial._coerce_free_messages_per_user(True) is None
    assert _free_trial._coerce_free_messages_per_user(False) is None
    # Sanity: but ``1`` and ``0`` (genuine ints) DO pass.
    assert _free_trial._coerce_free_messages_per_user(1) == 1
    assert _free_trial._coerce_free_messages_per_user(0) == 0


def test_coerce_rejects_nan_inf():
    """Non-finite floats are rejected even though they're technically
    ``float``. NaN-as-allowance is nonsense, and Inf-as-allowance
    would silently let a single user accumulate unlimited free
    messages until they overflow ``int``."""
    assert _free_trial._coerce_free_messages_per_user(float("nan")) is None
    assert _free_trial._coerce_free_messages_per_user(float("inf")) is None
    assert _free_trial._coerce_free_messages_per_user(float("-inf")) is None
    # Sibling: ``math.nan`` is the same singleton.
    assert _free_trial._coerce_free_messages_per_user(math.nan) is None


# =====================================================================
# set / clear / get override
# =====================================================================


@pytest.fixture(autouse=True)
def reset_override():
    """Every test starts with no override and restores afterwards."""
    _free_trial.clear_free_messages_per_user_override()
    yield
    _free_trial.clear_free_messages_per_user_override()


def test_set_override_round_trip():
    """Round-trip: set → get returns the value."""
    _free_trial.set_free_messages_per_user_override(25)
    assert _free_trial.get_free_messages_per_user_override() == 25
    assert _free_trial.get_free_messages_per_user() == 25


def test_set_override_at_boundaries():
    """The minimum (0) and maximum (10_000) values both round-trip."""
    _free_trial.set_free_messages_per_user_override(
        _free_trial.FREE_MESSAGES_PER_USER_MINIMUM,
    )
    assert _free_trial.get_free_messages_per_user_override() == 0
    _free_trial.set_free_messages_per_user_override(
        _free_trial.FREE_MESSAGES_PER_USER_MAXIMUM,
    )
    assert (
        _free_trial.get_free_messages_per_user_override()
        == _free_trial.FREE_MESSAGES_PER_USER_MAXIMUM
    )


@pytest.mark.parametrize(
    "bad_value", [-1, 10_001, 15.5, True, "abc", None],
)
def test_set_override_rejects_invalid(bad_value):
    """Invalid values raise ``ValueError`` and leave the cache
    untouched."""
    with pytest.raises(ValueError):
        _free_trial.set_free_messages_per_user_override(bad_value)
    assert _free_trial.get_free_messages_per_user_override() is None


def test_set_override_rejects_bool_explicitly():
    """Same defensive note as the coerce-reject test: the override
    setter MUST refuse ``bool`` so a buggy caller posting ``True``
    can't silently degrade the trial to 1 message."""
    with pytest.raises(ValueError, match="not bool"):
        _free_trial.set_free_messages_per_user_override(True)


def test_clear_override_returns_bool():
    """``clear_*_override`` returns whether one was active."""
    assert _free_trial.clear_free_messages_per_user_override() is False
    _free_trial.set_free_messages_per_user_override(7)
    assert _free_trial.clear_free_messages_per_user_override() is True
    # Idempotent.
    assert _free_trial.clear_free_messages_per_user_override() is False


# =====================================================================
# get_free_messages_per_user — resolution order
# =====================================================================


def test_resolution_default_when_no_override_no_env(monkeypatch):
    """No override + no env → compile-time default (10)."""
    monkeypatch.delenv("FREE_MESSAGES_PER_USER", raising=False)
    assert _free_trial.get_free_messages_per_user() == 10
    assert _free_trial.get_free_messages_per_user_source() == "default"


def test_resolution_env_when_no_override(monkeypatch):
    """No override + valid env → env wins."""
    monkeypatch.setenv("FREE_MESSAGES_PER_USER", "25")
    assert _free_trial.get_free_messages_per_user() == 25
    assert _free_trial.get_free_messages_per_user_source() == "env"


def test_resolution_override_beats_env(monkeypatch):
    """Override + valid env → override wins."""
    monkeypatch.setenv("FREE_MESSAGES_PER_USER", "25")
    _free_trial.set_free_messages_per_user_override(50)
    assert _free_trial.get_free_messages_per_user() == 50
    assert _free_trial.get_free_messages_per_user_source() == "db"


def test_resolution_invalid_env_falls_back(monkeypatch):
    """Invalid env → default. Source reflects the actual fallback."""
    monkeypatch.setenv("FREE_MESSAGES_PER_USER", "not-a-number")
    assert _free_trial.get_free_messages_per_user() == 10
    assert _free_trial.get_free_messages_per_user_source() == "default"


def test_resolution_out_of_range_env_falls_back(monkeypatch):
    """Env value out of [0, 10_000] → default."""
    monkeypatch.setenv("FREE_MESSAGES_PER_USER", "999999")
    assert _free_trial.get_free_messages_per_user() == 10
    assert _free_trial.get_free_messages_per_user_source() == "default"


# =====================================================================
# refresh_free_messages_per_user_override_from_db
# =====================================================================


@pytest.mark.asyncio
async def test_refresh_loads_valid_override():
    """Happy path: a valid string in ``system_settings`` populates
    the override cache."""
    db = type("DB", (), {})()
    db.get_setting = AsyncMock(return_value="42")
    loaded = await (
        _free_trial.refresh_free_messages_per_user_override_from_db(db)
    )
    assert loaded == 42
    assert _free_trial.get_free_messages_per_user_override() == 42


@pytest.mark.asyncio
async def test_refresh_clears_when_row_missing():
    """No row in ``system_settings`` clears the in-memory cache (which
    is what we want — operator deleted the row, the bot should
    immediately fall through to env / default)."""
    _free_trial.set_free_messages_per_user_override(50)
    db = type("DB", (), {})()
    db.get_setting = AsyncMock(return_value=None)
    loaded = await (
        _free_trial.refresh_free_messages_per_user_override_from_db(db)
    )
    assert loaded is None
    assert _free_trial.get_free_messages_per_user_override() is None


@pytest.mark.asyncio
async def test_refresh_clears_on_invalid_stored_value():
    """A malformed stored value is treated as 'no override' rather
    than crashing the bot. Logged at WARNING (not asserted here —
    the log path is incidental to the contract)."""
    _free_trial.set_free_messages_per_user_override(50)
    db = type("DB", (), {})()
    db.get_setting = AsyncMock(return_value="not-a-number")
    loaded = await (
        _free_trial.refresh_free_messages_per_user_override_from_db(db)
    )
    assert loaded is None
    assert _free_trial.get_free_messages_per_user_override() is None


@pytest.mark.asyncio
async def test_refresh_keeps_previous_on_db_error():
    """Critical fail-soft: a transient DB error MUST keep the
    previous cache in place. A pool blip should not silently revert
    the trial allowance to env / default mid-incident."""
    _free_trial.set_free_messages_per_user_override(50)
    db = type("DB", (), {})()
    db.get_setting = AsyncMock(side_effect=RuntimeError("DB down"))
    loaded = await (
        _free_trial.refresh_free_messages_per_user_override_from_db(db)
    )
    assert loaded == 50  # previous cache preserved
    assert _free_trial.get_free_messages_per_user_override() == 50


@pytest.mark.asyncio
async def test_refresh_handles_none_db():
    """A ``None`` db (local dev / boot before pool is ready) returns
    the current cache rather than crashing. Mirrors the same shape
    the other refresh helpers use."""
    _free_trial.set_free_messages_per_user_override(33)
    loaded = await (
        _free_trial.refresh_free_messages_per_user_override_from_db(None)
    )
    assert loaded == 33


@pytest.mark.asyncio
async def test_refresh_clears_when_row_is_out_of_range():
    """Defence-in-depth: a tampered row with ``"99999"`` (above the
    cap) is treated as 'no override' rather than letting it slide
    through and lock the bot into a free-forever state."""
    db = type("DB", (), {})()
    db.get_setting = AsyncMock(return_value="99999")
    loaded = await (
        _free_trial.refresh_free_messages_per_user_override_from_db(db)
    )
    assert loaded is None
    assert _free_trial.get_free_messages_per_user_override() is None


@pytest.mark.asyncio
async def test_refresh_clears_when_row_is_negative():
    """Sibling to the cap test: a negative row is rejected too."""
    db = type("DB", (), {})()
    db.get_setting = AsyncMock(return_value="-5")
    loaded = await (
        _free_trial.refresh_free_messages_per_user_override_from_db(db)
    )
    assert loaded is None
    assert _free_trial.get_free_messages_per_user_override() is None


# =====================================================================
# Database.create_user wiring (Stage-15-Step-E #10b row 6 surface)
# =====================================================================


@pytest.mark.asyncio
async def test_create_user_passes_resolved_allowance_to_sql():
    """The new ``create_user`` SQL has a ``$3`` parameter bound to
    the resolved allowance. Pin the wiring: monkey-patch
    ``get_free_messages_per_user`` and assert the bind value
    threads through."""
    from unittest.mock import AsyncMock, MagicMock
    from database import Database

    # Build a minimal Database fixture with a stubbed pool / connection.
    db = Database.__new__(Database)
    db.pool = MagicMock()
    conn = AsyncMock()
    db.pool.acquire = MagicMock(
        return_value=_AsyncCM(conn),
    )

    _free_trial.set_free_messages_per_user_override(42)
    await db.create_user(123, "alice")

    # Assert SQL + 3 args (telegram_id, username, free_msgs).
    args, kwargs = conn.execute.call_args
    sql = args[0]
    assert "free_messages_left" in sql
    assert args[1] == 123
    assert args[2] == "alice"
    assert args[3] == 42  # the override


class _AsyncCM:
    """Async-context-manager wrapper around a stub connection so
    ``async with self.pool.acquire() as connection:`` works."""

    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False
