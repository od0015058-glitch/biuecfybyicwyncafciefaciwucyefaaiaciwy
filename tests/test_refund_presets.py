"""Stage-15-Step-E #10b row 28: ``REFUND_PRESETS``.

Three layers covered here, mirroring the test plan
:mod:`tests.test_free_trial` uses for ``FREE_MESSAGES_PER_USER``:

1. **Pure coercion** —
   :func:`refund_presets.coerce_refund_presets` and
   :func:`refund_presets.parse_refund_presets_text` under
   happy-path lists, mixed types, NUL bytes, length cap, count
   cap, dedupe (case-insensitive), and the storage round-trip
   via :func:`refund_presets.encode_refund_presets_for_storage`.

2. **Override get / set / clear** with the integration to
   :func:`refund_presets.refresh_refund_presets_override_from_db`.

3. **Public lookup** —
   :func:`refund_presets.get_refund_presets` resolution
   (override → env → default) +
   :func:`refund_presets.get_refund_presets_source` returning
   ``"db"`` / ``"env"`` / ``"default"``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import refund_presets as _refund_presets


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture(autouse=True)
def reset_override():
    """Every test starts with no override and restores afterwards."""
    _refund_presets.clear_refund_presets_override()
    yield
    _refund_presets.clear_refund_presets_override()


# =====================================================================
# coerce_refund_presets — happy paths
# =====================================================================


def test_coerce_strips_whitespace_and_dedupes_case_insensitively():
    """Whitespace stripped, NUL stripped, case-insensitive dedupe with
    first-seen wins so the operator's preferred capitalisation is
    preserved."""
    raw = [
        "  Duplicate Payment  ",
        "duplicate payment",
        "USER cancellation",
        "user cancellation",
        "User Cancellation",
        "Bot error",
    ]
    out = _refund_presets.coerce_refund_presets(raw)
    assert out == ["Duplicate Payment", "USER cancellation", "Bot error"]


def test_coerce_truncates_oversize_to_max_length():
    """A single preset over MAX_PRESET_LENGTH gets truncated, NOT
    rejected — a hostile-on-paste experience would push operators
    back to free-text."""
    long = "a" * (_refund_presets.MAX_PRESET_LENGTH + 30)
    out = _refund_presets.coerce_refund_presets([long])
    assert len(out) == 1
    assert len(out[0]) == _refund_presets.MAX_PRESET_LENGTH


def test_coerce_drops_empty_after_strip():
    """Blanks / whitespace-only / NUL-only inputs are silently
    dropped so a stray newline in a paste doesn't blank a slot."""
    raw = ["", "   ", "\x00", "\x00\x00\x00", "Real reason"]
    out = _refund_presets.coerce_refund_presets(raw)
    assert out == ["Real reason"]


def test_coerce_caps_at_max_count():
    """Beyond MAX_PRESET_COUNT the tail is silently truncated."""
    raw = [f"reason-{i}" for i in range(_refund_presets.MAX_PRESET_COUNT + 5)]
    out = _refund_presets.coerce_refund_presets(raw)
    assert len(out) == _refund_presets.MAX_PRESET_COUNT


def test_coerce_ignores_non_string_entries():
    """An int / None / dict in the iterable is dropped; the rest of
    the list still survives."""
    raw = [
        None, 123, {"reason": "duplicate"},
        ["nested", "list"], "Real reason",
    ]
    out = _refund_presets.coerce_refund_presets(raw)
    assert out == ["Real reason"]


# =====================================================================
# parse_refund_presets_text — newline + pipe split
# =====================================================================


def test_parse_text_splits_on_newlines():
    raw = "Duplicate payment\nUser-requested cancellation\nFraud"
    assert _refund_presets.parse_refund_presets_text(raw) == [
        "Duplicate payment",
        "User-requested cancellation",
        "Fraud",
    ]


def test_parse_text_splits_on_pipes():
    raw = "Duplicate | User cancel | Fraud"
    assert _refund_presets.parse_refund_presets_text(raw) == [
        "Duplicate",
        "User cancel",
        "Fraud",
    ]


def test_parse_text_handles_mixed_separators():
    raw = "A | B\nC|D\n\n  | E"
    out = _refund_presets.parse_refund_presets_text(raw)
    assert out == ["A", "B", "C", "D", "E"]


def test_parse_text_empty_input_yields_empty_list():
    assert _refund_presets.parse_refund_presets_text("") == []
    assert _refund_presets.parse_refund_presets_text("   \n\n") == []


def test_parse_text_non_string_input_yields_empty_list():
    """Defensive: a caller passing ``None`` or an int gets ``[]``."""
    assert _refund_presets.parse_refund_presets_text(None) == []  # type: ignore[arg-type]
    assert _refund_presets.parse_refund_presets_text(42) == []  # type: ignore[arg-type]


# =====================================================================
# encode_refund_presets_for_storage — round-trip
# =====================================================================


def test_encode_round_trips_via_decode():
    """The encoder's output decodes back to the same list."""
    presets = ["Duplicate payment", "User cancel", "Bot error"]
    encoded = _refund_presets.encode_refund_presets_for_storage(presets)
    decoded = _refund_presets._decode_stored_presets(encoded)
    assert decoded == presets


def test_encode_preserves_unicode():
    """A Persian preset round-trips losslessly via
    ``ensure_ascii=False``."""
    presets = ["پرداخت تکراری", "بازگشت کاربر"]
    encoded = _refund_presets.encode_refund_presets_for_storage(presets)
    assert "\\u" not in encoded  # not ascii-escaped
    decoded = _refund_presets._decode_stored_presets(encoded)
    assert decoded == presets


def test_encode_max_payload_fits_column():
    """The documented worst case (MAX_PRESET_COUNT × MAX_PRESET_LENGTH)
    serialises to a string strictly under the 255-char column cap."""
    worst = ["x" * _refund_presets.MAX_PRESET_LENGTH] * (
        _refund_presets.MAX_PRESET_COUNT
    )
    # Dedup will collapse identical strings — use unique long strings.
    worst = [
        ("x" * (_refund_presets.MAX_PRESET_LENGTH - 1)) + str(i)
        for i in range(_refund_presets.MAX_PRESET_COUNT)
    ]
    encoded = _refund_presets.encode_refund_presets_for_storage(worst)
    assert len(encoded) < 255


def test_decode_rejects_non_array():
    """A stored value that decoded to a non-list returns ``None``
    (caller falls through to env / default rather than serving
    garbage)."""
    assert _refund_presets._decode_stored_presets('"oops"') is None
    assert _refund_presets._decode_stored_presets('{"k": "v"}') is None
    assert _refund_presets._decode_stored_presets("not json") is None


# =====================================================================
# set / get / clear override
# =====================================================================


def test_set_returns_coerced_list():
    out = _refund_presets.set_refund_presets_override(
        ["  A  ", "a", "B"]  # dedupe + strip
    )
    assert out == ["A", "B"]
    assert _refund_presets.get_refund_presets_override() == ["A", "B"]


def test_get_override_returns_independent_copy():
    """The returned list is a copy — caller mutations must NOT
    affect the cache."""
    _refund_presets.set_refund_presets_override(["A", "B"])
    snapshot = _refund_presets.get_refund_presets_override()
    snapshot.append("C")  # type: ignore[union-attr]
    assert _refund_presets.get_refund_presets_override() == ["A", "B"]


def test_set_empty_list_means_explicit_hide():
    """An empty-list override is a real override (the operator's
    explicit "hide the dropdown" choice), NOT the same as None."""
    _refund_presets.set_refund_presets_override([])
    assert _refund_presets.get_refund_presets_override() == []
    # Resolution returns empty list, NOT the default.
    assert _refund_presets.get_refund_presets() == []


def test_clear_returns_true_when_active():
    _refund_presets.set_refund_presets_override(["A"])
    assert _refund_presets.clear_refund_presets_override() is True
    assert _refund_presets.clear_refund_presets_override() is False


# =====================================================================
# get_refund_presets — resolution order
# =====================================================================


def test_resolution_default_when_no_override_no_env(monkeypatch):
    monkeypatch.delenv("REFUND_PRESETS", raising=False)
    assert (
        _refund_presets.get_refund_presets()
        == list(_refund_presets.DEFAULT_REFUND_PRESETS)
    )
    assert _refund_presets.get_refund_presets_source() == "default"


def test_resolution_env_when_no_override(monkeypatch):
    monkeypatch.setenv(
        "REFUND_PRESETS", "Refund A\nRefund B|Refund C"
    )
    assert _refund_presets.get_refund_presets() == [
        "Refund A", "Refund B", "Refund C",
    ]
    assert _refund_presets.get_refund_presets_source() == "env"


def test_resolution_override_beats_env(monkeypatch):
    monkeypatch.setenv(
        "REFUND_PRESETS", "Env A\nEnv B"
    )
    _refund_presets.set_refund_presets_override(["DB only"])
    assert _refund_presets.get_refund_presets() == ["DB only"]
    assert _refund_presets.get_refund_presets_source() == "db"


def test_resolution_invalid_env_falls_back(monkeypatch):
    """An env value that parses to an empty list (every line was
    blank / NUL only) falls through to the compile-time default
    rather than serving an accidentally-hidden dropdown.

    NOTE: We can't put a literal ``\\x00`` in the env var via
    ``monkeypatch.setenv`` because POSIX-level ``setenv`` rejects
    embedded NULs (the OS will raise ``ValueError: embedded null
    byte``). Use a whitespace-only string for the same effect —
    parse_refund_presets_text strips whitespace per-entry, so an
    all-blank input maps to an empty list which the resolver
    treats as 'env unset'."""
    monkeypatch.setenv("REFUND_PRESETS", "\n   \n   ")
    assert (
        _refund_presets.get_refund_presets()
        == list(_refund_presets.DEFAULT_REFUND_PRESETS)
    )
    assert _refund_presets.get_refund_presets_source() == "default"


def test_resolution_explicit_empty_override_hides_dropdown(monkeypatch):
    """An empty-list override beats env + default."""
    monkeypatch.setenv("REFUND_PRESETS", "Env A")
    _refund_presets.set_refund_presets_override([])
    assert _refund_presets.get_refund_presets() == []
    assert _refund_presets.get_refund_presets_source() == "db"


# =====================================================================
# refresh_refund_presets_override_from_db
# =====================================================================


@pytest.mark.asyncio
async def test_refresh_loads_valid_override():
    """Happy path: a valid JSON list in ``system_settings`` populates
    the override cache."""
    db = type("DB", (), {})()
    db.get_setting = AsyncMock(return_value='["A", "B"]')
    loaded = await (
        _refund_presets.refresh_refund_presets_override_from_db(db)
    )
    assert loaded == ["A", "B"]
    assert _refund_presets.get_refund_presets_override() == ["A", "B"]


@pytest.mark.asyncio
async def test_refresh_clears_when_row_missing():
    """No row in ``system_settings`` clears the in-memory cache."""
    _refund_presets.set_refund_presets_override(["stale"])
    db = type("DB", (), {})()
    db.get_setting = AsyncMock(return_value=None)
    loaded = await (
        _refund_presets.refresh_refund_presets_override_from_db(db)
    )
    assert loaded is None
    assert _refund_presets.get_refund_presets_override() is None


@pytest.mark.asyncio
async def test_refresh_clears_on_invalid_stored_value():
    """A malformed stored value (non-JSON, non-array) is treated as
    'no override'. Logged at WARNING (not asserted here)."""
    _refund_presets.set_refund_presets_override(["stale"])
    db = type("DB", (), {})()
    db.get_setting = AsyncMock(return_value="not json")
    loaded = await (
        _refund_presets.refresh_refund_presets_override_from_db(db)
    )
    assert loaded is None
    assert _refund_presets.get_refund_presets_override() is None


@pytest.mark.asyncio
async def test_refresh_keeps_previous_on_db_error():
    """Critical fail-soft: a transient DB error MUST keep the
    previous cache in place. A pool blip should not silently revert
    the dropdown to env / default mid-incident."""
    _refund_presets.set_refund_presets_override(["Saved A", "Saved B"])
    db = type("DB", (), {})()
    db.get_setting = AsyncMock(side_effect=RuntimeError("DB down"))
    loaded = await (
        _refund_presets.refresh_refund_presets_override_from_db(db)
    )
    assert loaded == ["Saved A", "Saved B"]


@pytest.mark.asyncio
async def test_refresh_handles_none_db():
    """A ``None`` db (local dev / boot before pool is ready) returns
    the current cache rather than crashing."""
    _refund_presets.set_refund_presets_override(["A"])
    loaded = await (
        _refund_presets.refresh_refund_presets_override_from_db(None)
    )
    assert loaded == ["A"]


@pytest.mark.asyncio
async def test_refresh_loads_empty_list_override():
    """An explicit empty-list override round-trips through the
    DB serialisation: ``"[]"`` decodes to ``[]`` which is the
    operator's explicit 'hide the dropdown' choice."""
    db = type("DB", (), {})()
    db.get_setting = AsyncMock(return_value="[]")
    loaded = await (
        _refund_presets.refresh_refund_presets_override_from_db(db)
    )
    assert loaded == []
    assert _refund_presets.get_refund_presets_override() == []
    assert _refund_presets.get_refund_presets() == []
