"""Tests for openrouter_keys — multi-key load balancing.

Stage-14-Step-C.
"""

from __future__ import annotations

import os

import pytest

import openrouter_keys


def _reset_env(*keys_to_clear: str):
    """Remove all OPENROUTER_API_KEY* env vars for a clean test.

    Also clears the lazy-load flag so the next ``key_for_user`` /
    ``key_count`` call re-reads the env (Stage-15-Step-D #2) AND
    wipes any leftover cooldown entries from the per-key rate-limit
    table (Stage-15-Step-E #4) so the next test starts hot.
    """
    for k in list(os.environ):
        if k.startswith("OPENROUTER_API_KEY"):
            os.environ.pop(k, None)
    for k in keys_to_clear:
        os.environ.pop(k, None)
    openrouter_keys._keys = []
    openrouter_keys._loaded = False
    openrouter_keys.clear_all_cooldowns()


# ---- load_keys -----------------------------------------------------


def test_bare_key_only():
    _reset_env()
    os.environ["OPENROUTER_API_KEY"] = "bare-key"
    openrouter_keys.load_keys()
    assert openrouter_keys.key_count() == 1
    assert openrouter_keys.key_for_user(12345) == "bare-key"
    _reset_env()


def test_numbered_keys():
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "key-one"
    os.environ["OPENROUTER_API_KEY_2"] = "key-two"
    openrouter_keys.load_keys()
    assert openrouter_keys.key_count() == 2
    _reset_env()


def test_numbered_overrides_bare():
    _reset_env()
    os.environ["OPENROUTER_API_KEY"] = "bare-key"
    os.environ["OPENROUTER_API_KEY_1"] = "key-one"
    openrouter_keys.load_keys()
    assert openrouter_keys.key_count() == 1
    assert openrouter_keys.key_for_user(0) == "key-one"
    _reset_env()


def test_no_keys_raises():
    _reset_env()
    openrouter_keys.load_keys()
    with pytest.raises(RuntimeError, match="No OpenRouter API keys"):
        openrouter_keys.key_for_user(1)
    _reset_env()


# ---- key_for_user sticky assignment ---------------------------------


def test_sticky_assignment():
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "key-a"
    os.environ["OPENROUTER_API_KEY_2"] = "key-b"
    os.environ["OPENROUTER_API_KEY_3"] = "key-c"
    openrouter_keys.load_keys()

    # Same user always gets the same key.
    user_42 = openrouter_keys.key_for_user(42)
    for _ in range(100):
        assert openrouter_keys.key_for_user(42) == user_42

    _reset_env()


def test_distribution_across_keys():
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "k1"
    os.environ["OPENROUTER_API_KEY_2"] = "k2"
    openrouter_keys.load_keys()

    # With modulo 2, even user ids get k1, odd get k2.
    assert openrouter_keys.key_for_user(100) == "k1"
    assert openrouter_keys.key_for_user(101) == "k2"

    _reset_env()


def test_empty_numbered_key_skipped():
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "real-key"
    os.environ["OPENROUTER_API_KEY_2"] = ""
    os.environ["OPENROUTER_API_KEY_3"] = "  "
    openrouter_keys.load_keys()
    assert openrouter_keys.key_count() == 1
    _reset_env()


# ---- lazy-load (Stage-15-Step-D #2) --------------------------------


def test_key_for_user_lazy_loads_without_explicit_load_keys_call():
    """``key_for_user`` triggers ``load_keys`` on first call.

    Pre-Stage-15-Step-D the module ran ``load_keys()`` at import time,
    so a test that monkeypatched ``OPENROUTER_API_KEY`` *after* import
    saw ``_keys`` as the snapshot from the cold import (often empty)
    unless it explicitly re-called ``load_keys``. The lazy-load
    contract makes ``key_for_user`` just work.
    """
    _reset_env()
    assert openrouter_keys._loaded is False
    os.environ["OPENROUTER_API_KEY"] = "lazy-loaded-key"
    # No explicit load_keys() call.
    assert openrouter_keys.key_for_user(0) == "lazy-loaded-key"
    assert openrouter_keys._loaded is True
    _reset_env()


def test_key_count_lazy_loads_without_explicit_load_keys_call():
    """``key_count`` is the other public entry point — same contract."""
    _reset_env()
    assert openrouter_keys._loaded is False
    os.environ["OPENROUTER_API_KEY_1"] = "k1"
    os.environ["OPENROUTER_API_KEY_2"] = "k2"
    assert openrouter_keys.key_count() == 2
    assert openrouter_keys._loaded is True
    _reset_env()


def test_lazy_load_runs_only_once_per_process():
    """A second access does not re-read env (would mask hot env edits,
    but the contract is "lazy *first* load"; explicit ``load_keys``
    is the supported path for re-reading).
    """
    _reset_env()
    os.environ["OPENROUTER_API_KEY"] = "first"
    assert openrouter_keys.key_for_user(0) == "first"
    # Now mutate env without calling load_keys() — the cached value wins.
    os.environ["OPENROUTER_API_KEY"] = "second"
    assert openrouter_keys.key_for_user(0) == "first"
    # Explicit reload picks up the change.
    openrouter_keys.load_keys()
    assert openrouter_keys.key_for_user(0) == "second"
    _reset_env()


def test_lazy_load_with_no_keys_still_raises():
    """An importer that calls ``key_for_user`` without configuring
    keys gets the same RuntimeError it always did — the lazy-load
    refactor doesn't change this contract.
    """
    _reset_env()
    with pytest.raises(RuntimeError, match="No OpenRouter API keys"):
        openrouter_keys.key_for_user(0)
    # Lazy load did run; it just produced an empty pool.
    assert openrouter_keys._loaded is True
    assert openrouter_keys.key_count() == 0
    _reset_env()


def test_load_keys_resets_loaded_flag():
    """``load_keys()`` always re-reads env and re-sets ``_loaded``."""
    _reset_env()
    openrouter_keys._loaded = False
    os.environ["OPENROUTER_API_KEY"] = "k"
    openrouter_keys.load_keys()
    assert openrouter_keys._loaded is True
    assert openrouter_keys.key_count() == 1
    _reset_env()


# ---- per-key 429 cooldown (Stage-15-Step-E #4) ---------------------


def _setup_three_keys():
    """Helper: load a deterministic 3-key pool keyed 'k0', 'k1', 'k2'."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "k0"
    os.environ["OPENROUTER_API_KEY_2"] = "k1"
    os.environ["OPENROUTER_API_KEY_3"] = "k2"
    openrouter_keys.load_keys()


def test_mark_key_rate_limited_puts_key_in_cooldown():
    """``mark_key_rate_limited`` flips ``is_key_rate_limited`` to True."""
    _setup_three_keys()
    assert not openrouter_keys.is_key_rate_limited("k1")
    openrouter_keys.mark_key_rate_limited("k1")
    assert openrouter_keys.is_key_rate_limited("k1")
    _reset_env()


def test_key_for_user_skips_cooldown_key():
    """When the user's sticky key is hot, route them to the next pool member."""
    _setup_three_keys()
    # User id 1 → sticky idx 1 → "k1"
    assert openrouter_keys.key_for_user(1) == "k1"
    openrouter_keys.mark_key_rate_limited("k1")
    fallback = openrouter_keys.key_for_user(1)
    assert fallback != "k1"
    assert fallback in {"k0", "k2"}
    _reset_env()


def test_key_for_user_returns_sticky_key_when_not_rate_limited():
    """Cooldown only kicks in for the marked key — others stay sticky."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k1")
    # User id 0 → sticky idx 0 → "k0" (not rate-limited)
    assert openrouter_keys.key_for_user(0) == "k0"
    # User id 2 → sticky idx 2 → "k2" (not rate-limited)
    assert openrouter_keys.key_for_user(2) == "k2"
    _reset_env()


def test_key_for_user_falls_back_to_sticky_when_all_cooled():
    """Every key in cooldown → return the sticky pick anyway, log warning."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0")
    openrouter_keys.mark_key_rate_limited("k1")
    openrouter_keys.mark_key_rate_limited("k2")
    # User id 1 → sticky idx 1 → "k1" — cooled, but we still return it.
    assert openrouter_keys.key_for_user(1) == "k1"
    _reset_env()


def test_cooldown_expires_after_deadline(monkeypatch):
    """A cooldown deadline in the past frees the key on next read."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k1", retry_after_secs=0.01)
    assert openrouter_keys.is_key_rate_limited("k1")
    # Fast-forward time past the deadline. ``time.monotonic`` is the
    # only timing source ``openrouter_keys`` reads, so monkeypatching
    # it advances the cooldown clock without sleeping.
    real_monotonic = openrouter_keys.time.monotonic
    monkeypatch.setattr(
        openrouter_keys.time,
        "monotonic",
        lambda: real_monotonic() + 60.0,
    )
    assert not openrouter_keys.is_key_rate_limited("k1")
    # Deadline-driven cleanup should have removed the entry.
    assert "k1" not in openrouter_keys._cooldowns
    _reset_env()


def test_mark_key_rate_limited_default_cooldown_secs():
    """When ``retry_after_secs`` is ``None``, default 60s applies."""
    _setup_three_keys()
    before = openrouter_keys.time.monotonic()
    openrouter_keys.mark_key_rate_limited("k0")
    after = openrouter_keys.time.monotonic()
    deadline = openrouter_keys._cooldowns["k0"]
    # Deadline is in [before+60, after+60].
    assert before + openrouter_keys.DEFAULT_COOLDOWN_SECS <= deadline
    assert deadline <= after + openrouter_keys.DEFAULT_COOLDOWN_SECS + 0.5
    _reset_env()


def test_mark_key_rate_limited_explicit_retry_after():
    """An explicit numeric Retry-After is honoured."""
    _setup_three_keys()
    before = openrouter_keys.time.monotonic()
    openrouter_keys.mark_key_rate_limited("k0", retry_after_secs=5.0)
    deadline = openrouter_keys._cooldowns["k0"]
    assert before + 5.0 <= deadline <= before + 6.0
    _reset_env()


def test_mark_key_rate_limited_clamps_excessive_retry_after():
    """A 24h Retry-After clamps down to MAX_COOLDOWN_SECS (1h)."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0", retry_after_secs=86_400.0)
    deadline = openrouter_keys._cooldowns["k0"]
    now = openrouter_keys.time.monotonic()
    assert deadline - now <= openrouter_keys.MAX_COOLDOWN_SECS + 0.5
    _reset_env()


def test_mark_key_rate_limited_falls_back_on_nan_retry_after():
    """NaN / Inf / negative Retry-After → default cooldown."""
    import math as _math

    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=_math.nan
    )
    deadline_nan = openrouter_keys._cooldowns["k0"]
    openrouter_keys.clear_all_cooldowns()
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=_math.inf
    )
    deadline_inf = openrouter_keys._cooldowns["k0"]
    openrouter_keys.clear_all_cooldowns()
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=-5.0
    )
    deadline_neg = openrouter_keys._cooldowns["k0"]
    now = openrouter_keys.time.monotonic()
    # All three should be ~DEFAULT_COOLDOWN_SECS into the future.
    for d in (deadline_nan, deadline_inf, deadline_neg):
        assert (
            now + openrouter_keys.DEFAULT_COOLDOWN_SECS - 1.0
            <= d
            <= now + openrouter_keys.DEFAULT_COOLDOWN_SECS + 1.0
        )
    _reset_env()


def test_mark_key_rate_limited_falls_back_on_non_numeric():
    """Non-numeric Retry-After header → default cooldown."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs="not-a-number"
    )
    deadline = openrouter_keys._cooldowns["k0"]
    now = openrouter_keys.time.monotonic()
    assert (
        now + openrouter_keys.DEFAULT_COOLDOWN_SECS - 1.0
        <= deadline
        <= now + openrouter_keys.DEFAULT_COOLDOWN_SECS + 1.0
    )
    _reset_env()


def test_mark_key_rate_limited_keeps_longer_existing_cooldown():
    """A second 429 with a smaller Retry-After must not shorten an
    already-running longer cooldown.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0", retry_after_secs=300.0)
    long_deadline = openrouter_keys._cooldowns["k0"]
    openrouter_keys.mark_key_rate_limited("k0", retry_after_secs=10.0)
    assert openrouter_keys._cooldowns["k0"] == long_deadline
    _reset_env()


def test_mark_key_rate_limited_extends_to_longer_new_cooldown():
    """If the new Retry-After is longer than the running cooldown, extend it."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0", retry_after_secs=10.0)
    short_deadline = openrouter_keys._cooldowns["k0"]
    openrouter_keys.mark_key_rate_limited("k0", retry_after_secs=300.0)
    assert openrouter_keys._cooldowns["k0"] > short_deadline
    _reset_env()


def test_mark_key_rate_limited_unknown_key_is_noop():
    """Marking a key that isn't in the pool is a logged no-op — we
    don't want a stale 429 to bloat the cooldown table.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("not-in-pool")
    assert "not-in-pool" not in openrouter_keys._cooldowns
    _reset_env()


def test_mark_key_rate_limited_empty_string_is_noop():
    """Empty / falsy api_key is silently ignored."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("")
    assert openrouter_keys._cooldowns == {}
    _reset_env()


def test_is_key_rate_limited_empty_string_is_false():
    _setup_three_keys()
    assert openrouter_keys.is_key_rate_limited("") is False
    _reset_env()


def test_available_key_count_reflects_cooldowns():
    _setup_three_keys()
    assert openrouter_keys.available_key_count() == 3
    openrouter_keys.mark_key_rate_limited("k1")
    assert openrouter_keys.available_key_count() == 2
    openrouter_keys.mark_key_rate_limited("k0")
    assert openrouter_keys.available_key_count() == 1
    openrouter_keys.mark_key_rate_limited("k2")
    assert openrouter_keys.available_key_count() == 0
    _reset_env()


def test_clear_all_cooldowns_wipes_table():
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0")
    openrouter_keys.mark_key_rate_limited("k1")
    assert openrouter_keys._cooldowns
    openrouter_keys.clear_all_cooldowns()
    assert openrouter_keys._cooldowns == {}
    assert openrouter_keys.available_key_count() == 3
    _reset_env()


def test_key_status_snapshot_shape():
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k1", retry_after_secs=42.0)
    snap = openrouter_keys.key_status_snapshot()
    assert len(snap) == 3
    # Indices in declaration order.
    assert [row["index"] for row in snap] == [0, 1, 2]
    assert snap[0]["rate_limited"] is False
    assert snap[0]["cooldown_remaining_secs"] is None
    assert snap[1]["rate_limited"] is True
    assert isinstance(snap[1]["cooldown_remaining_secs"], float)
    assert 0.0 < snap[1]["cooldown_remaining_secs"] <= 42.0
    assert snap[2]["rate_limited"] is False
    # Snapshot must NOT leak the api_key strings into its rows.
    for row in snap:
        assert "api_key" not in row
        assert "key" not in row
    _reset_env()


def test_cooldown_state_is_independent_of_pool_size():
    """A single-key pool with that key in cooldown still falls back to
    the sticky pick (so the user gets at least an attempt).
    """
    _reset_env()
    os.environ["OPENROUTER_API_KEY"] = "only-key"
    openrouter_keys.load_keys()
    openrouter_keys.mark_key_rate_limited("only-key")
    assert openrouter_keys.key_for_user(99) == "only-key"
    _reset_env()
