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


# ---- per-key 429 / fallback counters (Stage-15-Step-E #4 follow-up) -


def test_get_key_429_counters_returns_empty_initially():
    """A fresh pool has no recorded 429s."""
    _setup_three_keys()
    assert openrouter_keys.get_key_429_counters() == {}
    _reset_env()


def test_get_key_fallback_counters_returns_empty_initially():
    """A fresh pool has no recorded fallbacks."""
    _setup_three_keys()
    assert openrouter_keys.get_key_fallback_counters() == {}
    _reset_env()


def test_mark_key_rate_limited_increments_per_key_429_counter():
    """Each ``mark_key_rate_limited`` call bumps the 429 counter for
    the matching pool index."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k1")
    counters = openrouter_keys.get_key_429_counters()
    # ``k1`` is at pool idx 1 (declaration order in _setup_three_keys).
    assert counters == {1: 1}
    # Re-marking the same already-cooled key still counts the second
    # 429 event — the operator wants to see every event in the rate
    # plot, even if the second one didn't change the cooldown
    # deadline.
    openrouter_keys.mark_key_rate_limited("k1")
    counters = openrouter_keys.get_key_429_counters()
    assert counters == {1: 2}
    _reset_env()


def test_429_counter_is_per_index_not_per_key_string():
    """The counter family is keyed by 0-based pool index so the
    rendered ``/metrics`` body never carries the api_key string.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0")
    openrouter_keys.mark_key_rate_limited("k2")
    counters = openrouter_keys.get_key_429_counters()
    assert counters == {0: 1, 2: 1}
    # No string-keyed entry leaked into the dict.
    for key in counters:
        assert isinstance(key, int)
    _reset_env()


def test_mark_unknown_key_does_not_increment_counter():
    """``mark_key_rate_limited`` for a key not in the pool no-ops the
    cooldown table AND skips the 429 counter — otherwise a stale
    reference would inflate a slot that no longer exists."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("not-in-pool")
    assert openrouter_keys.get_key_429_counters() == {}
    _reset_env()


def test_fallback_counter_increments_against_absorbing_index():
    """``key_for_user`` records the fallback count against the
    *absorbing* pool slot, not the sticky source slot — so a
    "fallback rate per key" plot answers 'which key is taking the
    load when others go hot'."""
    _setup_three_keys()
    # User id 1 → sticky idx 1 → "k1"
    openrouter_keys.mark_key_rate_limited("k1")
    fallback = openrouter_keys.key_for_user(1)
    counters = openrouter_keys.get_key_fallback_counters()
    # Walk-forward order is sticky+1, sticky+2, ... so the next
    # available slot is idx 2 ("k2").
    assert fallback == "k2"
    assert counters == {2: 1}
    _reset_env()


def test_fallback_counter_not_incremented_when_sticky_available():
    """The user's sticky key was available — no fallback occurred,
    so the counter stays at zero. Defends against a future refactor
    that drops the early-return branch."""
    _setup_three_keys()
    # User id 0 → sticky idx 0 → "k0" (not in cooldown)
    openrouter_keys.key_for_user(0)
    assert openrouter_keys.get_key_fallback_counters() == {}
    _reset_env()


def test_fallback_counter_not_incremented_when_all_keys_cooled():
    """When every key is in cooldown, the picker falls back to the
    sticky pick (best-effort) — that's NOT a fallback to a different
    slot, so the counter stays at zero. The sticky-pick log is
    enough; the counter is reserved for genuine cross-slot routes."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0")
    openrouter_keys.mark_key_rate_limited("k1")
    openrouter_keys.mark_key_rate_limited("k2")
    # User id 1 → sticky idx 1 → "k1" — but k1 is hot AND every other
    # slot is hot too, so we get the sticky pick back.
    assert openrouter_keys.key_for_user(1) == "k1"
    assert openrouter_keys.get_key_fallback_counters() == {}
    _reset_env()


def test_reset_key_counters_for_tests_clears_both_dicts():
    """The tests-only reset wipes both counter dicts so each case
    starts from a known-zero state."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k1")
    openrouter_keys._increment_key_fallback(2)
    assert openrouter_keys.get_key_429_counters()
    assert openrouter_keys.get_key_fallback_counters()
    openrouter_keys.reset_key_counters_for_tests()
    assert openrouter_keys.get_key_429_counters() == {}
    assert openrouter_keys.get_key_fallback_counters() == {}
    _reset_env()


def test_load_keys_resets_per_key_counters():
    """Hot reload must reset the per-index counters — otherwise idx 0
    in the new pool inherits idx 0's count from the old pool, even
    though the underlying api_key is a completely different
    deployment slot. Tested here to pin the contract."""
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0")
    openrouter_keys.mark_key_rate_limited("k1")
    assert openrouter_keys.get_key_429_counters() == {0: 1, 1: 1}
    # Reload with a different pool — counters must reset.
    os.environ.pop("OPENROUTER_API_KEY_1", None)
    os.environ.pop("OPENROUTER_API_KEY_2", None)
    os.environ.pop("OPENROUTER_API_KEY_3", None)
    os.environ["OPENROUTER_API_KEY_1"] = "new-k0"
    os.environ["OPENROUTER_API_KEY_2"] = "new-k1"
    openrouter_keys.load_keys()
    assert openrouter_keys.get_key_429_counters() == {}
    assert openrouter_keys.get_key_fallback_counters() == {}
    _reset_env()


def test_increment_key_429_negative_index_is_noop():
    """Defence in depth: a future caller resolving an index from a
    stale snapshot must not be able to poison the counter dict
    with a negative slot."""
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys._increment_key_429(-1)
    openrouter_keys._increment_key_fallback(-1)
    assert openrouter_keys.get_key_429_counters() == {}
    assert openrouter_keys.get_key_fallback_counters() == {}


# ---- bundled bug fix: load_keys evicts stale cooldown entries -----


def test_load_keys_evicts_stale_cooldown_entries():
    """Bundled bug fix (Stage-15-Step-E #4 follow-up #2):
    ``load_keys`` must drop cooldown entries whose api_key isn't in
    the freshly-loaded pool. Pre-fix, a hot key rotation
    (operator script that swaps keys to dodge upstream throttling)
    left dangling entries in ``_cooldowns`` for up to
    ``MAX_COOLDOWN_SECS`` (1 hour), violating the comment-asserted
    invariant that the cooldown table size is bounded by
    ``len(_keys)``.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0")
    openrouter_keys.mark_key_rate_limited("k1")
    assert "k0" in openrouter_keys._cooldowns
    assert "k1" in openrouter_keys._cooldowns

    # Hot rotation: keep "k1" in the new pool but drop "k0" and
    # add a brand-new "kX". Post-load, the cooldown for "k0"
    # must be gone (its key no longer exists); the cooldown for
    # "k1" must also be gone because load_keys resets on a
    # composition change.
    for k in ("OPENROUTER_API_KEY_1", "OPENROUTER_API_KEY_2",
              "OPENROUTER_API_KEY_3"):
        os.environ.pop(k, None)
    os.environ["OPENROUTER_API_KEY_1"] = "k1"
    os.environ["OPENROUTER_API_KEY_2"] = "kX"
    openrouter_keys.load_keys()

    assert "k0" not in openrouter_keys._cooldowns
    # Sanity: pool size bound holds again.
    assert len(openrouter_keys._cooldowns) <= len(openrouter_keys._keys)
    _reset_env()


def test_load_keys_preserves_cooldowns_for_keys_still_in_pool():
    """The eviction rule must NOT drop cooldowns for keys that
    survived the rotation. (Bug-fix would over-eagerly clear
    everything, defeating the purpose of the cooldown table.)

    NOTE: load_keys() *does* clear cooldowns whose api_key is no
    longer in the pool. A key that IS still in the pool but
    whose deadline is still active should still be in cooldown
    after the reload, even though we've rotated the env around
    it. This pins that invariant so a future "always clear all"
    refactor doesn't slip through.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k1")
    assert "k1" in openrouter_keys._cooldowns

    # Reload with the SAME pool composition — k1 stays.
    openrouter_keys.load_keys()
    assert "k1" in openrouter_keys._cooldowns
    _reset_env()


# ---- Stage-15-Step-E #4 follow-up #2: DB-backed key registry --------


class _StubDB:
    """Minimal duck-typed DB stub for ``refresh_from_db`` tests."""

    def __init__(self, rows):
        self._rows = rows
        self.calls = 0

    async def list_enabled_openrouter_keys_with_secret(self):
        self.calls += 1
        if isinstance(self._rows, Exception):
            raise self._rows
        return self._rows


@pytest.mark.asyncio
async def test_refresh_from_db_appends_db_keys_to_env_pool():
    """Env keys load first; DB-backed enabled rows append after."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "env-1"
    db = _StubDB([
        {"id": 11, "label": "main", "api_key": "db-1"},
        {"id": 12, "label": "backup", "api_key": "db-2"},
    ])
    pool_size = await openrouter_keys.refresh_from_db(db)
    assert pool_size == 3
    assert openrouter_keys.key_count() == 3
    meta = openrouter_keys.get_key_meta_snapshot()
    assert meta[0]["source"] == "env"
    assert meta[1] == {"source": "db", "label": "main", "db_id": 11}
    assert meta[2] == {"source": "db", "label": "backup", "db_id": 12}
    _reset_env()


@pytest.mark.asyncio
async def test_refresh_from_db_idempotent_preserves_counters():
    """Calling refresh twice with the same DB rows must NOT reset
    per-key counters — that's what lets the admin GET path refresh
    on every page load without nuking the request/429 numbers."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "env-1"
    db = _StubDB([{"id": 11, "label": "main", "api_key": "db-1"}])
    await openrouter_keys.refresh_from_db(db)
    # Simulate a 429 on env-1 + a few requests on db-1 (idx 1). We
    # use telegram_id=1 to stick to idx 1 so the request counter
    # for idx 1 bumps directly.
    openrouter_keys.mark_key_rate_limited("env-1", retry_after_secs=42.0)
    openrouter_keys.key_for_user(1)  # sticks to idx 1
    openrouter_keys.key_for_user(1)
    counters_before_429 = openrouter_keys.get_key_429_counters()
    counters_before_req = openrouter_keys.get_key_request_counters()
    assert counters_before_429.get(0, 0) >= 1
    assert counters_before_req.get(1, 0) >= 2
    # Second refresh with identical rows is a no-op.
    await openrouter_keys.refresh_from_db(db)
    assert openrouter_keys.get_key_429_counters() == counters_before_429
    assert openrouter_keys.get_key_request_counters() == counters_before_req
    _reset_env()


@pytest.mark.asyncio
async def test_refresh_from_db_rebuilds_when_pool_changes():
    """Adding a new DB key must trigger a rebuild — counters can
    legitimately reset because the pool composition changed."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "env-1"
    db = _StubDB([{"id": 11, "label": "a", "api_key": "db-1"}])
    await openrouter_keys.refresh_from_db(db)
    assert openrouter_keys.key_count() == 2
    # Add a 2nd db key.
    db._rows = [
        {"id": 11, "label": "a", "api_key": "db-1"},
        {"id": 12, "label": "b", "api_key": "db-2"},
    ]
    await openrouter_keys.refresh_from_db(db)
    assert openrouter_keys.key_count() == 3
    meta = openrouter_keys.get_key_meta_snapshot()
    assert meta[2] == {"source": "db", "label": "b", "db_id": 12}
    _reset_env()


@pytest.mark.asyncio
async def test_refresh_from_db_skips_duplicate_against_env():
    """A DB row whose api_key already lives in env is skipped (not
    inserted twice). Defends against an operator who copy-pasted a
    key into both surfaces."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "shared-key"
    db = _StubDB([
        {"id": 99, "label": "dup", "api_key": "shared-key"},
        {"id": 100, "label": "fresh", "api_key": "fresh-key"},
    ])
    await openrouter_keys.refresh_from_db(db)
    assert openrouter_keys.key_count() == 2
    assert openrouter_keys._keys == ["shared-key", "fresh-key"]
    _reset_env()


@pytest.mark.asyncio
async def test_refresh_from_db_handles_db_error_gracefully():
    """A transient DB error must keep the env pool in place rather
    than blanking it."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "env-1"
    openrouter_keys.load_keys()
    db = _StubDB(RuntimeError("boom"))
    pool_size = await openrouter_keys.refresh_from_db(db)
    assert pool_size == 1
    assert openrouter_keys._keys == ["env-1"]
    _reset_env()


@pytest.mark.asyncio
async def test_refresh_from_db_handles_non_list_return():
    """A buggy stub that returns AsyncMock / None / anything-not-a-list
    must be tolerated — same fail-safe shape as the threshold-overrides
    path."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "env-1"

    class _BadDB:
        async def list_enabled_openrouter_keys_with_secret(self):
            return "not a list"

    pool_size = await openrouter_keys.refresh_from_db(_BadDB())
    assert pool_size == 1
    _reset_env()


@pytest.mark.asyncio
async def test_refresh_from_db_with_none_db_falls_through_to_env():
    """Passing ``None`` for the DB falls back to a pure env load."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "env-only"
    pool_size = await openrouter_keys.refresh_from_db(None)
    assert pool_size == 1
    assert openrouter_keys._keys == ["env-only"]
    _reset_env()


def test_key_for_user_bumps_request_counter():
    """Each ``key_for_user`` call must bump the request counter for
    the picked slot."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "k0"
    os.environ["OPENROUTER_API_KEY_2"] = "k1"
    openrouter_keys.load_keys()
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys.key_for_user(0)
    openrouter_keys.key_for_user(0)
    openrouter_keys.key_for_user(1)
    counts = openrouter_keys.get_key_request_counters()
    assert counts.get(0, 0) == 2
    assert counts.get(1, 0) == 1
    _reset_env()


def test_key_for_user_request_counter_bumps_on_fallback():
    """When the sticky pick is hot, the fallback slot's request
    counter must bump — not the sticky slot's."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "k0"
    os.environ["OPENROUTER_API_KEY_2"] = "k1"
    openrouter_keys.load_keys()
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys.mark_key_rate_limited("k0", retry_after_secs=60.0)
    # User 0 sticks to idx 0 (k0) but it's hot → fallback to idx 1.
    picked = openrouter_keys.key_for_user(0)
    assert picked == "k1"
    counts = openrouter_keys.get_key_request_counters()
    assert counts.get(0, 0) == 0
    assert counts.get(1, 0) == 1
    _reset_env()


def test_load_keys_marks_env_source_in_meta():
    """``load_keys()`` populates ``_KEY_META`` with ``source='env'``
    for every env-loaded slot so the panel can render the source
    column even on env-only deploys."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "k1"
    os.environ["OPENROUTER_API_KEY_2"] = "k2"
    openrouter_keys.load_keys()
    meta = openrouter_keys.get_key_meta_snapshot()
    assert len(meta) == 2
    assert all(m == {"source": "env"} for m in meta)


# ---- Stage-15-Step-F follow-up #5: _read_env_keys() bug fix --------


def test_read_env_keys_numbered_only():
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "n1"
    os.environ["OPENROUTER_API_KEY_2"] = "n2"
    assert openrouter_keys._read_env_keys() == ["n1", "n2"]
    _reset_env()


def test_read_env_keys_bare_only():
    _reset_env()
    os.environ["OPENROUTER_API_KEY"] = "bare"
    assert openrouter_keys._read_env_keys() == ["bare"]
    _reset_env()


def test_read_env_keys_empty_when_nothing_set():
    _reset_env()
    assert openrouter_keys._read_env_keys() == []


def test_read_env_keys_numbered_overrides_bare_matches_load_keys():
    """Bug fix regression: when both bare and numbered are set,
    ``_read_env_keys`` MUST mirror ``load_keys`` and ignore the bare
    value. Pre-fix it returned ``[bare, *numbered]``, causing
    :func:`refresh_from_db` to compute a "desired" pool one entry
    longer than the post-``load_keys`` pool — the rebuild branch
    then duplicated the last numbered slot, and the no-op fast path
    NEVER fired even when the env was unchanged. The right answer:
    drop the bare entirely, return only numbered."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY"] = "bare"
    os.environ["OPENROUTER_API_KEY_1"] = "n1"
    os.environ["OPENROUTER_API_KEY_2"] = "n2"

    # _read_env_keys returns numbered only.
    assert openrouter_keys._read_env_keys() == ["n1", "n2"]

    # And load_keys() puts the same set of keys into ``_keys`` —
    # the post-condition the fix exists to preserve.
    openrouter_keys.load_keys()
    assert openrouter_keys._keys == ["n1", "n2"]
    _reset_env()


def test_read_env_keys_dedupes_numbered_slots():
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "same"
    os.environ["OPENROUTER_API_KEY_2"] = "same"
    os.environ["OPENROUTER_API_KEY_3"] = "different"
    assert openrouter_keys._read_env_keys() == ["same", "different"]
    _reset_env()


@pytest.mark.asyncio
async def test_refresh_from_db_no_op_fast_path_with_bare_and_numbered():
    """Pre-fix this would have rebuilt every refresh AND duplicated
    a key — the fast path's equality check on ``_keys == desired``
    failed because ``desired`` over-counted by one. After the fix
    the second refresh is a true no-op."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY"] = "bare-ignored"
    os.environ["OPENROUTER_API_KEY_1"] = "n1"
    os.environ["OPENROUTER_API_KEY_2"] = "n2"
    db = _StubDB([{"id": 5, "label": "x", "api_key": "db-x"}])

    # First refresh: pool is built from scratch.
    await openrouter_keys.refresh_from_db(db)
    assert openrouter_keys._keys == ["n1", "n2", "db-x"]
    # Second refresh: identical inputs → must be a no-op (no
    # duplicate "db-x" appended). Pre-fix this assertion would
    # fail with ``["n1", "n2", "db-x", "db-x"]``.
    await openrouter_keys.refresh_from_db(db)
    assert openrouter_keys._keys == ["n1", "n2", "db-x"]
    _reset_env()


# ── Stage-15-Step-E #4 follow-up #3: per-key 24h usage tracker ──────


def test_get_key_24h_usage_returns_empty_initially():
    """Fresh process / fresh test → no 24h usage entries."""
    openrouter_keys.reset_key_counters_for_tests()
    assert openrouter_keys.get_key_24h_usage() == {}


def test_record_usage_at_idx_appends_to_buffer():
    """``_record_usage_at_idx`` appends a (timestamp, cost) pair."""
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys._record_usage_at_idx(0, 0.0125)
    openrouter_keys._record_usage_at_idx(0, 0.0375)
    openrouter_keys._record_usage_at_idx(1, 0.0050)
    snapshot = openrouter_keys.get_key_24h_usage()
    assert snapshot[0]["requests"] == 2.0
    # Float rounding tolerance — use approx.
    assert snapshot[0]["cost_usd"] == pytest.approx(0.05, rel=1e-9)
    assert snapshot[1]["requests"] == 1.0
    assert snapshot[1]["cost_usd"] == pytest.approx(0.0050, rel=1e-9)
    openrouter_keys.reset_key_counters_for_tests()


def test_record_usage_at_idx_negative_index_is_noop():
    """Defence in depth: a stale snapshot shouldn't poison the
    buffer with a negative index."""
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys._record_usage_at_idx(-1, 0.5)
    assert openrouter_keys.get_key_24h_usage() == {}


def test_record_usage_at_idx_rejects_non_finite_cost():
    """NaN / -Inf / +Inf must coerce to 0.0 — a poisoned cost
    shouldn't permanently corrupt the panel's 24h sum."""
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys._record_usage_at_idx(0, float("nan"))
    openrouter_keys._record_usage_at_idx(0, float("inf"))
    openrouter_keys._record_usage_at_idx(0, float("-inf"))
    openrouter_keys._record_usage_at_idx(0, -1.0)  # negative
    snapshot = openrouter_keys.get_key_24h_usage()
    # Four entries, all with cost coerced to 0.0.
    assert snapshot[0]["requests"] == 4.0
    assert snapshot[0]["cost_usd"] == 0.0
    openrouter_keys.reset_key_counters_for_tests()


def test_record_usage_at_idx_handles_non_numeric_cost():
    """A non-numeric cost (e.g. a stringly-typed pricing bug)
    must coerce to 0.0 rather than raise. The buffer is for ops
    eyes-on, not a financial ledger."""
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys._record_usage_at_idx(0, "not-a-number")  # type: ignore[arg-type]
    snapshot = openrouter_keys.get_key_24h_usage()
    assert snapshot[0]["requests"] == 1.0
    assert snapshot[0]["cost_usd"] == 0.0
    openrouter_keys.reset_key_counters_for_tests()


def test_get_key_24h_usage_trims_expired_entries():
    """Entries older than 24h must be evicted on read."""
    openrouter_keys.reset_key_counters_for_tests()
    import time as _time
    now = _time.time()
    # Entry from 25h ago — must be evicted.
    openrouter_keys._record_usage_at_idx(
        0, 1.0, ts=now - 25 * 3600,
    )
    # Entry from 23h ago — must survive.
    openrouter_keys._record_usage_at_idx(
        0, 2.0, ts=now - 23 * 3600,
    )
    # Entry from 1s ago — must survive.
    openrouter_keys._record_usage_at_idx(
        0, 3.0, ts=now - 1,
    )
    snapshot = openrouter_keys.get_key_24h_usage()
    assert snapshot[0]["requests"] == 2.0
    assert snapshot[0]["cost_usd"] == pytest.approx(5.0, rel=1e-9)
    openrouter_keys.reset_key_counters_for_tests()


def test_get_key_24h_usage_evicts_idx_when_buffer_empties():
    """When all entries for an idx expire, the idx must drop out
    of the dict so the dict size stays bounded by the active key
    set, not the historical key set."""
    openrouter_keys.reset_key_counters_for_tests()
    import time as _time
    now = _time.time()
    # Both entries from 30h ago — both must evict.
    openrouter_keys._record_usage_at_idx(0, 1.0, ts=now - 30 * 3600)
    openrouter_keys._record_usage_at_idx(0, 2.0, ts=now - 29 * 3600)
    snapshot = openrouter_keys.get_key_24h_usage()
    assert snapshot == {}
    # Internal state is also cleaned up.
    assert 0 not in openrouter_keys._KEY_USAGE_BUCKETS
    openrouter_keys.reset_key_counters_for_tests()


def test_record_usage_safety_cap_evicts_oldest_entries():
    """Buffer must self-limit at ``_KEY_USAGE_MAX_ENTRIES``
    so a buggy caller can't OOM the process."""
    openrouter_keys.reset_key_counters_for_tests()
    cap = openrouter_keys._KEY_USAGE_MAX_ENTRIES
    # Patch to a smaller cap for the test so we don't have to
    # append 100k entries.
    original_cap = openrouter_keys._KEY_USAGE_MAX_ENTRIES
    openrouter_keys._KEY_USAGE_MAX_ENTRIES = 100
    try:
        for i in range(150):
            openrouter_keys._record_usage_at_idx(0, 0.001)
        # Buffer should be capped well below 150.
        assert len(openrouter_keys._KEY_USAGE_BUCKETS[0]) < 150
        assert len(openrouter_keys._KEY_USAGE_BUCKETS[0]) <= 100
    finally:
        openrouter_keys._KEY_USAGE_MAX_ENTRIES = original_cap
        openrouter_keys.reset_key_counters_for_tests()


def test_idx_for_api_key_returns_pool_index():
    """Reverse lookup finds the right index for a known key."""
    _setup_three_keys()
    assert openrouter_keys._idx_for_api_key("k0") == 0
    assert openrouter_keys._idx_for_api_key("k1") == 1
    assert openrouter_keys._idx_for_api_key("k2") == 2
    _reset_env()


def test_idx_for_api_key_unknown_key_returns_none():
    """A key not in the current pool returns None — a stale
    reference (rotated-out key) won't poison the buffer."""
    _setup_three_keys()
    assert openrouter_keys._idx_for_api_key("not-in-pool") is None
    assert openrouter_keys._idx_for_api_key("") is None
    _reset_env()


@pytest.mark.asyncio
async def test_record_key_usage_appends_to_buffer():
    """The public ``record_key_usage(api_key, cost)`` entry point
    appends to the 24h buffer for the matching pool index."""
    _setup_three_keys()
    openrouter_keys.reset_key_counters_for_tests()
    await openrouter_keys.record_key_usage("k1", 0.0125)
    await openrouter_keys.record_key_usage("k1", 0.0375)
    snapshot = openrouter_keys.get_key_24h_usage()
    assert snapshot[1]["requests"] == 2.0
    assert snapshot[1]["cost_usd"] == pytest.approx(0.05, rel=1e-9)
    # k0 / k2 have no entries so they don't appear in the snapshot.
    assert 0 not in snapshot
    assert 2 not in snapshot
    _reset_env()


@pytest.mark.asyncio
async def test_record_key_usage_unknown_key_silently_noops():
    """A stale api_key reference (key rotated out between request
    start and finish) is silently ignored — no exception, no
    buffer entry."""
    _setup_three_keys()
    openrouter_keys.reset_key_counters_for_tests()
    await openrouter_keys.record_key_usage("rotated-out-key", 1.0)
    assert openrouter_keys.get_key_24h_usage() == {}
    _reset_env()


@pytest.mark.asyncio
async def test_record_key_usage_calls_mark_openrouter_key_used_for_db_key():
    """Bug fix: ``mark_openrouter_key_used`` is now invoked when
    ``record_key_usage`` is called for a DB-loaded key. Pre-fix
    the DB column ``last_used_at`` was never updated — the panel's
    "Last used" column always rendered ``—``."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "env-k0"
    db = _StubDB([{"id": 42, "label": "main", "api_key": "db-key-1"}])
    await openrouter_keys.refresh_from_db(db)
    # Pool is now [env-k0, db-key-1] with idx 1 = db source.
    assert openrouter_keys._keys == ["env-k0", "db-key-1"]

    # Patch the DB stub to also track mark_openrouter_key_used calls.
    mark_calls: list[int] = []

    async def fake_mark(key_id):
        mark_calls.append(key_id)

    db.mark_openrouter_key_used = fake_mark  # type: ignore[attr-defined]

    await openrouter_keys.record_key_usage("db-key-1", 0.025, db=db)
    assert mark_calls == [42], (
        "DB key should bump last_used_at via mark_openrouter_key_used"
    )
    _reset_env()


@pytest.mark.asyncio
async def test_record_key_usage_skips_mark_for_env_keys():
    """Env-loaded keys have no DB row → no
    ``mark_openrouter_key_used`` call."""
    _setup_three_keys()
    openrouter_keys.reset_key_counters_for_tests()

    mark_calls: list[int] = []

    class _DB:
        async def mark_openrouter_key_used(self, key_id):
            mark_calls.append(key_id)

    db = _DB()
    await openrouter_keys.record_key_usage("k1", 0.5, db=db)
    assert mark_calls == []
    _reset_env()


@pytest.mark.asyncio
async def test_record_key_usage_with_none_db_skips_mark_branch():
    """``db=None`` is the contract for "don't bump last_used_at"
    — used by tests / scripts that don't have a DB handle."""
    _setup_three_keys()
    openrouter_keys.reset_key_counters_for_tests()
    # Should not raise even without a db.
    await openrouter_keys.record_key_usage("k1", 0.5, db=None)
    assert openrouter_keys.get_key_24h_usage()[1]["requests"] == 1.0
    _reset_env()


@pytest.mark.asyncio
async def test_record_key_usage_swallows_db_error():
    """A transient DB error on ``mark_openrouter_key_used`` must
    not block the user-facing AI reply. The in-memory 24h buffer
    must still be populated."""
    _reset_env()
    os.environ["OPENROUTER_API_KEY_1"] = "env-k0"
    db = _StubDB([{"id": 7, "label": "x", "api_key": "db-key-1"}])
    await openrouter_keys.refresh_from_db(db)

    async def boom(key_id):
        raise RuntimeError("transient DB blip")

    db.mark_openrouter_key_used = boom  # type: ignore[attr-defined]

    # Must NOT raise.
    await openrouter_keys.record_key_usage("db-key-1", 0.125, db=db)

    # Buffer was still populated — that's the contract.
    snapshot = openrouter_keys.get_key_24h_usage()
    assert snapshot[1]["requests"] == 1.0
    assert snapshot[1]["cost_usd"] == pytest.approx(0.125, rel=1e-9)
    _reset_env()


def test_load_keys_resets_24h_usage_buffer():
    """Hot reload must reset the per-index 24h buffer alongside
    the 429/fallback counters — otherwise idx 0 in the new pool
    inherits idx 0's 24h history from the old pool, even though
    the underlying api_key is a different deployment slot."""
    _setup_three_keys()
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys._record_usage_at_idx(0, 0.5)
    openrouter_keys._record_usage_at_idx(1, 0.25)
    assert openrouter_keys.get_key_24h_usage() != {}

    # Reload with a different pool — buffer must reset.
    os.environ.pop("OPENROUTER_API_KEY_1", None)
    os.environ.pop("OPENROUTER_API_KEY_2", None)
    os.environ.pop("OPENROUTER_API_KEY_3", None)
    os.environ["OPENROUTER_API_KEY_1"] = "new-k0"
    openrouter_keys.load_keys()
    assert openrouter_keys.get_key_24h_usage() == {}
    _reset_env()


def test_reset_key_counters_for_tests_clears_24h_buffer():
    """Tests-only reset wipes the 24h buffer too so each case
    starts from zero."""
    _setup_three_keys()
    openrouter_keys._record_usage_at_idx(0, 0.5)
    openrouter_keys._record_usage_at_idx(1, 0.25)
    assert openrouter_keys.get_key_24h_usage() != {}
    openrouter_keys.reset_key_counters_for_tests()
    assert openrouter_keys.get_key_24h_usage() == {}
    _reset_env()
