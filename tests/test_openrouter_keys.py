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


# ---- Stage-15-Step-E #4 follow-up #4: per-(key, model) cooldown ----


def test_mark_key_rate_limited_with_model_uses_per_model_table():
    """Calling ``mark_key_rate_limited(key, model="x/y")`` writes to
    ``_per_model_cooldowns`` and NOT the whole-key ``_cooldowns``.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k1", model="x/y")
    assert ("k1", "x/y") in openrouter_keys._per_model_cooldowns
    assert "k1" not in openrouter_keys._cooldowns
    _reset_env()


def test_mark_key_rate_limited_without_model_uses_whole_key_table():
    """Calling ``mark_key_rate_limited(key)`` (no model kwarg) keeps
    the back-compat behaviour: writes to the whole-key
    ``_cooldowns`` table.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k1")
    assert "k1" in openrouter_keys._cooldowns
    assert not any(
        pair[0] == "k1" for pair in openrouter_keys._per_model_cooldowns
    )
    _reset_env()


def test_is_key_rate_limited_with_model_consults_both_tables():
    """``is_key_rate_limited(key, model=m)`` returns True when EITHER
    the whole-key cooldown OR the (key, model) cooldown is active.
    """
    _setup_three_keys()
    # Per-model only.
    openrouter_keys.mark_key_rate_limited("k0", model="vendor/foo")
    assert openrouter_keys.is_key_rate_limited("k0", model="vendor/foo")
    assert not openrouter_keys.is_key_rate_limited("k0", model="vendor/bar")
    # Whole-key only also blocks the model query.
    openrouter_keys.mark_key_rate_limited("k1")
    assert openrouter_keys.is_key_rate_limited("k1", model="anything/x")
    _reset_env()


def test_is_key_rate_limited_without_model_only_whole_key():
    """Back-compat: ``is_key_rate_limited(key)`` (no model kwarg) only
    checks the whole-key table — a per-(key, model) entry alone
    should not flip the membership check.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0", model="vendor/foo")
    assert not openrouter_keys.is_key_rate_limited("k0")
    _reset_env()


def test_per_model_cooldown_blocks_only_that_model_in_picker():
    """``key_for_user(uid, model=m)`` must skip a slot whose
    (api_key, m) pair is in cooldown but pick that same slot
    happily for a *different* model.
    """
    _setup_three_keys()
    # User 1 → sticky idx 1 → "k1"
    openrouter_keys.mark_key_rate_limited("k1", model="paid/expensive")
    # Same user, OTHER model: sticky still works.
    assert openrouter_keys.key_for_user(1, model="paid/cheap") == "k1"
    # Same user, the cooled model: walks to next slot ("k2").
    assert openrouter_keys.key_for_user(1, model="paid/expensive") == "k2"
    _reset_env()


def test_per_model_cooldown_does_not_affect_no_model_picker():
    """Back-compat: ``key_for_user(uid)`` (no model kwarg) doesn't
    consult the per-(key, model) table — only the whole-key
    cooldown can divert it.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k1", model="anything/x")
    # No model context → the per-(key, model) entry is ignored.
    assert openrouter_keys.key_for_user(1) == "k1"
    _reset_env()


def test_available_key_count_with_model_excludes_per_model_blocks():
    """``available_key_count(model=m)`` excludes slots blocked for
    that model. ``available_key_count()`` (no model) ignores the
    per-(key, model) table.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0", model="hot/free")
    openrouter_keys.mark_key_rate_limited("k1", model="hot/free")
    assert openrouter_keys.available_key_count(model="hot/free") == 1
    # Different model: all 3 keys still available.
    assert openrouter_keys.available_key_count(model="cool/paid") == 3
    # No model context: also all 3 (no whole-key cooldowns).
    assert openrouter_keys.available_key_count() == 3
    _reset_env()


def test_per_model_cooldown_expires_lazily(monkeypatch):
    """Per-(key, model) cooldown expires on read like the whole-key
    table does. After the deadline elapses,
    ``is_key_rate_limited(..., model=...)`` returns False AND the
    entry is pruned.
    """
    _setup_three_keys()
    # Mark with a small Retry-After we'll accelerate past.
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=5, model="fast/expire",
    )
    assert ("k0", "fast/expire") in openrouter_keys._per_model_cooldowns

    real_monotonic = openrouter_keys.time.monotonic
    advanced = [real_monotonic() + 10.0]
    monkeypatch.setattr(
        openrouter_keys.time, "monotonic", lambda: advanced[0]
    )
    assert not openrouter_keys.is_key_rate_limited(
        "k0", model="fast/expire",
    )
    # And the entry was pruned by the lazy-expiry side effect.
    assert ("k0", "fast/expire") not in openrouter_keys._per_model_cooldowns
    _reset_env()


def test_drop_expired_cooldowns_prunes_per_model_table():
    """``_drop_expired_cooldowns`` operates on both tables in
    lockstep so the per-model table self-cleans without needing
    its own sweeper.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0", model="model/a")
    openrouter_keys.mark_key_rate_limited("k1", model="model/b")
    # Force entries to be in the past.
    far_past = openrouter_keys.time.monotonic() - 1000.0
    openrouter_keys._per_model_cooldowns[("k0", "model/a")] = far_past
    openrouter_keys._per_model_cooldowns[("k1", "model/b")] = far_past
    openrouter_keys._drop_expired_cooldowns()
    assert openrouter_keys._per_model_cooldowns == {}
    _reset_env()


def test_clear_all_cooldowns_wipes_per_model_table():
    """``clear_all_cooldowns`` wipes the per-(key, model) table too —
    leaving it half-cleared after an ops "force back online"
    button would be the worst-of-both-worlds bug.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0")
    openrouter_keys.mark_key_rate_limited("k1", model="model/a")
    assert "k0" in openrouter_keys._cooldowns
    assert ("k1", "model/a") in openrouter_keys._per_model_cooldowns
    openrouter_keys.clear_all_cooldowns()
    assert openrouter_keys._cooldowns == {}
    assert openrouter_keys._per_model_cooldowns == {}
    _reset_env()


def test_load_keys_evicts_stale_per_model_cooldown_entries():
    """``load_keys`` must drop per-(key, model) entries whose api_key
    is no longer in the new pool — same eviction discipline the
    whole-key table follows.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k1", model="model/a")
    assert ("k1", "model/a") in openrouter_keys._per_model_cooldowns
    # Hot rotation: drop k1 from the env; reload.
    os.environ.pop("OPENROUTER_API_KEY_2", None)
    openrouter_keys.load_keys()
    # k1 is no longer in the pool; the per-(k1, *) entry must be
    # evicted alongside the whole-key table eviction.
    assert ("k1", "model/a") not in openrouter_keys._per_model_cooldowns
    _reset_env()


def test_mark_per_model_keeps_longer_existing_deadline():
    """Two back-to-back 429s for the same (key, model) with a
    *shorter* second Retry-After must NOT shorten the first
    cooldown. Same "keep the longer deadline" discipline the
    whole-key table follows.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=300, model="model/x",
    )
    long_deadline = openrouter_keys._per_model_cooldowns[("k0", "model/x")]
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=10, model="model/x",
    )
    assert (
        openrouter_keys._per_model_cooldowns[("k0", "model/x")]
        == long_deadline
    )
    _reset_env()


def test_mark_per_model_extends_to_longer_new_deadline():
    """Conversely: a longer second Retry-After EXTENDS the
    cooldown. Same as the whole-key behaviour.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=10, model="model/x",
    )
    short_deadline = openrouter_keys._per_model_cooldowns[("k0", "model/x")]
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=300, model="model/x",
    )
    new_deadline = openrouter_keys._per_model_cooldowns[("k0", "model/x")]
    assert new_deadline > short_deadline
    _reset_env()


def test_mark_per_model_with_blank_or_none_falls_back_to_whole_key():
    """``model=""`` / ``model=None`` / ``model="   "`` collapse via
    ``_normalise_model`` to "no model context" — the cooldown
    lands on the whole-key table, preserving back-compat for
    callers that pass a falsy value.
    """
    _setup_three_keys()
    for falsy in (None, "", "   ", "\t\n"):
        openrouter_keys.clear_all_cooldowns()
        openrouter_keys.mark_key_rate_limited("k0", model=falsy)
        assert "k0" in openrouter_keys._cooldowns, (
            f"falsy model={falsy!r} should collapse to whole-key"
        )
        assert openrouter_keys._per_model_cooldowns == {}
    _reset_env()


def test_mark_per_model_strips_surrounding_whitespace():
    """Surrounding whitespace on the model id is stripped before
    keying the cooldown table — ``"  vendor/x  "`` and
    ``"vendor/x"`` are the same model from a routing POV.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0", model="  vendor/x  ")
    assert ("k0", "vendor/x") in openrouter_keys._per_model_cooldowns
    assert ("k0", "  vendor/x  ") not in openrouter_keys._per_model_cooldowns
    # ``is_key_rate_limited`` strips too so the membership check
    # finds the canonical entry.
    assert openrouter_keys.is_key_rate_limited(
        "k0", model="vendor/x",
    )
    assert openrouter_keys.is_key_rate_limited(
        "k0", model="  vendor/x  ",
    )
    _reset_env()


def test_mark_per_model_does_not_lower_case_model():
    """OpenRouter ids are case-sensitive — ``vendor/X`` and
    ``vendor/x`` are different routes. Verify the cooldown key
    keeps the original case.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0", model="vendor/X")
    assert ("k0", "vendor/X") in openrouter_keys._per_model_cooldowns
    # Querying with the lower-case form must NOT match.
    assert not openrouter_keys.is_key_rate_limited(
        "k0", model="vendor/x",
    )
    _reset_env()


def test_mark_per_model_increments_429_counter():
    """A per-model cooldown still bumps the per-key 429 counter —
    ops aggregating "429s seen against this key" want every
    429 counted regardless of which table absorbed it.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k1", model="model/x")
    counts = openrouter_keys.get_key_429_counters()
    assert counts.get(1, 0) == 1
    _reset_env()


def test_per_model_cooldown_snapshot_returns_active_pairs_only():
    """``per_model_cooldown_snapshot`` returns one row per active
    (key, model) cooldown with ``index``, ``model``, and
    ``cooldown_remaining_secs > 0``. Expired entries are excluded.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=120, model="vendor/a",
    )
    openrouter_keys.mark_key_rate_limited(
        "k1", retry_after_secs=120, model="vendor/b",
    )
    snap = openrouter_keys.per_model_cooldown_snapshot()
    assert len(snap) == 2
    rows_by_key = {(row["index"], row["model"]): row for row in snap}
    assert (0, "vendor/a") in rows_by_key
    assert (1, "vendor/b") in rows_by_key
    for row in snap:
        assert row["cooldown_remaining_secs"] > 0
    _reset_env()


def test_per_model_cooldown_snapshot_filters_rotated_keys():
    """A per-(key, model) entry whose api_key has been rotated out
    is filtered from the snapshot — rendering an idx that no
    longer exists would be misleading. Same discipline as
    ``get_key_24h_usage``.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited(
        "k1", retry_after_secs=120, model="vendor/a",
    )
    # Force a stale entry: simulate "key rotated out" by clearing
    # the load flag and removing k1 from _keys directly. The next
    # snapshot read should filter it out (idx_by_key won't find
    # "k1" so the row is skipped).
    openrouter_keys._keys = ["k0", "k2"]
    snap = openrouter_keys.per_model_cooldown_snapshot()
    assert all(row["model"] != "vendor/a" for row in snap)
    _reset_env()


def test_per_model_cooldown_snapshot_sorts_deterministically():
    """Snapshot rows are sorted by (index, model) so test
    assertions and Prometheus label ordering are deterministic
    across scrapes / runs.
    """
    _setup_three_keys()
    # Mark out of order.
    openrouter_keys.mark_key_rate_limited("k2", model="vendor/c")
    openrouter_keys.mark_key_rate_limited("k0", model="vendor/b")
    openrouter_keys.mark_key_rate_limited("k0", model="vendor/a")
    openrouter_keys.mark_key_rate_limited("k1", model="vendor/x")
    snap = openrouter_keys.per_model_cooldown_snapshot()
    keys = [(row["index"], row["model"]) for row in snap]
    assert keys == sorted(keys)
    _reset_env()


def test_key_status_snapshot_with_model_folds_per_model_into_view():
    """``key_status_snapshot(model=m)`` reflects (key, m) cooldowns
    in the per-slot ``rate_limited`` flag and reports the LATER
    of the two deadlines (whichever expires later).
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=120, model="vendor/x",
    )
    snap = openrouter_keys.key_status_snapshot(model="vendor/x")
    assert snap[0]["rate_limited"] is True
    assert 119.0 <= snap[0]["cooldown_remaining_secs"] <= 121.0
    # No-model snapshot doesn't see the per-model entry.
    snap_global = openrouter_keys.key_status_snapshot()
    assert snap_global[0]["rate_limited"] is False
    _reset_env()


def test_key_status_snapshot_with_model_takes_max_of_both_deadlines():
    """When a slot has BOTH a whole-key cooldown AND a per-(key,m)
    cooldown active, ``cooldown_remaining_secs`` is the max of
    the two — the slot only becomes usable when *both* expire.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=30,
    )
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=300, model="vendor/x",
    )
    snap = openrouter_keys.key_status_snapshot(model="vendor/x")
    # Whole-key 30s vs per-model 300s → max is ~300s.
    assert 295.0 <= snap[0]["cooldown_remaining_secs"] <= 305.0
    _reset_env()


def test_key_for_user_walks_past_per_model_blocked_slots():
    """When a user's sticky slot is per-model-blocked AND the next
    slot is per-model-blocked too, the picker walks all the way
    around the pool until it finds an available slot.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited("k0", model="hot/x")
    openrouter_keys.mark_key_rate_limited("k1", model="hot/x")
    # User 0 → sticky idx 0 → "k0" (blocked).
    # Walk: k1 (blocked), k2 (free).
    assert openrouter_keys.key_for_user(0, model="hot/x") == "k2"
    _reset_env()


def test_key_for_user_falls_back_to_sticky_when_all_keys_per_model_cooled():
    """Every (key, m) blocked → fall back to sticky idx, log
    warning. Mirrors the whole-key all-cooled branch.
    """
    _setup_three_keys()
    for k in ("k0", "k1", "k2"):
        openrouter_keys.mark_key_rate_limited(k, model="hot/x")
    # All three blocked for "hot/x"; sticky pick wins.
    assert openrouter_keys.key_for_user(1, model="hot/x") == "k1"
    _reset_env()


def test_per_model_cooldown_isolates_same_key_different_models():
    """Two distinct (key, model) cooldowns on the SAME key for
    DIFFERENT models don't interact: clearing one leaves the
    other in place.
    """
    _setup_three_keys()
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=10, model="model/a",
    )
    openrouter_keys.mark_key_rate_limited(
        "k0", retry_after_secs=300, model="model/b",
    )
    # Force model/a to expire.
    openrouter_keys._per_model_cooldowns[("k0", "model/a")] = (
        openrouter_keys.time.monotonic() - 1.0
    )
    openrouter_keys._drop_expired_cooldowns()
    assert ("k0", "model/a") not in openrouter_keys._per_model_cooldowns
    assert ("k0", "model/b") in openrouter_keys._per_model_cooldowns
    _reset_env()


def test_normalise_model_returns_none_for_non_string():
    """``_normalise_model`` returns ``None`` for non-str inputs so
    a buggy caller that passes (e.g.) an int doesn't end up
    keying the cooldown table on a non-str tuple."""
    assert openrouter_keys._normalise_model(None) is None
    assert openrouter_keys._normalise_model(42) is None
    assert openrouter_keys._normalise_model(["a", "b"]) is None


def test_normalise_model_strips_whitespace():
    """Surrounding whitespace is stripped; otherwise non-empty
    inputs pass through unchanged."""
    assert openrouter_keys._normalise_model("  vendor/x  ") == "vendor/x"
    assert openrouter_keys._normalise_model("vendor/x") == "vendor/x"
    assert openrouter_keys._normalise_model("\tvendor/x\n") == "vendor/x"


def test_normalise_model_returns_none_for_blank():
    """All-whitespace inputs collapse to ``None`` — same as the
    falsy-fallback path in :func:`mark_key_rate_limited`."""
    assert openrouter_keys._normalise_model("") is None
    assert openrouter_keys._normalise_model("   ") is None
    assert openrouter_keys._normalise_model("\t\n  ") is None


# ---------------------------------------------------------------------
# Stage-15-Step-E #4 follow-up #5: one-shot retry outcome counters.
# ---------------------------------------------------------------------
# The aggregate (pool-wide, NOT per-key) counter family is bumped by
# ``ai_engine.chat_with_model`` after a 429 + retry attempt; this
# block pins the helper API the engine relies on plus the alphabet
# of valid outcome labels.
# ---------------------------------------------------------------------


def test_get_oneshot_retry_counters_starts_empty():
    """Fresh process / fresh test → no outcomes recorded."""
    openrouter_keys.reset_key_counters_for_tests()
    assert openrouter_keys.get_oneshot_retry_counters() == {}


def test_increment_oneshot_retry_records_each_outcome():
    """Each valid outcome label gets its own counter entry; repeated
    increments accumulate. The full alphabet (six labels) round-trips
    through the accessor."""
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys._increment_oneshot_retry("attempted")
    openrouter_keys._increment_oneshot_retry("attempted")
    openrouter_keys._increment_oneshot_retry("succeeded")
    openrouter_keys._increment_oneshot_retry("second_429")
    openrouter_keys._increment_oneshot_retry("second_other_status")
    openrouter_keys._increment_oneshot_retry("transport_error")
    openrouter_keys._increment_oneshot_retry("no_alternate_key")

    snapshot = openrouter_keys.get_oneshot_retry_counters()
    assert snapshot == {
        "attempted": 2,
        "succeeded": 1,
        "second_429": 1,
        "second_other_status": 1,
        "transport_error": 1,
        "no_alternate_key": 1,
    }


def test_increment_oneshot_retry_unknown_outcome_is_noop():
    """Defence in depth: a typoed outcome label must not be able to
    poison the counter table with a key outside the pinned alphabet
    (``_ONE_SHOT_RETRY_OUTCOMES``). Without this guard a future
    refactor that mistypes ``"succeded"`` would silently shift
    success counts into a phantom bucket the metrics layer would
    never render — making retry-success-rate plots wrong AND
    untraceable.
    """
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys._increment_oneshot_retry("typo_outcome")
    openrouter_keys._increment_oneshot_retry("")
    openrouter_keys._increment_oneshot_retry("SUCCEEDED")  # case-sensitive
    assert openrouter_keys.get_oneshot_retry_counters() == {}


def test_get_oneshot_retry_counters_returns_independent_copy():
    """Mutating the snapshot must not leak back into the registry —
    same defensive shallow-copy discipline ``get_key_429_counters``
    follows. Without this, a caller iterating the snapshot in a
    metrics path could accidentally clobber the live counter."""
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys._increment_oneshot_retry("succeeded")
    snapshot = openrouter_keys.get_oneshot_retry_counters()
    snapshot["succeeded"] = 999
    snapshot["new_label"] = 1
    # The live counters are untouched.
    assert openrouter_keys.get_oneshot_retry_counters() == {"succeeded": 1}


def test_reset_key_counters_for_tests_clears_oneshot_retry_counters():
    """Stage-15-Step-E #4 follow-up #5 added the one-shot-retry
    counter family — ensure ``reset_key_counters_for_tests`` (which
    every test uses to start clean) wipes it too. Without this,
    test isolation breaks: a counter set in test A leaks into test
    B's assertions."""
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys._increment_oneshot_retry("attempted")
    openrouter_keys._increment_oneshot_retry("succeeded")
    assert openrouter_keys.get_oneshot_retry_counters()
    openrouter_keys.reset_key_counters_for_tests()
    assert openrouter_keys.get_oneshot_retry_counters() == {}


def test_load_keys_resets_oneshot_retry_counters():
    """Hot reload (operator key rotation) must reset the
    one-shot-retry counters too — they're process-global aggregates
    and the new pool's retry behaviour is qualitatively different
    from the old pool's. Without this, a deploy that rotated a hot
    key carries forward stale ``second_429`` counts from the
    pre-rotation key, misleading the next operator shift's retry-
    success-rate plot.
    """
    _setup_three_keys()
    openrouter_keys.reset_key_counters_for_tests()
    openrouter_keys._increment_oneshot_retry("attempted")
    openrouter_keys._increment_oneshot_retry("succeeded")
    assert openrouter_keys.get_oneshot_retry_counters()

    os.environ.pop("OPENROUTER_API_KEY_1", None)
    os.environ.pop("OPENROUTER_API_KEY_2", None)
    os.environ.pop("OPENROUTER_API_KEY_3", None)
    os.environ["OPENROUTER_API_KEY_1"] = "new-k0"
    openrouter_keys.load_keys()
    assert openrouter_keys.get_oneshot_retry_counters() == {}
    _reset_env()
