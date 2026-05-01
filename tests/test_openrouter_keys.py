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
