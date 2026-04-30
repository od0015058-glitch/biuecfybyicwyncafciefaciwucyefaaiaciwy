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
    ``key_count`` call re-reads the env (Stage-15-Step-D #2).
    """
    for k in list(os.environ):
        if k.startswith("OPENROUTER_API_KEY"):
            os.environ.pop(k, None)
    for k in keys_to_clear:
        os.environ.pop(k, None)
    openrouter_keys._keys = []
    openrouter_keys._loaded = False


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
