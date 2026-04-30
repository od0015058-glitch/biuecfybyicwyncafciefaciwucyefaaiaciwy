"""Tests for openrouter_keys — multi-key load balancing.

Stage-14-Step-C.
"""

from __future__ import annotations

import os

import pytest

import openrouter_keys


def _reset_env(*keys_to_clear: str):
    """Remove all OPENROUTER_API_KEY* env vars for a clean test."""
    for k in list(os.environ):
        if k.startswith("OPENROUTER_API_KEY"):
            os.environ.pop(k, None)
    for k in keys_to_clear:
        os.environ.pop(k, None)
    openrouter_keys._keys = []


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
