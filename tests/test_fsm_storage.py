"""Tests for main.build_fsm_storage().

We don't actually connect to Redis here — RedisStorage.from_url() is
lazy (it doesn't open a connection until something hits it), so the
test just confirms which class the factory hands back based on
REDIS_URL.
"""

from __future__ import annotations

import importlib
import logging
import sys

import pytest


@pytest.fixture
def reload_main(monkeypatch):
    """Yield a fresh copy of ``main`` with .env disabled.

    main.py calls ``load_dotenv()`` at import time. We don't want a
    developer's local .env to leak REDIS_URL into the test.
    """
    monkeypatch.delenv("REDIS_URL", raising=False)
    # Stub load_dotenv so it can't override our monkeypatched env.
    fake_dotenv = type(
        "FakeDotenv", (), {"load_dotenv": lambda *a, **k: False}
    )()
    monkeypatch.setitem(sys.modules, "dotenv", fake_dotenv)
    sys.modules.pop("main", None)
    main = importlib.import_module("main")
    yield main
    sys.modules.pop("main", None)


def test_no_redis_url_falls_back_to_memory(reload_main, caplog):
    from aiogram.fsm.storage.memory import MemoryStorage

    caplog.set_level(logging.WARNING, logger="bot.main")
    storage = reload_main.build_fsm_storage()
    assert isinstance(storage, MemoryStorage)
    assert any(
        "REDIS_URL is not set" in r.message for r in caplog.records
    ), "expected a WARNING log when falling back"


def test_blank_redis_url_also_falls_back(reload_main, monkeypatch):
    from aiogram.fsm.storage.memory import MemoryStorage

    monkeypatch.setenv("REDIS_URL", "   ")  # whitespace-only
    storage = reload_main.build_fsm_storage()
    assert isinstance(storage, MemoryStorage)


def test_redis_url_returns_redis_storage(reload_main, monkeypatch, caplog):
    from aiogram.fsm.storage.redis import RedisStorage

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    caplog.set_level(logging.INFO, logger="bot.main")
    storage = reload_main.build_fsm_storage()
    assert isinstance(storage, RedisStorage)
    assert any(
        "Using Redis FSM storage" in r.message for r in caplog.records
    )
