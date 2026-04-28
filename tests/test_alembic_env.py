"""Tests for alembic/env.py URL construction.

Regression for the URL-encoding bug Devin Review flagged on PR #44:
DB_USER / DB_PASSWORD with characters meaningful in a URL (``@`` / ``:``
/ ``/`` / ``%`` / ``#`` / ``?``) used to corrupt the connection string
and crash-loop the bot container.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

ENV_PY_PATH = (
    Path(__file__).resolve().parent.parent / "alembic" / "env.py"
)


def _load_build_url(monkeypatch: pytest.MonkeyPatch):
    """Import the private _build_url helper from alembic/env.py.

    alembic/env.py runs migration code at import time (it calls
    ``context.is_offline_mode()`` etc.), so we can't naively import it.
    Stub the alembic + sqlalchemy entry points first, then load only the
    module's symbols.
    """
    fake_context = type(
        "FakeContext",
        (),
        {
            "config": type(
                "FakeConfig",
                (),
                {
                    "config_file_name": None,
                    "config_ini_section": "alembic",
                    "set_main_option": lambda *a, **k: None,
                    "get_main_option": lambda *a, **k: "",
                    "get_section": lambda *a, **k: {},
                },
            )(),
            "is_offline_mode": lambda: True,
            "configure": lambda *a, **k: None,
            "begin_transaction": lambda: _NullCM(),
            "run_migrations": lambda *a, **k: None,
        },
    )
    fake_alembic = type("FakeAlembic", (), {"context": fake_context})()
    monkeypatch.setitem(sys.modules, "alembic", fake_alembic)
    monkeypatch.setitem(sys.modules, "alembic.context", fake_context)

    # Loading the module will execute the offline branch (no DB needed).
    spec = importlib.util.spec_from_file_location(
        "alembic_env_under_test", ENV_PY_PATH
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod._build_url


class _NullCM:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _set_db_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    user: str,
    password: str,
    host: str = "localhost",
    port: str = "5432",
    name: str = "aibot_db",
):
    monkeypatch.setenv("DB_USER", user)
    monkeypatch.setenv("DB_PASSWORD", password)
    monkeypatch.setenv("DB_HOST", host)
    monkeypatch.setenv("DB_PORT", port)
    monkeypatch.setenv("DB_NAME", name)


def test_simple_credentials_url(monkeypatch):
    _set_db_env(monkeypatch, user="botuser", password="testpw")
    url = _load_build_url(monkeypatch)()
    assert url == "postgresql+psycopg2://botuser:testpw@localhost:5432/aibot_db"


@pytest.mark.parametrize(
    "raw_password,encoded",
    [
        ("p@ss", "p%40ss"),
        ("p:ss", "p%3Ass"),
        ("p/ss", "p%2Fss"),
        ("p%ss", "p%25ss"),
        ("p#ss", "p%23ss"),
        ("p?ss", "p%3Fss"),
        ("p ss", "p+ss"),  # quote_plus → space becomes +
        ("p&q=r", "p%26q%3Dr"),
    ],
)
def test_password_special_chars_are_encoded(monkeypatch, raw_password, encoded):
    """Each of these characters used to corrupt the URL pre-fix."""
    _set_db_env(monkeypatch, user="botuser", password=raw_password)
    url = _load_build_url(monkeypatch)()
    assert encoded in url, f"password {raw_password!r} not encoded as {encoded!r}"
    assert raw_password not in url.split("@")[0].split(":", 2)[2], (
        "raw password leaked into URL unencoded"
    )


def test_username_special_chars_are_encoded(monkeypatch):
    _set_db_env(monkeypatch, user="bot@user", password="testpw")
    url = _load_build_url(monkeypatch)()
    assert "bot%40user" in url


def test_host_port_dbname_not_encoded(monkeypatch):
    """Host / port / dbname stay raw — they are operator-supplied
    identifiers, not user-supplied secrets."""
    _set_db_env(
        monkeypatch,
        user="botuser",
        password="testpw",
        host="db.internal",
        port="6543",
        name="my_app_db",
    )
    url = _load_build_url(monkeypatch)()
    assert url.endswith("@db.internal:6543/my_app_db")


def test_defaults_when_env_unset(monkeypatch):
    for k in ("DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"):
        monkeypatch.delenv(k, raising=False)
    url = _load_build_url(monkeypatch)()
    assert url == "postgresql+psycopg2://botuser:@localhost:5432/aibot_db"
