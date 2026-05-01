"""Unit tests for the helpers exposed by ``tests/integration/conftest.py``.

The integration suite itself is opt-in (it runs only when the four
``TG_*`` env vars are set — see ``tests/integration/conftest.py``).
But the small env-var parsers / helpers it ships *can* be tested
without Telethon, and those tests gate the bundled bug fix on
Stage-15-Step-E #6 follow-up #1: ``_read_float_env`` previously let
``+inf`` through, which would deadlock the suite at
``await asyncio.sleep(inf)`` if an operator misconfigured
``TG_TEST_SETTLE_SECONDS``.
"""

from __future__ import annotations

import math
from importlib import import_module

import pytest

# Direct module import keeps the test independent of pytest's plugin-
# style "import the conftest" path. The ``tests.integration``
# package is registered in ``tests/integration/__init__.py``.
_conftest = import_module("tests.integration.conftest")
_read_float_env = _conftest._read_float_env
_read_int_env = _conftest._read_int_env


# ---------------------------------------------------------------------
# _read_int_env
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,default,expected",
    [
        # Empty / whitespace → default.
        ("", 15, 15),
        ("   ", 15, 15),
        # Non-numeric → default.
        ("abc", 15, 15),
        # Negative / zero → default (these would mean "no timeout"
        # which the helper actively rejects so a misconfig produces
        # the documented 15-second floor).
        ("0", 15, 15),
        ("-7", 15, 15),
        # Positive int → returned as-is.
        ("7", 15, 7),
        ("30", 15, 30),
        ("90", 15, 90),
        # Whitespace tolerated by the .strip() upstream.
        (" 30 ", 15, 30),
    ],
)
def test_read_int_env_parses_or_falls_back(monkeypatch, raw, default, expected):
    monkeypatch.setenv("INTEG_TEST_VAR", raw)
    assert _read_int_env("INTEG_TEST_VAR", default) == expected


def test_read_int_env_unset_returns_default(monkeypatch):
    monkeypatch.delenv("INTEG_TEST_VAR", raising=False)
    assert _read_int_env("INTEG_TEST_VAR", 42) == 42


# ---------------------------------------------------------------------
# _read_float_env — bundled bug-fix coverage
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,default,expected",
    [
        # Empty / unset → default.
        ("", 0.5, 0.5),
        ("   ", 0.5, 0.5),
        # Non-numeric → default.
        ("abc", 0.5, 0.5),
        ("--", 0.5, 0.5),
        # Negative → default.
        ("-1", 0.5, 0.5),
        ("-0.1", 0.5, 0.5),
        # Zero allowed (means "don't sleep").
        ("0", 0.5, 0.0),
        ("0.0", 0.5, 0.0),
        # Positive finite → returned as-is.
        ("0.25", 0.5, 0.25),
        ("1", 0.5, 1.0),
        ("1.5", 0.5, 1.5),
    ],
)
def test_read_float_env_finite_inputs(monkeypatch, raw, default, expected):
    monkeypatch.setenv("INTEG_TEST_FLOAT", raw)
    result = _read_float_env("INTEG_TEST_FLOAT", default)
    assert result == pytest.approx(expected)


@pytest.mark.parametrize(
    "raw",
    [
        # NaN — already rejected pre-fix (the old guard relied on
        # ``not (value >= 0.0)`` being True for NaN); pinned here
        # so a future refactor that loses that property fails loudly.
        "nan",
        "NaN",
        "NAN",
        # Positive infinity — the bundled bug-fix target. Pre-fix
        # ``inf >= 0.0`` was True so this slipped through, which then
        # deadlocked ``asyncio.sleep(inf)`` inside ``send_and_wait``.
        "inf",
        "INF",
        "Infinity",
        "+inf",
        # Negative infinity — also non-finite; rejected.
        "-inf",
        "-Infinity",
    ],
)
def test_read_float_env_rejects_non_finite(monkeypatch, raw):
    """Stage-15-Step-E #6 follow-up #1 bundled bug fix: every
    non-finite value (NaN / ±inf, in any case-insensitive spelling
    Python's ``float()`` accepts) MUST fall back to the default so
    the integration suite cannot wedge on
    ``await asyncio.sleep(inf)``."""
    monkeypatch.setenv("INTEG_TEST_FLOAT", raw)
    result = _read_float_env("INTEG_TEST_FLOAT", 0.5)
    assert result == 0.5
    # Belt-and-braces: the returned value MUST itself be finite.
    assert math.isfinite(result)


def test_read_float_env_default_must_be_finite_too():
    """Sanity: a caller passing ``inf`` as the default would defeat
    the guard at the call site (the default is returned untouched
    when the env var is missing). The fixture wires sane defaults
    so the integration suite never opts in to a pathological
    timeout, but pin it here as documentation."""
    # The helper itself doesn't validate the default — it only
    # validates the parsed input — but the live fixture passes 0.5,
    # so this is a guardrail for future contributors who might add
    # new env-var-backed knobs.
    from tests.integration.conftest import (
        integration_timeouts as _integration_timeouts,
    )
    # The fixture is async-friendly; we only inspect its source,
    # not call it.
    src = _integration_timeouts.__wrapped__.__code__.co_consts  # type: ignore[attr-defined]
    # Find every numeric literal hardcoded in the fixture body.
    finite_numerics = [c for c in src if isinstance(c, (int, float))]
    for c in finite_numerics:
        assert math.isfinite(c), (
            f"integration_timeouts default {c!r} is not finite — "
            "would deadlock the suite if it propagated"
        )
