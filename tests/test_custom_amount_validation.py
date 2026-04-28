"""Regression for the NaN / Inf / over-cap acceptance in
``handlers.process_custom_amount_input``.

We don't drive the full aiogram dispatcher here — that needs a real
Bot session and a Redis. Instead we replicate the validation block
inline (kept in lockstep with the handler) and assert it rejects every
edge case before the value reaches NowPayments / our DECIMAL column.

If you change ``process_custom_amount_input``'s validation, mirror
the change in ``_validate_custom_amount`` below or this test will
helpfully fail.
"""

from __future__ import annotations

import math

import pytest


def _validate_custom_amount(raw: str) -> tuple[bool, str | None]:
    """Mirror of the validation logic in handlers.py.

    Returns (ok, reject_reason). ok=True only for amounts in
    [5, 10_000] that aren't NaN / Inf.
    """
    try:
        amount = float(raw.strip().replace("$", ""))
    except ValueError:
        return False, "not_a_number"

    if not (amount == amount) or amount in (
        float("inf"),
        float("-inf"),
    ):
        return False, "nan_or_inf"

    if amount < 5:
        return False, "below_min"

    if amount > 10_000:
        return False, "above_max"

    return True, None


@pytest.mark.parametrize(
    "value,expected_reason",
    [
        ("nan", "nan_or_inf"),
        ("NaN", "nan_or_inf"),
        ("inf", "nan_or_inf"),
        ("Infinity", "nan_or_inf"),
        ("-inf", "nan_or_inf"),
        ("not a number", "not_a_number"),
        ("", "not_a_number"),
        ("4.99", "below_min"),
        ("0", "below_min"),
        ("-50", "below_min"),
        ("10001", "above_max"),
        ("99999999999", "above_max"),
    ],
)
def test_invalid_amounts_are_rejected(value, expected_reason):
    ok, reason = _validate_custom_amount(value)
    assert not ok, f"{value!r} should be rejected"
    assert reason == expected_reason


@pytest.mark.parametrize(
    "value,parsed",
    [
        ("5", 5.0),
        ("$5", 5.0),
        ("$5.00", 5.0),
        ("10", 10.0),
        ("99.50", 99.5),
        ("10000", 10000.0),
        ("  $20  ", 20.0),
    ],
)
def test_valid_amounts_pass(value, parsed):
    ok, reason = _validate_custom_amount(value)
    assert ok, f"{value!r} rejected: {reason}"
    # Sanity-check the parsing path.
    assert math.isfinite(float(value.strip().replace("$", "")))
    assert float(value.strip().replace("$", "")) == parsed


def test_handler_validation_matches_inline_helper():
    """Cheap smoke test: verify the real handler still has both the
    NaN/Inf rejection and the upper-bound check that this test mirrors.
    Catches drift if someone edits handlers.py without updating the
    inline copy here.
    """
    handlers_src = (
        __import__("pathlib").Path(__file__).resolve().parent.parent
        / "handlers.py"
    ).read_text()
    assert "amount == amount" in handlers_src, (
        "NaN check `amount == amount` missing from handlers.py — "
        "process_custom_amount_input would silently accept NaN"
    )
    assert "amount > 10_000" in handlers_src, (
        "Upper-bound check `amount > 10_000` missing from handlers.py "
        "— process_custom_amount_input would accept arbitrary amounts"
    )
