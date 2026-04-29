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


from payments import GLOBAL_MIN_TOPUP_USD


def _validate_custom_amount(raw: str | None) -> tuple[bool, str | None]:
    """Mirror of the validation logic in handlers.py.

    Returns (ok, reject_reason). ok=True only for amounts in
    [GLOBAL_MIN_TOPUP_USD, 10_000] that aren't NaN / Inf. ``None``
    (e.g. when a user sends a sticker / photo while in
    waiting_custom_amount) is treated like an empty string and
    rejected as ``not_a_number``.
    """
    raw_text = (raw or "").strip()
    try:
        amount = float(raw_text.replace("$", ""))
    except ValueError:
        return False, "not_a_number"

    if not (amount == amount) or amount in (
        float("inf"),
        float("-inf"),
    ):
        return False, "nan_or_inf"

    if amount < GLOBAL_MIN_TOPUP_USD:
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
        # Stickers / photos / voice / video notes: aiogram delivers
        # ``message.text is None``. Must not crash; must reject.
        (None, "not_a_number"),
        # Below the $2 floor introduced alongside the per-currency
        # min-amount preflight. Pre-$2-floor the threshold was $5.
        ("1.99", "below_min"),
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
        # The $2 floor — smallest accepted amount.
        ("2", 2.0),
        ("$2", 2.0),
        # Between old $5 floor and new $2 floor: these USED to be
        # rejected as below_min but now pass. Explicit to prevent
        # accidental re-raising of the floor.
        ("2.50", 2.5),
        ("3", 3.0),
        ("4.99", 4.99),
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
    assert "amount < GLOBAL_MIN_TOPUP_USD" in handlers_src, (
        "Lower-bound check `amount < GLOBAL_MIN_TOPUP_USD` missing from "
        "handlers.py — the $2 floor would be bypassed"
    )
