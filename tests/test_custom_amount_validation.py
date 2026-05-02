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


from amount_input import normalize_amount
from payments import GLOBAL_MIN_TOPUP_USD


def _validate_custom_amount(raw: str | None) -> tuple[bool, str | None]:
    """Mirror of the validation logic in handlers.py.

    Returns (ok, reject_reason). ok=True only for amounts in
    [GLOBAL_MIN_TOPUP_USD, 10_000] that aren't NaN / Inf. ``None``
    (e.g. when a user sends a sticker / photo while in
    waiting_custom_amount) is treated like an empty string and
    rejected as ``not_a_number``.

    Stage-11-Step-B: handlers.py now delegates input-level rejection
    (unparseable text, NaN, Inf, ≤0) to ``amount_input.normalize_amount``;
    this mirror does the same so reject reasons stay aligned.
    """
    raw_text = (raw or "").strip() if isinstance(raw, str) else ""
    amount = normalize_amount(raw_text)
    if amount is None:
        # normalize_amount swallows NaN / Inf / unparseable /
        # negative / zero into one bucket. For the drift guard we
        # just need to confirm rejection — the reason is informative
        # only. Classify by a re-parse so downstream tests that
        # expect "nan_or_inf" vs "not_a_number" still bucket right.
        try:
            raw_float = float((raw or "").strip().replace("$", ""))
        except (ValueError, AttributeError, TypeError):
            return False, "not_a_number"
        if raw_float != raw_float or raw_float in (float("inf"), float("-inf")):
            return False, "nan_or_inf"
        return False, "not_a_number"

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
    """Cheap smoke test: verify the real handler still exercises
    ``normalize_amount`` (which owns the NaN/Inf/empty rejection) and
    both the $2 lower bound and the $10k upper bound.

    Catches drift if someone edits handlers.py without updating the
    inline mirror above.
    """
    handlers_src = (
        __import__("pathlib").Path(__file__).resolve().parent.parent
        / "handlers.py"
    ).read_text()
    assert "normalize_amount(raw_text)" in handlers_src, (
        "handlers.py no longer funnels custom-amount input through "
        "amount_input.normalize_amount — NaN/Inf/empty rejection is at risk"
    )
    assert "amount > 10_000" in handlers_src or "usd_amount > 10_000" in handlers_src, (
        "Upper-bound check missing from handlers.py — the USD path "
        "would accept arbitrary amounts"
    )
    # Stage-15-Step-E #10b row 4: the lower-bound check now routes
    # through ``get_min_topup_usd()`` so a runtime DB override
    # (system_settings.MIN_TOPUP_USD) can move the floor without a
    # redeploy. Accept either the legacy direct-constant spelling or
    # the new function-call spelling so this drift guard doesn't
    # whip-saw on the rename.
    assert (
        "amount < GLOBAL_MIN_TOPUP_USD" in handlers_src
        or "usd_amount < GLOBAL_MIN_TOPUP_USD" in handlers_src
        or "amount < get_min_topup_usd()" in handlers_src
        or "usd_amount < get_min_topup_usd()" in handlers_src
    ), (
        "Lower-bound check missing from handlers.py — the $2 floor "
        "would be bypassed"
    )
