"""Tests for the NaN / Infinity guard in ``payments._compute_actually_paid_usd``
and the ``finished``-IPN ``price_amount`` extraction.

Pre-fix bug
-----------
Both code paths used the ``float(x) <= 0`` idiom to validate user-supplied
amounts coming off the wire. Every comparison against ``NaN`` returns
``False`` (IEEE-754 semantics), so a payload like
``{"actually_paid": "NaN", "pay_amount": 1, "price_amount": 100}`` slipped
straight past the guard and got passed to ``finalize_partial_payment`` /
``finalize_payment`` as the credit amount. PostgreSQL accepts
``'NaN'::numeric`` so the INSERT didn't fail, but every subsequent
balance comparison against the user's wallet (e.g. ``deduct_balance``'s
``WHERE balance_usd >= $1 RETURNING ...``) is then a silent no-op
because ``NaN >= anything`` is ``False`` — effectively bricking the
wallet without an obvious error in logs.

The fix introduces a single ``_finite_positive_float`` helper that uses
``math.isfinite`` (which rejects ``NaN``, ``+inf``, and ``-inf`` in one
call) and is reused by both the partial-payment computation and the
``finished``-path ``price_amount`` parse.
"""

from __future__ import annotations

import math

import payments


# ---------------------------------------------------------------------
# _finite_positive_float (the new helper, exercised directly)
# ---------------------------------------------------------------------


def test_finite_positive_float_accepts_normal_value():
    assert payments._finite_positive_float(5.0) == 5.0
    assert payments._finite_positive_float("5.0") == 5.0
    assert payments._finite_positive_float(1) == 1.0


def test_finite_positive_float_rejects_nan():
    assert payments._finite_positive_float(float("nan")) is None
    # Wire formats are usually JSON strings -> Python ``float("NaN")``
    assert payments._finite_positive_float("NaN") is None
    assert payments._finite_positive_float("nan") is None


def test_finite_positive_float_rejects_infinity():
    assert payments._finite_positive_float(float("inf")) is None
    assert payments._finite_positive_float(float("-inf")) is None
    assert payments._finite_positive_float("Infinity") is None
    assert payments._finite_positive_float("-Infinity") is None


def test_finite_positive_float_rejects_zero_and_negative():
    assert payments._finite_positive_float(0) is None
    assert payments._finite_positive_float(0.0) is None
    assert payments._finite_positive_float(-1) is None
    assert payments._finite_positive_float("-3.14") is None


def test_finite_positive_float_rejects_unparseable():
    assert payments._finite_positive_float(None) is None
    assert payments._finite_positive_float("") is None
    assert payments._finite_positive_float("not a number") is None
    assert payments._finite_positive_float([]) is None


# ---------------------------------------------------------------------
# _compute_actually_paid_usd (the primary fix site)
# ---------------------------------------------------------------------


def test_compute_actually_paid_usd_happy_path():
    """Sanity: the regular partial-payment math still works."""
    out = payments._compute_actually_paid_usd({
        "actually_paid": "0.5",
        "pay_amount": "1.0",
        "price_amount": "100.0",
    })
    assert out is not None
    # 0.5 / 1.0 * 100.0 == 50.0
    assert math.isclose(out, 50.0)


def test_compute_actually_paid_usd_caps_at_price_amount():
    """Existing defense-in-depth: an over-payment is capped at price_amount.
    Pinned here so the NaN-guard refactor doesn't silently regress it."""
    out = payments._compute_actually_paid_usd({
        # 2 BTC paid against a 1 BTC = $100 invoice -> raw math gives $200,
        # but we cap at the original $100 price so the user can't intentionally
        # over-pay to drain margin.
        "actually_paid": "2.0",
        "pay_amount": "1.0",
        "price_amount": "100.0",
    })
    assert out == 100.0


def test_compute_actually_paid_usd_rejects_nan_actually_paid():
    """The original wallet-bricker repro: NaN ``actually_paid`` slipped
    past ``<= 0`` because ``nan <= 0`` is ``False``."""
    out = payments._compute_actually_paid_usd({
        "actually_paid": "NaN",
        "pay_amount": "1.0",
        "price_amount": "100.0",
    })
    assert out is None


def test_compute_actually_paid_usd_rejects_nan_pay_amount():
    out = payments._compute_actually_paid_usd({
        "actually_paid": "0.5",
        "pay_amount": "NaN",
        "price_amount": "100.0",
    })
    assert out is None


def test_compute_actually_paid_usd_rejects_nan_price_amount():
    out = payments._compute_actually_paid_usd({
        "actually_paid": "0.5",
        "pay_amount": "1.0",
        "price_amount": "NaN",
    })
    assert out is None


def test_compute_actually_paid_usd_rejects_inf_actually_paid():
    out = payments._compute_actually_paid_usd({
        "actually_paid": "Infinity",
        "pay_amount": "1.0",
        "price_amount": "100.0",
    })
    assert out is None


def test_compute_actually_paid_usd_rejects_inf_pay_amount():
    out = payments._compute_actually_paid_usd({
        "actually_paid": "0.5",
        "pay_amount": "Infinity",
        "price_amount": "100.0",
    })
    assert out is None


def test_compute_actually_paid_usd_rejects_zero_pay_amount():
    """Pre-existing guard, pinned to make sure the refactor preserved it.
    A zero ``pay_amount`` would also be a ZeroDivisionError in the math."""
    out = payments._compute_actually_paid_usd({
        "actually_paid": "0.5",
        "pay_amount": "0",
        "price_amount": "100.0",
    })
    assert out is None


def test_compute_actually_paid_usd_rejects_missing_field():
    """Pre-existing guard, pinned: missing ``actually_paid`` returns None."""
    out = payments._compute_actually_paid_usd({
        "pay_amount": "1.0",
        "price_amount": "100.0",
    })
    assert out is None


def test_compute_actually_paid_usd_rejects_unparseable():
    out = payments._compute_actually_paid_usd({
        "actually_paid": "not a number",
        "pay_amount": "1.0",
        "price_amount": "100.0",
    })
    assert out is None
