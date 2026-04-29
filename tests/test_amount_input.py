"""Tests for :mod:`amount_input` — top-up free-text amount parsing.

Stage-11-Step-B. Exercise the realistic inputs an Iranian user would
actually type into the bot: Persian digits, mixed separators,
trailing currency markers, etc. A parser regression here means a
user typing ``۴۰۰٬۰۰۰ تومان`` gets "invalid amount" and gives up on
topping up — we want to catch that in CI, not in support tickets.
"""

from __future__ import annotations

import pytest

from amount_input import normalize_amount


# ---------------------------------------------------------------------
# Happy path: plain ASCII, already-clean inputs
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("15", 15.0),
        ("15.5", 15.5),
        ("0.5", 0.5),
        ("400000", 400_000.0),
        ("$15", 15.0),
        ("15 USD", 15.0),
        ("15 dollars", 15.0),
        ("  25.75  ", 25.75),
    ],
)
def test_ascii_happy_path(raw, expected):
    assert normalize_amount(raw) == pytest.approx(expected)


# ---------------------------------------------------------------------
# Persian digit conversion
# ---------------------------------------------------------------------


def test_persian_digits_alone():
    assert normalize_amount("۴۰۰۰۰۰") == 400_000.0


def test_arabic_indic_digits():
    assert normalize_amount("٤٠٠٠٠٠") == 400_000.0


def test_mixed_persian_and_ascii_digits():
    assert normalize_amount("۴00000") == 400_000.0


# ---------------------------------------------------------------------
# Thousand separators (Arabic thousands U+066C, Arabic comma, commas,
# dots, spaces, NBSP, bidi marks)
# ---------------------------------------------------------------------


def test_arabic_thousands_separator():
    # ۴۰۰٬۰۰۰ — the Persian/Arabic thousands separator U+066C
    assert normalize_amount("۴۰۰٬۰۰۰") == 400_000.0


def test_arabic_comma_as_thousands():
    assert normalize_amount("۴۰۰،۰۰۰") == 400_000.0


def test_ascii_comma_thousands_multiple_groups():
    # Three-digit groups, treated as thousands separators — NOT as
    # decimal. Regression guard: must not produce 1.234567.
    assert normalize_amount("1,234,567") == 1_234_567.0


def test_european_dot_thousands():
    # "400.000" in fa usage is an integer 400 000, not 400.0.
    # Heuristic: more than 2 digits after the last "." => thousands.
    assert normalize_amount("400.000") == 400_000.0


def test_space_thousands_separator():
    assert normalize_amount("۴۰۰ ۰۰۰") == 400_000.0


def test_nbsp_thousands_separator():
    assert normalize_amount("400\u00a0000") == 400_000.0


def test_bidi_mark_stripped():
    # A leading RTL mark from a copy-paste should not break parsing.
    assert normalize_amount("\u200f۴۰۰۰۰۰") == 400_000.0


# ---------------------------------------------------------------------
# Decimal-point detection (one decimal separator + ≤2 digits)
# ---------------------------------------------------------------------


def test_comma_as_decimal_eu_style():
    assert normalize_amount("15,50") == 15.50


def test_dot_as_decimal_usd_style():
    assert normalize_amount("25.75") == 25.75


def test_european_format_thousands_and_decimal():
    # 1.234,56 (EU) — dot is thousands, comma is decimal.
    assert normalize_amount("1.234,56") == pytest.approx(1234.56)


def test_us_format_thousands_and_decimal():
    # 1,234.56 (US) — comma is thousands, dot is decimal.
    assert normalize_amount("1,234.56") == pytest.approx(1234.56)


# ---------------------------------------------------------------------
# Currency suffix stripping
# ---------------------------------------------------------------------


def test_toman_suffix():
    assert normalize_amount("400000 تومان") == 400_000.0


def test_tomn_short_suffix():
    assert normalize_amount("400000تومن") == 400_000.0


def test_toman_latin_suffix():
    assert normalize_amount("400000 toman") == 400_000.0


def test_dollar_prefix():
    assert normalize_amount("$15.5") == 15.5


def test_full_persian_with_suffix():
    assert normalize_amount("۴۰۰٬۰۰۰ تومان") == 400_000.0


# ---------------------------------------------------------------------
# Rejection: non-numeric / empty / NaN / inf / negative / zero
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        "abc",
        "$$$",
        "تومان",         # only currency marker
        None,            # not a string
        "nan",
        "NaN",
        "inf",
        "Infinity",
        "-inf",
    ],
)
def test_rejects_invalid_inputs(raw):
    assert normalize_amount(raw) is None


@pytest.mark.parametrize(
    "raw, expected",
    [
        # Zero and negatives parse as-is — the caller applies the
        # lower bound (``amount < GLOBAL_MIN_TOPUP_USD``) and renders
        # the "minimum is $2" error rather than "invalid number".
        ("0", 0.0),
        ("0.0", 0.0),
        ("-15", -15.0),
        ("-0.5", -0.5),
    ],
)
def test_passes_through_zero_and_negative_for_downstream_bound_check(raw, expected):
    assert normalize_amount(raw) == pytest.approx(expected)


def test_handles_huge_but_plausible_toman_amounts():
    # 50 000 000 TMN ≈ $500 at current rates — perfectly realistic.
    assert normalize_amount("۵۰٬۰۰۰٬۰۰۰ تومان") == 50_000_000.0
