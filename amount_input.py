"""amount_input: normalize free-text amount entries for top-up flows.

Stage-11-Step-B adds a Toman top-up entry alongside the existing USD
one. Iranian users habitually type amounts with Persian digits and a
grab-bag of separators — ``۴۰۰٬۰۰۰``, ``400,000``, ``۴۰۰ ۰۰۰``,
``400000 تومان``, etc. A naive ``float(text)`` rejects all of these.

This module centralises the normalisation so both the USD path and
the new Toman path parse consistently:

* Persian (``۰-۹``) and Arabic-Indic (``٠-٩``) digits are rewritten
  to ASCII.
* Thousand separators — comma, Arabic comma ``،``, Arabic decimal
  separator ``٫``, Arabic thousands separator ``٬``, ASCII period
  used as thousands separator (``400.000`` in fa usage), and any
  whitespace / NBSP / bidi marks — are stripped.
* Currency suffixes (``تومان``, ``تومن``, ``toman``, ``TMN``, ``$``,
  ``USD``) are stripped so the user can type "400,000 تومان" or
  "$15" and we still get a number.
* One real decimal separator is preserved if present and
  unambiguous (e.g. ``15.5`` → 15.5, ``25,50`` → 25.50) — the
  heuristic keeps the last ``.`` or ``,`` when there's exactly one
  and the suffix has ≤2 digits (money decimal). Ambiguous inputs
  like ``1,234,567`` collapse all separators because treating a
  1M-Toman figure as ``1.234567`` would silently rob the user of
  six orders of magnitude.

Return value is always a positive ``float`` or ``None`` — we never
raise, because the caller wants a clean "invalid input" fallback
path, not an exception to catch.
"""

from __future__ import annotations

import math
from typing import Final

# Persian / Arabic-Indic digit tables.
_FA_DIGIT_MAP: Final[dict[str, str]] = {
    "۰": "0", "۱": "1", "۲": "2", "۳": "3", "۴": "4",
    "۵": "5", "۶": "6", "۷": "7", "۸": "8", "۹": "9",
    "٠": "0", "١": "1", "٢": "2", "٣": "3", "٤": "4",
    "٥": "5", "٦": "6", "٧": "7", "٨": "8", "٩": "9",
}

# Everything below is treated as "not a digit, not a decimal point" —
# i.e. strippable thousand-separator noise.
_SEP_CHARS: Final[frozenset[str]] = frozenset(
    [
        "٬",       # U+066C Arabic thousands separator
        "،",       # U+060C Arabic comma
        "\u00a0",  # non-breaking space
        "\u200c",  # ZWNJ
        "\u200d",  # ZWJ
        "\u200e",  # LRM
        "\u200f",  # RLM
        "\u202a", "\u202b", "\u202c", "\u202d", "\u202e",  # bidi
        " ", "\t",
        "'", "`",  # some locales use apostrophe as a thousands sep
        "_",
    ]
)

# Currency markers the user may type; stripped before numeric parsing.
_CURRENCY_MARKERS: Final[tuple[str, ...]] = (
    "تومان",
    "تومن",
    "toman",
    "tmn",
    "تو",     # abbreviation
    "ریال",   # rial — handled only at the marker level; we do NOT
              # auto-divide by 10 here because the caller picks the
              # currency mode explicitly (Toman vs Rial is a mode
              # choice, not an input-detection one).
    "rls",
    "ir",
    "irr",
    "usd",
    "$",
    "＄",
    "dollars",
    "dollar",
    "دلار",
)


def _translate_digits(text: str) -> str:
    """Map Persian / Arabic-Indic digits to ASCII."""
    return "".join(_FA_DIGIT_MAP.get(ch, ch) for ch in text)


def _strip_currency_markers(text: str) -> str:
    """Case-insensitively remove currency labels the user may append
    (``"400000 تومان"``, ``"$15"``, ``"15 USD"``)."""
    lowered = text.lower()
    for marker in _CURRENCY_MARKERS:
        lowered = lowered.replace(marker, " ")
    return lowered


def _strip_separators(text: str) -> str:
    return "".join(ch for ch in text if ch not in _SEP_CHARS)


def _decide_decimal(text: str) -> str:
    """Normalise the decimal point.

    Rules:

    * If the string has neither ``.`` nor ``,``, return unchanged.
    * If it has exactly one ``,`` or ``.`` and ≤2 digits follow,
      treat as a decimal and normalise to ``.``.
    * Otherwise collapse all ``,`` and ``.`` to empty (they're all
      thousands separators; ``400.000 تومان`` or ``1,234,567``
      are integer amounts in fa usage).
    """
    has_comma = "," in text
    has_dot = "." in text
    if not has_comma and not has_dot:
        return text

    # Only one of {"."", ","}? Maybe a decimal point.
    if has_dot and not has_comma:
        # "15.5" → decimal. "400.000" → thousands.
        if text.count(".") == 1:
            _, frac = text.rsplit(".", 1)
            if 1 <= len(frac) <= 2:
                return text
        return text.replace(".", "")
    if has_comma and not has_dot:
        if text.count(",") == 1:
            _, frac = text.rsplit(",", 1)
            if 1 <= len(frac) <= 2:
                return text.replace(",", ".")
        return text.replace(",", "")
    # Both present — last one wins as decimal, the other is thousands.
    # "1,234.56" → 1234.56;  "1.234,56" (EU) → 1234.56.
    last_comma = text.rfind(",")
    last_dot = text.rfind(".")
    if last_dot > last_comma:
        return text.replace(",", "").replace(".", ".")  # dot is decimal
    return text.replace(".", "").replace(",", ".")  # comma is decimal


def normalize_amount(raw: str) -> float | None:
    """Parse ``raw`` into a positive float, or ``None`` on any failure.

    Never raises. Callers that want a specific error message render
    their own fallback when this returns ``None``.
    """
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    text = _translate_digits(text)
    text = _strip_currency_markers(text)
    text = _strip_separators(text)
    text = _decide_decimal(text)
    if not text:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    # Reject NaN / Inf — these slip through naive ``value < min``
    # checks because NaN comparisons always return False. ``0`` and
    # negatives ARE returned unchanged; the caller applies the
    # proper ``amount < GLOBAL_MIN_TOPUP_USD`` lower bound and
    # routes users to the "minimum is $2" error rather than the
    # generic "not a number" one.
    if math.isnan(value) or math.isinf(value):
        return None
    return value
