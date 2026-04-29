"""wallet_display: shared helpers for rendering wallet balances.

Stage-11-Step-D. The wallet is denominated in USD (an explicit
invariant — the balance must not lose purchasing power if the rial
swings). Iranian users still want to *see* their balance in the
currency they think in, so every wallet surface (the hub view, the
back-to-wallet button after charge, the post-credit notification)
augments the USD figure with a ``≈ N تومان`` annotation when an FX
snapshot is available.

This module owns the formatting rules so the call sites stay tiny:

* :func:`format_toman_annotation` returns the ``"\\n≈ N TMN"`` /
  ``"\\n≈ N تومان (نرخ تقریبی)"`` (or English-locale equivalent) line
  to splice into the ``wallet_text`` template, or ``""`` when no
  rate is available.

* :func:`format_balance_block` packages the USD figure + the Toman
  annotation into a single ready-to-render string for callers that
  don't go through ``strings.t``. Used by Stage-11-Step-D follow-on
  surfaces (post-credit notification copy) that want the same shape
  without re-templating ``wallet_text``.

The rendering rules:

* ``snap is None`` → no annotation. We fail soft rather than render
  ``≈ 0 TMN``: a missing rate means the whole concept of "Toman
  equivalent" is unknowable, not that it's zero.
* ``snap.is_stale()`` → annotation is present but suffixed with the
  ``approx`` marker. The user sees that the figure is informational,
  not a quote.
* ``balance_usd <= 0`` → still render ``≈ 0 تومان`` so a fresh
  account doesn't look broken — the rate is known, the conversion
  is well-defined, the result is just zero.
* ``balance_usd`` non-finite (NaN / ±Inf) → no annotation. Defense in
  depth: if a future code path passes a corrupted balance we render
  USD only and skip the Toman line rather than printing ``≈ nan``.

The Toman figure is always rendered as a thousands-separated integer
(``≈ 412,500 TMN``). Iranian users habitually write Toman without
fractional digits; sub-toman precision adds noise without clarifying
anything.
"""

from __future__ import annotations

import math

from fx_rates import FxRateSnapshot
from strings import t


def format_toman_annotation(
    lang: str | None,
    balance_usd: float,
    snap: FxRateSnapshot | None,
) -> str:
    """Return the ``\\n≈ N TMN`` annotation line to splice into a
    wallet-display string, or ``""`` when no annotation is warranted.

    Always returns a leading newline when non-empty so the caller can
    splice it directly after the USD figure (``f"${b:.2f}{ann}"``)
    without conditionally emitting a separator. Empty string when
    ``snap is None`` or ``balance_usd`` is non-finite.

    The ``approx`` marker is keyed off :meth:`FxRateSnapshot.is_stale`
    (default: 4× the refresh interval, i.e. 40 minutes at 10-min
    cadence). A stale rate is still shown — better than no number —
    but the user sees that it's informational.
    """
    if snap is None:
        return ""
    if not isinstance(balance_usd, (int, float)):
        return ""
    if not math.isfinite(balance_usd):
        return ""
    toman_value = balance_usd * snap.toman_per_usd
    if not math.isfinite(toman_value):
        # Defense in depth: a finite balance times a finite rate
        # could in principle overflow at extreme magnitudes.
        return ""
    if snap.is_stale():
        key = "wallet_toman_line_stale"
    else:
        key = "wallet_toman_line"
    rendered = t(lang, key, toman=toman_value)
    if not rendered:
        return ""
    return "\n" + rendered


def format_balance_block(
    lang: str | None,
    balance_usd: float,
    snap: FxRateSnapshot | None,
) -> str:
    """Format ``$X.YZ`` plus the optional Toman annotation as a single
    ready-to-render block. Mirrors what the ``wallet_text`` template
    produces but without re-templating, for callers (post-credit DMs,
    upcoming wallet sub-screens) that don't go through ``strings.t``.
    """
    head = f"${balance_usd:.2f}"
    return head + format_toman_annotation(lang, balance_usd, snap)


__all__ = [
    "format_balance_block",
    "format_toman_annotation",
]
