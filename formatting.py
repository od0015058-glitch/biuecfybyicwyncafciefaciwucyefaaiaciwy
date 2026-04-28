"""Stage-9-Step-7 bug-fix bundle: a single canonical USD formatter.

Pre-fix, the admin UI was sprinkled with ad-hoc dollar formatters:

* ``"${:,.4f}".format(amount_usd)`` — transactions browser, user
  detail page (4 decimal places, comma-grouped)
* ``"${:,.2f}".format(...)`` — gift codes list, promo codes list
  (2 decimal places, comma-grouped)
* ``f"${value:.4f}"`` — Telegram-side ``/admin_balance`` and
  refusal messages (4 decimal places, NO comma grouping)
* ``f"${value:.2f}"`` — discount/gift validation error templates
  (2 decimal places, NO comma grouping)

The inconsistency mattered: an admin reading both
``/admin/transactions`` and ``/admin_balance`` for the same row
would see the same number rendered as ``$1,234.5678`` in one
place and ``$1234.5678`` in the other, then ``$1,234.57`` in the
gift codes UI. For a quarterly audit reconciliation this kind of
drift forces the auditor to constantly second-guess whether two
numbers actually match.

This module exposes a single :func:`format_usd` and a Jinja2
filter wired in :func:`web_admin.setup_admin_routes`. Default is
**4 decimal places** because individual API calls in this bot
cost on the order of $0.0001-$0.001 — anything coarser hides
genuine ledger movements.
"""

from __future__ import annotations


def format_usd(value: float | int, places: int = 4) -> str:
    """Format *value* as a USD string with comma-grouped thousands.

    >>> format_usd(0)
    '$0.0000'
    >>> format_usd(1234.5)
    '$1,234.5000'
    >>> format_usd(1234.5, places=2)
    '$1,234.50'
    >>> format_usd(-7.89)
    '-$7.8900'
    >>> format_usd(0.00001, places=4)
    '$0.0000'

    The minus sign is placed *before* the dollar sign, matching the
    convention used in the Stage-8 admin UI and most accounting
    software (``-$1,234.56``, NOT ``$-1,234.56``). ``places`` is
    clamped to ``[0, 8]`` so a stray ``places=99`` from a
    misconfigured caller can't blow Python's float-repr stack.
    """
    places = max(0, min(int(places), 8))
    if value < 0:
        return f"-${-float(value):,.{places}f}"
    return f"${float(value):,.{places}f}"
