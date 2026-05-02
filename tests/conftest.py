"""pytest fixtures.

Adds the repo root to ``sys.path`` so test modules can ``import payments``
without a package install. Kept tiny on purpose — heavier fixtures (live
postgres for db tests, etc.) get added when their tests are written.

Stage-15-Step-F follow-up #5: also imports every loop module so each
one's ``@register_loop`` call fires before any test inspects
``bot_health.LOOP_CADENCES`` or ``metrics._LOOP_METRIC_NAMES``. In
production these imports happen via ``main.py``; tests don't import
``main`` so the registrations would otherwise be skipped and any test
that asserts on the per-loop stale-threshold contract would see an
empty registry. The cost is one-time at collection — each module is
imported lazily by other tests anyway.
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Force every background-loop module to import so its
# ``@register_loop`` call populates ``bot_health.LOOP_CADENCES`` +
# ``metrics._LOOP_METRIC_NAMES``. Order doesn't matter — registrations
# are commutative and idempotent.
import bot_health_alert  # noqa: E402,F401
import fx_rates  # noqa: E402,F401
import model_discovery  # noqa: E402,F401
import models_catalog  # noqa: E402,F401
import payments  # noqa: E402,F401
import pending_alert  # noqa: E402,F401
import pending_expiration  # noqa: E402,F401
import zarinpal_backfill  # noqa: E402,F401

import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_admin_setting_overrides():
    """Reset every DB-backed system_settings override between tests.

    Stage-15-Step-E #10b ships a growing family of in-process override
    caches (``pricing._MARKUP_OVERRIDE``, ``payments._MIN_TOPUP_USD_OVERRIDE``,
    ``bot_health._THRESHOLD_OVERRIDES``…). Tests that exercise the admin
    write paths SET those caches; if a later test in the same session
    reads them, it inherits state it didn't ask for. The most expensive
    instance: ``test_zarinpal_telegram_fsm`` enters $4 in Toman, but a
    leftover MIN_TOPUP_USD override of e.g. $7.5 from a wallet-config
    test causes the keyboard render path to short-circuit (below floor).

    Cheap and idempotent — every reset is a no-op if no override is set,
    so adding this autouse globally costs nothing in tests that don't
    touch the override layer.
    """
    import pricing
    import payments as _payments
    pricing.clear_markup_override()
    _payments.clear_min_topup_override()
    yield
    pricing.clear_markup_override()
    _payments.clear_min_topup_override()
