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
