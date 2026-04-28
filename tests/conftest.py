"""pytest fixtures.

Adds the repo root to ``sys.path`` so test modules can ``import payments``
without a package install. Kept tiny on purpose — heavier fixtures (live
postgres for db tests, etc.) get added when their tests are written.
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
