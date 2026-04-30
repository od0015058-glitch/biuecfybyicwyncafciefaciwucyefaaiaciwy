"""In-memory cache for admin-disabled models and gateways.

Stage-14. The admin can disable individual AI models and payment
gateways from the web panel. The disabled sets are persisted in
``disabled_models`` / ``disabled_gateways`` tables (alembic 0015)
and cached in-process so the hot path (model picker render, chat
handler, currency picker) never issues an extra DB round-trip.

Public surface:

* :func:`load_disabled_models` / :func:`load_disabled_gateways` —
  called once at boot (from ``main.main``) to warm the cache.
* :func:`refresh_disabled_models` / :func:`refresh_disabled_gateways` —
  called after an admin toggle to re-sync the cache.
* :func:`is_model_disabled` / :func:`is_gateway_disabled` — fast,
  lock-free read from the in-memory set.
* :func:`get_disabled_models` / :func:`get_disabled_gateways` —
  return a snapshot copy for the admin UI.
"""

from __future__ import annotations

import logging

log = logging.getLogger("bot.admin_toggles")

_disabled_models: set[str] = set()
_disabled_gateways: set[str] = set()


async def load_disabled_models(db) -> None:
    """Warm the in-memory disabled-models cache from the DB."""
    global _disabled_models
    try:
        _disabled_models = await db.get_disabled_models()
        log.info("Loaded %d disabled model(s) from DB.", len(_disabled_models))
    except Exception:
        log.exception("Failed to load disabled models — cache stays empty.")
        _disabled_models = set()


async def load_disabled_gateways(db) -> None:
    """Warm the in-memory disabled-gateways cache from the DB."""
    global _disabled_gateways
    try:
        _disabled_gateways = await db.get_disabled_gateways()
        log.info("Loaded %d disabled gateway(s) from DB.", len(_disabled_gateways))
    except Exception:
        log.exception("Failed to load disabled gateways — cache stays empty.")
        _disabled_gateways = set()


async def refresh_disabled_models(db) -> None:
    """Re-sync the in-memory cache after an admin toggle.

    Fail-soft on a DB read error: log the exception and **preserve the
    previous cache** (do NOT clear it). The admin path that calls this
    already issued the canonical write to the ``disabled_models`` table,
    so the source of truth is correct — the cache will resync on the
    next successful refresh (or process restart). Clearing the cache
    on a transient SELECT blip would falsely re-enable every disabled
    model in the meantime, which is the opposite of fail-safe.

    Stage-15-Step-D #3-extension. Pre-fix, the bare ``await`` would
    propagate up to the aiohttp ``_models_toggle_post`` handler and
    return a 500 to the admin even though the toggle itself succeeded
    on the DB row — confusing the admin into clicking again.
    """
    global _disabled_models
    try:
        _disabled_models = await db.get_disabled_models()
    except Exception:
        log.exception(
            "refresh_disabled_models failed; preserving previous "
            "in-memory cache (size=%d) until next successful refresh.",
            len(_disabled_models),
        )


async def refresh_disabled_gateways(db) -> None:
    """Re-sync the in-memory cache after an admin toggle.

    See :func:`refresh_disabled_models` for the fail-soft rationale.
    """
    global _disabled_gateways
    try:
        _disabled_gateways = await db.get_disabled_gateways()
    except Exception:
        log.exception(
            "refresh_disabled_gateways failed; preserving previous "
            "in-memory cache (size=%d) until next successful refresh.",
            len(_disabled_gateways),
        )


def is_model_disabled(model_id: str) -> bool:
    """Fast check — no DB, no await."""
    return model_id in _disabled_models


def is_gateway_disabled(gateway_key: str) -> bool:
    """Fast check — no DB, no await."""
    return gateway_key in _disabled_gateways


def get_disabled_models() -> frozenset[str]:
    """Snapshot of the current disabled-models set (for admin UI)."""
    return frozenset(_disabled_models)


def get_disabled_gateways() -> frozenset[str]:
    """Snapshot of the current disabled-gateways set (for admin UI)."""
    return frozenset(_disabled_gateways)
