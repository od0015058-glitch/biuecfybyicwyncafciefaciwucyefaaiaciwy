"""Live catalog of OpenRouter models, with a 24h in-memory cache.

OpenRouter exposes the full model list at ``GET /api/v1/models`` with
no auth required. Each entry has an ``id`` (e.g. ``"openai/gpt-4o"``),
a ``name``, and per-token ``pricing`` dict. We:

* fetch on demand the first time anyone asks,
* refresh after :data:`CATALOG_TTL_SECONDS` (24h by default),
* fall back to the static :mod:`pricing` table if the fetch fails so the
  bot is still usable when OpenRouter is down,
* group models by their ``id.split("/")[0]`` provider prefix so the UI
  can render a two-step picker (provider → model).

Public surface:

* :func:`get_catalog` — async accessor, returns a :class:`Catalog`.
* :func:`get_model_price` — async accessor, returns
  :class:`pricing.ModelPrice` for a given model id; falls back to the
  static table or :data:`pricing.FALLBACK_PRICE`.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

import aiohttp

from pricing import FALLBACK_PRICE, MODEL_PRICES, ModelPrice

log = logging.getLogger("bot.models_catalog")

OPENROUTER_MODELS_URL = "https://openrouter.ai/api/v1/models"
CATALOG_TTL_SECONDS = 24 * 60 * 60
FETCH_TIMEOUT_SECONDS = 15


@dataclass(frozen=True)
class CatalogModel:
    """One entry in the catalog."""

    id: str
    name: str
    provider: str  # `id.split("/")[0]`
    price: ModelPrice


@dataclass
class Catalog:
    """Snapshot of the OpenRouter catalog."""

    models: tuple[CatalogModel, ...] = field(default_factory=tuple)
    by_provider: dict[str, tuple[CatalogModel, ...]] = field(default_factory=dict)
    fetched_at: float = 0.0  # epoch seconds; 0 means never fetched
    is_fallback: bool = False  # True if we couldn't reach OpenRouter

    def get(self, model_id: str) -> CatalogModel | None:
        for m in self.models:
            if m.id == model_id:
                return m
        return None


# Module-level singleton state. The lock protects against multiple
# concurrent refresh attempts when many users tap Models at once.
_catalog: Catalog = Catalog()
_lock = asyncio.Lock()


def _build_fallback_catalog() -> Catalog:
    """Build a Catalog from the static MODEL_PRICES table.

    Used at boot if OpenRouter is unreachable. The display name is
    derived from the id by stripping the provider prefix and
    title-casing the remainder.
    """
    models: list[CatalogModel] = []
    for model_id, price in MODEL_PRICES.items():
        provider, _, slug = model_id.partition("/")
        name = slug.replace("-", " ").title() if slug else model_id
        models.append(
            CatalogModel(id=model_id, name=name, provider=provider, price=price)
        )
    return _finalize_catalog(models, is_fallback=True)


def _finalize_catalog(
    models: list[CatalogModel], *, is_fallback: bool
) -> Catalog:
    """Sort and group models by provider; return an immutable Catalog."""
    models.sort(key=lambda m: (m.provider, m.id))
    by_provider: dict[str, list[CatalogModel]] = {}
    for m in models:
        by_provider.setdefault(m.provider, []).append(m)
    return Catalog(
        models=tuple(models),
        by_provider={p: tuple(ms) for p, ms in by_provider.items()},
        fetched_at=time.time(),
        is_fallback=is_fallback,
    )


def _parse_price(raw: object) -> float | None:
    """OpenRouter returns prices as strings of USD-per-token. Parse safely.

    Returns ``None`` when the field is missing (``raw is None``) or
    malformed (non-numeric). The caller uses ``None`` as a signal that
    we don't actually know this model's price, distinct from
    legitimately free models whose price is the explicit value
    ``0`` / ``"0"``. Conflating the two leads to silently charging
    paid models at $0 (when OpenRouter returns malformed pricing) or
    silently charging free models at FALLBACK_PRICE (when we treat
    explicit zero as missing).
    """
    if raw is None:
        return None
    try:
        return float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


async def _fetch_from_openrouter() -> Catalog:
    """Fetch the live model list. Returns a Catalog or raises."""
    timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(OPENROUTER_MODELS_URL) as response:
            response.raise_for_status()
            payload = await response.json()

    raw_models = payload.get("data") or []
    models: list[CatalogModel] = []
    skipped_no_pricing = 0
    for entry in raw_models:
        model_id = entry.get("id")
        if not isinstance(model_id, str) or "/" not in model_id:
            continue
        provider = model_id.split("/", 1)[0]
        name = entry.get("name") or model_id
        pricing_dict = entry.get("pricing") or {}
        # OpenRouter pricing fields are USD per token; convert to per-1M
        # to match our internal ModelPrice format.
        prompt_per_token = _parse_price(pricing_dict.get("prompt"))
        completion_per_token = _parse_price(pricing_dict.get("completion"))
        if prompt_per_token is None or completion_per_token is None:
            # Pricing data missing or malformed for this model. Drop it
            # from the catalog so the picker doesn't display it (we'd
            # have nothing trustworthy to show as the price), and so
            # get_model_price falls through to MODEL_PRICES /
            # FALLBACK_PRICE for any user already on this id. Silently
            # charging $0 here would let the platform pay OpenRouter
            # for an API call the user never paid for.
            skipped_no_pricing += 1
            continue
        price = ModelPrice(
            input_per_1m_usd=prompt_per_token * 1_000_000.0,
            output_per_1m_usd=completion_per_token * 1_000_000.0,
        )
        models.append(
            CatalogModel(id=model_id, name=str(name), provider=provider, price=price)
        )

    if skipped_no_pricing:
        log.info(
            "Dropped %d OpenRouter model(s) from catalog due to missing/malformed pricing",
            skipped_no_pricing,
        )

    if not models:
        # Empty response → keep the fallback rather than caching emptiness.
        raise RuntimeError("OpenRouter returned no models")

    return _finalize_catalog(models, is_fallback=False)


async def _refresh_if_stale() -> Catalog:
    """Refresh the catalog if it's empty or older than the TTL."""
    global _catalog
    async with _lock:
        now = time.time()
        if _catalog.models and (now - _catalog.fetched_at) < CATALOG_TTL_SECONDS:
            return _catalog
        try:
            _catalog = await _fetch_from_openrouter()
            log.info(
                "Refreshed OpenRouter catalog: %d models across %d providers",
                len(_catalog.models),
                len(_catalog.by_provider),
            )
        except Exception:
            log.exception("OpenRouter /models fetch failed; using static fallback")
            # Only fall back if we have *no* catalog at all. If we have a
            # stale-but-real one, keep serving it rather than downgrading
            # the UX to the small static list.
            if not _catalog.models:
                _catalog = _build_fallback_catalog()
        return _catalog


async def get_catalog() -> Catalog:
    """Public accessor. Refreshes the catalog if it has gone stale."""
    return await _refresh_if_stale()


async def get_model_price(model_id: str) -> ModelPrice:
    """Resolve a model's price using the catalog, then static table, then fallback.

    If the model is present in the live catalog, return its price
    unconditionally — including legitimately-free models (e.g.
    ``meta-llama/llama-3.2-1b-instruct:free``) where both
    ``input_per_1m_usd`` and ``output_per_1m_usd`` are 0. The earlier
    ``> 0`` guard mistook those for missing data and fell through to
    :data:`FALLBACK_PRICE` (~$10/$30 per 1M × markup), so a user who
    picked a "$0.00 / $0.00" model from the picker would actually be
    charged the most expensive rate. The picker shows whatever the
    catalog reports, and OpenRouter is the source of truth — pricing
    must agree.

    The static :data:`MODEL_PRICES` and :data:`FALLBACK_PRICE` only
    come into play when the model id is not in the catalog at all
    (catalog fetch failed and the static table is being used as a
    fallback list, or the user is somehow on a model id the catalog
    has since dropped).
    """
    catalog = await get_catalog()
    entry = catalog.get(model_id)
    if entry is not None:
        return entry.price
    if model_id in MODEL_PRICES:
        return MODEL_PRICES[model_id]
    return FALLBACK_PRICE
