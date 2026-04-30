"""Per-model pricing for OpenRouter calls.

All prices are USD per 1,000,000 tokens, taken from OpenRouter's public model
list. Update this table when prices change or when new models are enabled in
the bot. Prices are intentionally rounded up slightly to be conservative.

The final cost charged to the user is:

    (prompt_tokens * input_price + completion_tokens * output_price) / 1_000_000
        * MARKUP

MARKUP is read from the ``COST_MARKUP`` env var (default 1.5x).
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass

log = logging.getLogger("bot.pricing")


@dataclass(frozen=True)
class ModelPrice:
    """Price per 1M tokens for a single model."""

    input_per_1m_usd: float
    output_per_1m_usd: float


# Conservative fallback used when a model is not in the table. Picked to be
# expensive enough that an unmapped model can never silently lose money;
# operators should explicitly add new models to the table.
FALLBACK_PRICE = ModelPrice(input_per_1m_usd=10.0, output_per_1m_usd=30.0)


# Keep keys in sync with OpenRouter's model identifiers.
MODEL_PRICES: dict[str, ModelPrice] = {
    "openai/gpt-3.5-turbo": ModelPrice(0.50, 1.50),
    "openai/gpt-4o": ModelPrice(2.50, 10.00),
    "openai/gpt-4o-mini": ModelPrice(0.15, 0.60),
    "anthropic/claude-3.5-sonnet": ModelPrice(3.00, 15.00),
    "anthropic/claude-3-haiku": ModelPrice(0.25, 1.25),
    "google/gemini-pro-1.5": ModelPrice(1.25, 5.00),
    "google/gemini-flash-1.5": ModelPrice(0.075, 0.30),
    "meta-llama/llama-3.1-70b-instruct": ModelPrice(0.52, 0.75),
    "meta-llama/llama-3.1-8b-instruct": ModelPrice(0.055, 0.055),
    "mistralai/mistral-large": ModelPrice(2.00, 6.00),
}


def get_markup() -> float:
    """Return the cost markup multiplier from env, default 1.5x.

    Rejects ``NaN`` / ``±Infinity`` explicitly. ``float()`` accepts the
    strings ``"nan"`` and ``"inf"`` (case-insensitive) so a typo or a
    deliberately malicious ``COST_MARKUP=nan`` would otherwise slip
    past the parse step. A NaN markup propagates through
    :func:`_apply_markup` (every IEEE-754 op against NaN returns NaN)
    and ultimately reaches ``database.deduct_balance``; the
    ``_is_finite_amount`` guard there refuses the SQL but the user
    still gets a free OpenRouter reply because ``log_usage`` records
    ``cost=0``. Reject upstream so paid models stay paid even with a
    deploy-time misconfiguration.
    """
    raw = os.getenv("COST_MARKUP", "1.5")
    try:
        markup = float(raw)
    except ValueError:
        return 1.5
    if not math.isfinite(markup):
        return 1.5
    # A markup below 1.0 would mean charging less than cost; guard against typos.
    return max(markup, 1.0)


def get_price(model: str) -> ModelPrice:
    """Look up a model's price from the static table.

    Returns :data:`FALLBACK_PRICE` for anything not in :data:`MODEL_PRICES`.
    Used as a synchronous fallback when the live OpenRouter catalog
    isn't reachable (or when the caller can't await).
    """
    return MODEL_PRICES.get(model, FALLBACK_PRICE)


async def get_price_async(model: str) -> ModelPrice:
    """Resolve a model's price preferring the live OpenRouter catalog.

    Async because the live lookup can trigger a network refresh on the
    first call / past the 24h TTL. Falls back to the static table and
    then :data:`FALLBACK_PRICE`. Imported lazily to avoid a circular
    import (``models_catalog`` imports from this module).
    """
    from models_catalog import get_model_price

    return await get_model_price(model)


def _apply_markup(price: ModelPrice, prompt_tokens: int, completion_tokens: int) -> float:
    # Defense-in-depth against ``NaN`` / ``±Infinity`` (or negative)
    # values slipping into the ModelPrice via the live catalog.
    # ``models_catalog._parse_price`` rejects them at ingest, but a
    # legacy cached row, a unit-test caller stubbing in a hand-built
    # ModelPrice, or future plumbing could route a non-finite value
    # here. NaN propagates through every downstream IEEE-754 op
    # (``raw * markup`` is NaN, ``max(NaN, 0)`` is NaN) and ultimately
    # reaches ``database.deduct_balance`` whose finite guard refuses
    # the SQL — but the user still gets a free OpenRouter reply
    # because ``log_usage`` records ``cost=0``. A negative price
    # likewise rounds to zero through ``max(raw * markup, 0.0)``,
    # silently turning a paid model into a free one. Substitute the
    # conservative fallback so paid models stay paid.
    if not (
        math.isfinite(price.input_per_1m_usd)
        and math.isfinite(price.output_per_1m_usd)
        and price.input_per_1m_usd >= 0.0
        and price.output_per_1m_usd >= 0.0
    ):
        price = FALLBACK_PRICE
    # Stage-15-Step-E #4 bundled bug fix: the price half was
    # already NaN-guarded above, but the ``prompt_tokens`` /
    # ``completion_tokens`` half was not. They flow in straight
    # from ``data["usage"]["prompt_tokens"]`` /
    # ``["completion_tokens"]`` in ``ai_engine.chat_with_model``,
    # where Python's stdlib ``json.loads`` accepts the literal
    # ``NaN`` token by default — meaning a quirky OpenRouter 200
    # response (or, more realistically, a misbehaving stub /
    # custom proxy / future internal billing path) with a
    # non-finite token count would propagate NaN through the
    # multiplication, through ``raw * markup``, and finally
    # through ``max(NaN, 0.0)`` — which returns NaN in CPython
    # because the comparison ``NaN < 0.0`` is False, so ``max``
    # treats NaN as the maximum. The downstream impact mirrors
    # the price-side hole: ``deduct_balance`` refuses the NaN,
    # ``log_usage`` refuses the NaN, the user gets free chat
    # AND there's no row in ``usage_logs`` so the audit trail
    # has a hole. Defence-in-depth: clamp non-finite / negative
    # token counts to 0 (a zero-cost row keeps the audit trail
    # intact and lets the caller see "this call had corrupt
    # token counts" via the log line below). Genuine free
    # models are charged at the static price already; this only
    # affects the corrupted-input path.
    safe_prompt_tokens = _coerce_token_count(prompt_tokens, "prompt_tokens")
    safe_completion_tokens = _coerce_token_count(
        completion_tokens, "completion_tokens"
    )
    raw = (
        safe_prompt_tokens * price.input_per_1m_usd
        + safe_completion_tokens * price.output_per_1m_usd
    ) / 1_000_000.0
    return max(raw * get_markup(), 0.0)


def _coerce_token_count(value: object, label: str) -> float:
    """Clamp a token count to ``[0, +inf)`` and to a finite float.

    Defensive helper for :func:`_apply_markup` (Stage-15-Step-E #4
    bundled bug fix). Non-finite / non-numeric values become ``0.0``
    with a logged warning so ops can investigate the upstream source
    of the corrupt count. Negative ints / floats also clamp to ``0``.
    """
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        log.warning(
            "Refusing non-numeric %s=%r in cost computation; "
            "treating as 0. Investigate upstream caller.",
            label,
            value,
        )
        return 0.0
    if not math.isfinite(coerced):
        log.warning(
            "Refusing non-finite %s=%r in cost computation; "
            "treating as 0. Investigate upstream caller.",
            label,
            value,
        )
        return 0.0
    if coerced < 0.0:
        log.warning(
            "Refusing negative %s=%r in cost computation; "
            "treating as 0. Investigate upstream caller.",
            label,
            value,
        )
        return 0.0
    return coerced


def apply_markup_to_price(price: ModelPrice) -> ModelPrice:
    """Return a ``ModelPrice`` with :func:`get_markup` applied to each side.

    **Why this exists.** Billing applies the markup per-call via
    :func:`_apply_markup` after tokens are known, but the bot's model
    picker renders the *raw* OpenRouter-reported ``$/1M`` numbers —
    which are **not** what we actually charge the user (we charge
    ``raw * COST_MARKUP``). Showing the upstream sticker price in the
    picker and then deducting more from their wallet is dishonest and
    eroded user trust (2026-04-29 user feedback: *"right now in
    selecting models of chat it shows the price of same as the site.
    but we want som profits dont we. and thats not true to dont tell
    them"*).

    This helper is the single source of truth for the *display* side.
    Reuses :func:`_apply_markup`'s defensive fallback (non-finite /
    negative sides collapse to :data:`FALLBACK_PRICE`) so the picker
    never renders a silly ``$nan/1M``.
    """
    if not (
        math.isfinite(price.input_per_1m_usd)
        and math.isfinite(price.output_per_1m_usd)
        and price.input_per_1m_usd >= 0.0
        and price.output_per_1m_usd >= 0.0
    ):
        price = FALLBACK_PRICE
    markup = get_markup()
    return ModelPrice(
        input_per_1m_usd=price.input_per_1m_usd * markup,
        output_per_1m_usd=price.output_per_1m_usd * markup,
    )


def calculate_cost(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    """Synchronous cost using the static table. Kept for tests / callers
    that genuinely can't await. Production code should prefer
    :func:`calculate_cost_async`.
    """
    return _apply_markup(get_price(model), prompt_tokens, completion_tokens)


async def calculate_cost_async(
    model: str, prompt_tokens: int, completion_tokens: int
) -> float:
    """Compute the USD cost to charge the user, using the live catalog.

    Falls back to the static table when the catalog can't be reached.
    Result includes the configured markup and is always non-negative.
    """
    price = await get_price_async(model)
    return _apply_markup(price, prompt_tokens, completion_tokens)
