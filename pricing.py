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

import os
from dataclasses import dataclass


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
    """Return the cost markup multiplier from env, default 1.5x."""
    raw = os.getenv("COST_MARKUP", "1.5")
    try:
        markup = float(raw)
    except ValueError:
        return 1.5
    # A markup below 1.0 would mean charging less than cost; guard against typos.
    return max(markup, 1.0)


def get_price(model: str) -> ModelPrice:
    """Look up a model's price, returning the fallback if unknown."""
    return MODEL_PRICES.get(model, FALLBACK_PRICE)


def calculate_cost(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    """Compute the USD cost to charge the user for one request.

    Includes the configured markup. Result is always non-negative.
    """
    price = get_price(model)
    raw = (
        prompt_tokens * price.input_per_1m_usd
        + completion_tokens * price.output_per_1m_usd
    ) / 1_000_000.0
    return max(raw * get_markup(), 0.0)
