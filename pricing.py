"""Per-model pricing for OpenRouter calls.

All prices are USD per 1,000,000 tokens, taken from OpenRouter's public model
list. Update this table when prices change or when new models are enabled in
the bot. Prices are intentionally rounded up slightly to be conservative.

The final cost charged to the user is:

    (prompt_tokens * input_price + completion_tokens * output_price) / 1_000_000
        * MARKUP

MARKUP defaults to 1.5x. The resolution order (Stage-15-Step-E #10b row 2):

    1. DB override (``system_settings`` row keyed ``COST_MARKUP``).
    2. ``COST_MARKUP`` env var.
    3. Compile-time default of ``1.5``.

The DB override is populated from the web admin ``/admin/monetization``
markup-editor form; :func:`refresh_markup_override_from_db` is called at
boot and after every write so the next call to :func:`get_markup` sees the
new value without a process restart. Same shape as
``bot_health._THRESHOLD_OVERRIDES`` so the two surfaces look familiar.
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass

log = logging.getLogger("bot.pricing")

# ---------------------------------------------------------------------------
# DB-backed markup override (Stage-15-Step-E #10b row 2)
# ---------------------------------------------------------------------------
#
# Process-local cache populated from ``system_settings`` by
# :func:`refresh_markup_override_from_db`. ``None`` means "no override
# active" — fall through to env / default. Any non-``None`` value is
# already validated (finite + ``>= 1.0``).
_MARKUP_OVERRIDE: float | None = None

# Compile-time default. Single source of truth so the env-fallback
# branch in :func:`get_markup` and the "default" badge on
# ``/admin/monetization`` agree on the number.
DEFAULT_MARKUP: float = 1.5

# Floor on any markup the operator can configure. A multiplier
# below 1.0 means "charge less than cost" — a typo with that
# shape would leak money on every paid request and the operator
# would not notice until the wallet ran dry.
MARKUP_MINIMUM: float = 1.0

# Ceiling on the override side ONLY. Env / default callers are
# already free to set arbitrarily large markups (the existing
# behaviour — we don't want to accidentally cap a deliberate
# 50x markup that worked yesterday). The ceiling exists to guard
# against an admin-form fat-finger like ``150`` (intended ``1.5``)
# that would 100x every charge silently. The web form refuses
# anything ``>= MARKUP_OVERRIDE_MAXIMUM`` with a flash banner.
MARKUP_OVERRIDE_MAXIMUM: float = 100.0

# Setting key in the ``system_settings`` table. Public so the
# web admin module + tests don't repeat the literal.
MARKUP_SETTING_KEY: str = "COST_MARKUP"


def _coerce_markup(value: object) -> float | None:
    """Best-effort parse of a markup string.

    Returns a finite ``>= MARKUP_MINIMUM`` float, or ``None`` for
    anything malformed (empty / non-numeric / NaN / ±Inf / below
    minimum). The shape mirrors ``bot_health._env_int`` so a future
    refactor can extract a generic env-overlay helper.
    """
    if value is None:
        return None
    try:
        coerced = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if not math.isfinite(coerced):
        return None
    if coerced < MARKUP_MINIMUM:
        return None
    return coerced


def set_markup_override(value: float) -> None:
    """Apply an in-process override for ``COST_MARKUP``.

    Refuses non-finite / below-minimum / above-ceiling / non-numeric
    inputs with ``ValueError``. The web admin form runs the same
    checks before persisting; this guard is defence-in-depth so a
    buggy direct caller can't poison the cache.
    """
    if isinstance(value, bool):
        # bool is a subclass of int — refuse it explicitly so
        # ``True`` doesn't sneak through as ``1.0`` (nonsense markup).
        raise ValueError(
            f"markup must be int|float, got bool ({value!r})"
        )
    try:
        coerced = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"markup is not numeric: {value!r}") from exc
    if not math.isfinite(coerced):
        raise ValueError(f"markup is not finite: {value!r}")
    if coerced < MARKUP_MINIMUM:
        raise ValueError(
            f"markup {coerced!r} is below minimum {MARKUP_MINIMUM}"
        )
    if coerced >= MARKUP_OVERRIDE_MAXIMUM:
        raise ValueError(
            f"markup {coerced!r} is at or above the override maximum "
            f"{MARKUP_OVERRIDE_MAXIMUM}"
        )
    global _MARKUP_OVERRIDE
    _MARKUP_OVERRIDE = coerced


def clear_markup_override() -> bool:
    """Drop the in-process override. Returns True if one existed."""
    global _MARKUP_OVERRIDE
    had_override = _MARKUP_OVERRIDE is not None
    _MARKUP_OVERRIDE = None
    return had_override


def get_markup_override() -> float | None:
    """Return the current override value, or ``None`` if unset."""
    return _MARKUP_OVERRIDE


async def refresh_markup_override_from_db(db) -> float | None:
    """Reload the markup override from the ``system_settings`` table.

    Same defensive shape as
    ``bot_health.refresh_threshold_overrides_from_db``: a transient
    DB error keeps the previous cache in place (so a pool blip
    can't accidentally revert to env default mid-incident). A
    malformed stored value clears the override (so a bad write
    that somehow bypassed the validators doesn't permanently
    poison the markup).
    """
    global _MARKUP_OVERRIDE
    if db is None:
        return _MARKUP_OVERRIDE
    try:
        raw = await db.get_setting(MARKUP_SETTING_KEY)
    except Exception:
        log.exception(
            "pricing: refresh_markup_override_from_db failed; "
            "keeping previous override cache"
        )
        return _MARKUP_OVERRIDE
    if raw is None:
        # Row absent — clear any stale process-local override.
        _MARKUP_OVERRIDE = None
        return None
    parsed = _coerce_markup(raw)
    if parsed is None:
        log.warning(
            "pricing: invalid stored COST_MARKUP override %r in "
            "system_settings; clearing in-process override and "
            "falling through to env / default",
            raw,
        )
        _MARKUP_OVERRIDE = None
        return None
    _MARKUP_OVERRIDE = parsed
    return parsed


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
    """Return the cost markup multiplier with DB → env → default precedence.

    Resolution order (Stage-15-Step-E #10b row 2):

    1. In-process DB override populated from ``system_settings`` by
       :func:`refresh_markup_override_from_db`. The web admin
       ``/admin/monetization`` markup-editor form writes the row +
       refreshes the cache after every save.
    2. ``COST_MARKUP`` env var.
    3. :data:`DEFAULT_MARKUP` (``1.5``).

    Rejects ``NaN`` / ``±Infinity`` at every layer. ``float()``
    accepts the strings ``"nan"`` and ``"inf"`` (case-insensitive)
    so a typo or a deliberately malicious ``COST_MARKUP=nan`` would
    otherwise slip past the parse step. A NaN markup propagates
    through :func:`_apply_markup` (every IEEE-754 op against NaN
    returns NaN) and ultimately reaches ``database.deduct_balance``;
    the ``_is_finite_amount`` guard there refuses the SQL but the
    user still gets a free OpenRouter reply because ``log_usage``
    records ``cost=0``. Reject upstream so paid models stay paid
    even with a deploy-time misconfiguration.
    """
    # Layer 1: DB override populated by refresh_markup_override_from_db.
    # _coerce_markup defends against the rare case where ``set_markup_override``
    # was bypassed by a future caller.
    if _MARKUP_OVERRIDE is not None:
        validated = _coerce_markup(_MARKUP_OVERRIDE)
        if validated is not None:
            return validated

    # Layer 2: env var.
    raw = os.getenv("COST_MARKUP", str(DEFAULT_MARKUP))
    try:
        markup = float(raw)
    except ValueError:
        return DEFAULT_MARKUP
    if not math.isfinite(markup):
        return DEFAULT_MARKUP
    # A markup below 1.0 would mean charging less than cost; guard against typos.
    return max(markup, MARKUP_MINIMUM)


def get_markup_source() -> str:
    """Return where :func:`get_markup` resolved from (``db`` / ``env`` / ``default``).

    Renders on ``/admin/monetization`` next to the effective markup so
    operators see at a glance which knob is actually live. Mirrors
    the ``source`` column on the bot-health threshold table.
    """
    if _MARKUP_OVERRIDE is not None and _coerce_markup(_MARKUP_OVERRIDE) is not None:
        return "db"
    raw = os.getenv("COST_MARKUP", "").strip()
    if raw:
        try:
            parsed = float(raw)
        except ValueError:
            return "default"
        if not math.isfinite(parsed):
            return "default"
        return "env"
    return "default"


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
