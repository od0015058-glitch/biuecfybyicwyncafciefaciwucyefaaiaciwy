"""Tests for pricing.calculate_cost and the markup logic.

Money-touching pure-Python — perfect first target for unit tests after the
IPN verifier (HANDOFF.md §8 P3-Op-3).
"""

from __future__ import annotations

import pytest

import pricing
from pricing import (
    DEFAULT_MARKUP,
    FALLBACK_PRICE,
    MARKUP_MINIMUM,
    MARKUP_OVERRIDE_MAXIMUM,
    MARKUP_SETTING_KEY,
    MODEL_PRICES,
    ModelPrice,
    _apply_markup,
    apply_markup_to_price,
    calculate_cost,
    clear_markup_override,
    get_markup,
    get_markup_override,
    get_markup_source,
    get_price,
    refresh_markup_override_from_db,
    set_markup_override,
)


@pytest.fixture(autouse=True)
def _reset_markup_override_between_tests():
    """Drop any process-local override set by a prior test.

    Stage-15-Step-E #10b row 2 added a module-level
    ``_MARKUP_OVERRIDE`` that, if leaked across tests, would shadow
    ``monkeypatch.setenv("COST_MARKUP", ...)`` in the existing
    suite — flipping their assertions silently. Autouse fixture so
    every test in this module starts from "no override active".
    """
    clear_markup_override()
    yield
    clear_markup_override()


def test_known_model_uses_table_price():
    """gpt-4o-mini is in the table at (0.15, 0.60). 1M input + 1M output, no
    markup environment override here so default 1.5x applies."""
    cost = calculate_cost("openai/gpt-4o-mini", 1_000_000, 1_000_000)
    # (1.0M * 0.15 + 1.0M * 0.60) / 1M = 0.75. * 1.5 markup = 1.125.
    assert cost == pytest.approx(1.125)


def test_unknown_model_uses_fallback_price():
    """Unmapped models charge at FALLBACK_PRICE so we never silently undercharge."""
    cost = calculate_cost("vendor/never-heard-of-it", 1_000_000, 1_000_000)
    # FALLBACK is (10, 30): (10 + 30) * 1.5 = 60.
    assert cost == pytest.approx(60.0)


def test_zero_tokens_zero_cost():
    assert calculate_cost("openai/gpt-3.5-turbo", 0, 0) == 0.0


def test_cost_scales_linearly_with_tokens():
    a = calculate_cost("openai/gpt-4o", 1000, 1000)
    b = calculate_cost("openai/gpt-4o", 2000, 2000)
    # 2x tokens -> 2x cost (within float).
    assert b == pytest.approx(2 * a)


def test_markup_env_override(monkeypatch):
    monkeypatch.setenv("COST_MARKUP", "2.0")
    assert get_markup() == 2.0


def test_markup_below_1_clamped(monkeypatch):
    """COST_MARKUP=0.5 would mean charging less than cost; treat as a typo
    and clamp to 1.0 (no profit, no loss)."""
    monkeypatch.setenv("COST_MARKUP", "0.5")
    assert get_markup() == 1.0


def test_markup_invalid_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("COST_MARKUP", "not-a-number")
    assert get_markup() == 1.5


def test_markup_unset_default(monkeypatch):
    monkeypatch.delenv("COST_MARKUP", raising=False)
    assert get_markup() == 1.5


def test_apply_markup_never_negative():
    """Defensive: cost should never be negative even on weird inputs."""
    cost = _apply_markup(FALLBACK_PRICE, prompt_tokens=0, completion_tokens=0)
    assert cost == 0.0


def test_get_price_returns_fallback_for_unmapped():
    assert get_price("totally-fake/model") is FALLBACK_PRICE


def test_get_price_returns_table_entry_for_mapped():
    p = get_price("anthropic/claude-3.5-sonnet")
    assert p is MODEL_PRICES["anthropic/claude-3.5-sonnet"]
    assert p.input_per_1m_usd == 3.00
    assert p.output_per_1m_usd == 15.00


# ---------------------------------------------------------------------
# Non-finite / negative pricing guards (PR: pricing-finite-guards)
# ---------------------------------------------------------------------
#
# Pre-fix bugs:
#
# (a) ``get_markup`` parsed ``COST_MARKUP`` with a bare ``float()``,
#     which accepts the strings ``"nan"``, ``"inf"``, ``"-inf"``
#     (case-insensitive). A NaN markup propagates through every
#     IEEE-754 op (``raw * NaN`` is NaN, ``max(NaN, 1.0)`` is NaN),
#     eventually reaching ``database.deduct_balance``. The DB-layer
#     ``_is_finite_amount`` guard refuses the SQL — but the caller
#     in ``ai_engine.chat_with_model`` then logs ``cost=0`` and the
#     user gets a free reply on a paid model, with the platform
#     eating the OpenRouter cost.
#
# (b) ``_apply_markup`` had no defensive check on the ModelPrice
#     fields themselves. A non-finite or negative price slipping in
#     from the live OpenRouter catalog (or a stub in a test) would
#     short-circuit to ``cost=0`` via the same path.
#
# Both regressions are pinned below.

@pytest.mark.parametrize("bad_value", ["nan", "NaN", "inf", "Infinity", "-inf", "-Infinity"])
def test_markup_rejects_non_finite_env(monkeypatch, bad_value):
    """COST_MARKUP=nan / inf / -inf must fall back to the default 1.5x.

    Pre-fix ``float("nan")`` slipped through ``max(nan, 1.0)`` —
    Python's ``max`` returns the first arg when neither comparison
    is true, so ``max(nan, 1.0)`` returns ``nan``. That NaN then
    poisoned every cost calculation downstream.
    """
    monkeypatch.setenv("COST_MARKUP", bad_value)
    assert get_markup() == 1.5


def test_markup_rejects_negative_inf_env_via_finite_check(monkeypatch):
    """Belt-and-suspenders: ``float("-inf")`` is also < 1.0 so the
    pre-existing ``max(markup, 1.0)`` clamp would have caught it,
    but the explicit ``isfinite`` guard is what catches ``+inf``
    (which is > 1.0 and would otherwise pass through unchanged
    into ``raw * inf = inf`` cost).
    """
    monkeypatch.setenv("COST_MARKUP", "inf")
    assert get_markup() == 1.5


@pytest.mark.parametrize(
    "input_per_1m, output_per_1m",
    [
        (float("nan"), 1.0),
        (1.0, float("nan")),
        (float("inf"), 1.0),
        (1.0, float("inf")),
        (float("-inf"), 1.0),
        (-1.0, 1.0),
        (1.0, -1.0),
    ],
)
def test_apply_markup_falls_back_for_non_finite_or_negative_price(
    monkeypatch, input_per_1m, output_per_1m
):
    """A non-finite or negative price must NOT silently round to $0.

    Pre-fix a NaN price gave ``raw = NaN``, then ``max(NaN, 0.0) =
    NaN``; ``deduct_balance`` refused the SQL but ``log_usage``
    recorded ``cost=0``, so the user got a free paid-model reply.
    Post-fix we substitute ``FALLBACK_PRICE`` (the conservative
    $10/$30 per-1M default) so paid models stay paid even when the
    upstream price is corrupted.
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    bad_price = type(FALLBACK_PRICE)(
        input_per_1m_usd=input_per_1m, output_per_1m_usd=output_per_1m
    )
    cost = _apply_markup(bad_price, prompt_tokens=1_000_000, completion_tokens=1_000_000)
    # FALLBACK_PRICE is (10, 30): (10 + 30) * 1.5 markup = 60.
    assert cost == pytest.approx(60.0)


def test_apply_markup_to_price_default_markup():
    """Default 1.5x markup should multiply both sides of a ModelPrice.

    This is the display-side equivalent of ``_apply_markup`` (which
    operates on token-weighted cost). Powers the model picker so the
    user sees what they'll actually be charged, not the upstream
    sticker price.
    """
    raw = ModelPrice(input_per_1m_usd=0.15, output_per_1m_usd=0.60)
    marked = apply_markup_to_price(raw)
    assert marked.input_per_1m_usd == pytest.approx(0.225)
    assert marked.output_per_1m_usd == pytest.approx(0.90)


def test_apply_markup_to_price_honours_env_override(monkeypatch):
    monkeypatch.setenv("COST_MARKUP", "2.5")
    raw = ModelPrice(input_per_1m_usd=1.00, output_per_1m_usd=2.00)
    marked = apply_markup_to_price(raw)
    assert marked.input_per_1m_usd == pytest.approx(2.50)
    assert marked.output_per_1m_usd == pytest.approx(5.00)


def test_apply_markup_to_price_preserves_zero_for_free_models():
    """Free-tier models (both sides $0/M) must stay $0 after markup so
    the `:free` semantics survive — otherwise a free model would
    display a nonsense non-zero price from
    ``0 * markup`` drift or a fallback substitution."""
    free = ModelPrice(input_per_1m_usd=0.0, output_per_1m_usd=0.0)
    marked = apply_markup_to_price(free)
    assert marked.input_per_1m_usd == 0.0
    assert marked.output_per_1m_usd == 0.0


def test_apply_markup_to_price_rejects_nan_with_fallback():
    """Same defensive fallback as ``_apply_markup``: a NaN/infinite/
    negative ModelPrice collapses to FALLBACK_PRICE so the picker
    never renders $nan/1M."""
    bad = ModelPrice(input_per_1m_usd=float("nan"), output_per_1m_usd=1.0)
    marked = apply_markup_to_price(bad)
    # Fallback is (10, 30); default markup 1.5x → (15, 45).
    assert marked.input_per_1m_usd == pytest.approx(15.0)
    assert marked.output_per_1m_usd == pytest.approx(45.0)


def test_apply_markup_to_price_display_matches_actual_charge(monkeypatch):
    """The key honesty property: what the picker shows per-1M is the
    same rate we actually charge via _apply_markup. Pick a specific
    COST_MARKUP, compute the displayed input rate, then compute the
    cost of exactly 1M input tokens — must match.
    """
    monkeypatch.setenv("COST_MARKUP", "1.8")
    raw = ModelPrice(input_per_1m_usd=2.00, output_per_1m_usd=8.00)
    displayed = apply_markup_to_price(raw)
    charged_for_1m_input = _apply_markup(raw, 1_000_000, 0)
    assert displayed.input_per_1m_usd == pytest.approx(charged_for_1m_input)
    charged_for_1m_output = _apply_markup(raw, 0, 1_000_000)
    assert displayed.output_per_1m_usd == pytest.approx(charged_for_1m_output)


def test_calculate_cost_happy_path_pin_with_finite_guards(monkeypatch):
    """Regression pin: the finite/negative guards in ``_apply_markup``
    must NOT alter the happy-path cost for a normal positive price.
    Without this pin a refactor of the guard could accidentally
    round every legitimate cost to FALLBACK_PRICE.
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    # 1M input + 1M output at gpt-4o-mini's (0.15, 0.60) per 1M.
    # (0.15 + 0.60) * 1.5 = 1.125.
    assert calculate_cost("openai/gpt-4o-mini", 1_000_000, 1_000_000) == pytest.approx(1.125)


# ---- Stage-15-Step-E #4 bundled bug fix: token-side NaN/non-finite guard ----


def test_apply_markup_clamps_nan_prompt_tokens_to_zero(monkeypatch):
    """A NaN prompt_token count (e.g. parsed from a quirky JSON payload
    that uses the literal ``NaN``) used to propagate through the
    multiplication, through ``raw * markup``, and through
    ``max(NaN, 0.0)`` (which returns NaN in CPython because
    ``NaN < 0.0`` is False) — yielding a NaN cost that
    ``deduct_balance`` and ``log_usage`` both refuse, leaving the
    audit trail with a hole and the user with free chat.

    Post-fix: NaN clamps to 0, the call records cleanly, ops sees
    a logged warning to investigate the upstream source.
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    cost = _apply_markup(
        ModelPrice(input_per_1m_usd=2.0, output_per_1m_usd=8.0),
        prompt_tokens=float("nan"),
        completion_tokens=100,
    )
    import math as _math

    assert _math.isfinite(cost)
    assert cost >= 0.0
    # The completion_tokens half is still charged: 100 * 8.0 / 1M * 1.5
    # = 0.0012.
    assert cost == pytest.approx(100 * 8.0 / 1_000_000.0 * 1.5)


def test_apply_markup_clamps_inf_completion_tokens_to_zero(monkeypatch):
    """+Inf completion_tokens used to produce +Inf cost; same downstream
    poisoning as the NaN case (deduct + log refuse, hole in audit
    trail). Post-fix: clamps to 0, only prompt half is charged.
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    cost = _apply_markup(
        ModelPrice(input_per_1m_usd=2.0, output_per_1m_usd=8.0),
        prompt_tokens=200,
        completion_tokens=float("inf"),
    )
    import math as _math

    assert _math.isfinite(cost)
    # Only the prompt half is charged: 200 * 2.0 / 1M * 1.5 = 0.0006.
    assert cost == pytest.approx(200 * 2.0 / 1_000_000.0 * 1.5)


def test_apply_markup_clamps_negative_tokens_to_zero(monkeypatch):
    """Negative tokens (a corrupted DB row, a misbehaving stub) used to
    produce a negative ``raw`` which ``max(raw * markup, 0.0)`` then
    rounded to 0 — silently dropping the cost. That zero-cost result
    was correct in this case, but only by accident. Pin the exact
    behaviour so a future refactor doesn't reintroduce a sign-flip.
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    cost = _apply_markup(
        ModelPrice(input_per_1m_usd=2.0, output_per_1m_usd=8.0),
        prompt_tokens=-100,
        completion_tokens=-200,
    )
    assert cost == 0.0


def test_apply_markup_clamps_string_token_count(monkeypatch):
    """Defence-in-depth: a non-numeric token count (a stub that
    forgot to int-cast; an OpenRouter response with a typo'd
    schema) used to raise TypeError on the multiplication. The
    raise was caught by an outer except in some call paths but
    not all — and the user-facing error was confusing. Post-fix:
    clamps to 0 with a logged warning so the call records cleanly.
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    cost = _apply_markup(
        ModelPrice(input_per_1m_usd=2.0, output_per_1m_usd=8.0),
        prompt_tokens="bad",  # type: ignore[arg-type]
        completion_tokens=100,
    )
    assert cost == pytest.approx(100 * 8.0 / 1_000_000.0 * 1.5)


def test_apply_markup_normal_positive_tokens_unchanged(monkeypatch):
    """Pin: the new guard must NOT change the happy-path cost. A
    refactor that accidentally clamped legitimate counts would
    still pass the NaN test but fail this one.
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    cost = _apply_markup(
        ModelPrice(input_per_1m_usd=2.0, output_per_1m_usd=8.0),
        prompt_tokens=500,
        completion_tokens=1500,
    )
    expected = (500 * 2.0 + 1500 * 8.0) / 1_000_000.0 * 1.5
    assert cost == pytest.approx(expected)


def test_apply_markup_zero_tokens_returns_zero(monkeypatch):
    """Edge case: a 200 with empty content + zero tokens. Cost is 0,
    no warning logged.
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    cost = _apply_markup(
        ModelPrice(input_per_1m_usd=2.0, output_per_1m_usd=8.0),
        prompt_tokens=0,
        completion_tokens=0,
    )
    assert cost == 0.0


def test_apply_markup_nan_tokens_combined_with_nan_price_both_clamp(monkeypatch):
    """When BOTH the price and the token counts are corrupt, both
    guards fire and the result is still finite (the price collapses
    to FALLBACK_PRICE, the tokens collapse to 0, so the cost is 0).
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    cost = _apply_markup(
        ModelPrice(
            input_per_1m_usd=float("nan"),
            output_per_1m_usd=float("nan"),
        ),
        prompt_tokens=float("nan"),
        completion_tokens=float("inf"),
    )
    import math as _math

    assert _math.isfinite(cost)
    assert cost == 0.0


# ---------------------------------------------------------------------
# DB-backed markup override (Stage-15-Step-E #10b row 2)
# ---------------------------------------------------------------------
#
# The override is a process-local cache populated by
# ``refresh_markup_override_from_db`` from a ``system_settings`` row
# keyed ``COST_MARKUP``. Resolution order is override → env → 1.5
# default. The web admin form writes the row + refreshes the cache
# after every save so a new value goes live without a restart.
#
# These tests use a stub ``Database`` so the suite stays sync /
# stdlib-only. The real DB path is exercised in
# ``tests/test_web_admin.py`` against the Postgres-backed harness.


class _StubMarkupDB:
    """Minimal in-memory stub of the Database settings surface."""

    def __init__(self, *, value: str | None = None, raise_on_get: bool = False):
        self._value = value
        self._raise_on_get = raise_on_get

    async def get_setting(self, key: str) -> str | None:
        if self._raise_on_get:
            raise RuntimeError("DB blip")
        if key != MARKUP_SETTING_KEY:
            return None
        return self._value


def test_set_markup_override_changes_get_markup(monkeypatch):
    monkeypatch.setenv("COST_MARKUP", "2.0")
    assert get_markup() == 2.0
    set_markup_override(3.5)
    assert get_markup() == 3.5
    assert get_markup_override() == 3.5
    assert get_markup_source() == "db"


def test_clear_markup_override_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("COST_MARKUP", "2.0")
    set_markup_override(3.5)
    assert get_markup() == 3.5
    assert clear_markup_override() is True
    assert get_markup() == 2.0
    assert get_markup_source() == "env"
    # Second clear is a no-op.
    assert clear_markup_override() is False


def test_clear_markup_override_falls_back_to_default(monkeypatch):
    monkeypatch.delenv("COST_MARKUP", raising=False)
    set_markup_override(3.0)
    assert get_markup() == 3.0
    assert get_markup_source() == "db"
    clear_markup_override()
    assert get_markup() == DEFAULT_MARKUP
    assert get_markup_source() == "default"


@pytest.mark.parametrize(
    "bad_value",
    [
        "not-a-number", "", "  ", "nan", "inf", "-inf",
        # Below the minimum.
        "0.5", "0.99", "-1",
    ],
)
def test_set_markup_override_rejects_bad_values(bad_value):
    with pytest.raises(ValueError):
        set_markup_override(bad_value)
    # Override must not have been polluted by the failed call.
    assert get_markup_override() is None


def test_set_markup_override_rejects_above_maximum():
    with pytest.raises(ValueError):
        set_markup_override(MARKUP_OVERRIDE_MAXIMUM)
    with pytest.raises(ValueError):
        set_markup_override(MARKUP_OVERRIDE_MAXIMUM + 1.0)
    assert get_markup_override() is None


def test_set_markup_override_rejects_bool():
    """``True`` is a subclass of ``int`` in Python — so a buggy caller
    posting ``True`` would otherwise sneak through as ``1.0``. Refuse
    explicitly so a nonsense value can't poison the cache."""
    with pytest.raises(ValueError):
        set_markup_override(True)
    with pytest.raises(ValueError):
        set_markup_override(False)


def test_set_markup_override_accepts_minimum():
    set_markup_override(MARKUP_MINIMUM)
    assert get_markup_override() == MARKUP_MINIMUM
    assert get_markup() == MARKUP_MINIMUM


def test_set_markup_override_accepts_just_below_maximum():
    set_markup_override(MARKUP_OVERRIDE_MAXIMUM - 0.0001)
    assert get_markup_override() == pytest.approx(MARKUP_OVERRIDE_MAXIMUM - 0.0001)


def test_get_markup_source_handles_invalid_env(monkeypatch):
    """An invalid env value should mark source as ``default``, not ``env``,
    so the panel doesn't claim an env value is active when ``get_markup()``
    is actually returning the default."""
    monkeypatch.setenv("COST_MARKUP", "not-a-number")
    assert get_markup() == DEFAULT_MARKUP
    assert get_markup_source() == "default"


def test_get_markup_source_handles_unset_env(monkeypatch):
    monkeypatch.delenv("COST_MARKUP", raising=False)
    assert get_markup_source() == "default"


def test_get_markup_source_reports_env_when_set(monkeypatch):
    monkeypatch.setenv("COST_MARKUP", "2.5")
    assert get_markup_source() == "env"


@pytest.mark.asyncio
async def test_refresh_markup_override_from_db_loads_value(monkeypatch):
    monkeypatch.setenv("COST_MARKUP", "2.0")
    db = _StubMarkupDB(value="3.25")
    result = await refresh_markup_override_from_db(db)
    assert result == 3.25
    assert get_markup_override() == 3.25
    assert get_markup() == 3.25
    assert get_markup_source() == "db"


@pytest.mark.asyncio
async def test_refresh_markup_override_from_db_clears_when_row_absent(monkeypatch):
    monkeypatch.setenv("COST_MARKUP", "2.0")
    set_markup_override(3.0)
    db = _StubMarkupDB(value=None)
    result = await refresh_markup_override_from_db(db)
    assert result is None
    assert get_markup_override() is None
    assert get_markup() == 2.0


@pytest.mark.asyncio
async def test_refresh_markup_override_from_db_clears_on_invalid(monkeypatch):
    """A malformed stored value (e.g. a hand-written DB row that bypasses
    the validators) clears the override so the bot falls through to env /
    default rather than charging based on garbage."""
    monkeypatch.setenv("COST_MARKUP", "2.0")
    set_markup_override(3.0)
    db = _StubMarkupDB(value="not-a-number")
    result = await refresh_markup_override_from_db(db)
    assert result is None
    assert get_markup_override() is None
    assert get_markup() == 2.0


@pytest.mark.asyncio
async def test_refresh_markup_override_from_db_keeps_cache_on_error(monkeypatch):
    """A transient DB error must keep the previous cache in place so a
    pool blip can't accidentally revert to env / default mid-incident."""
    monkeypatch.setenv("COST_MARKUP", "2.0")
    set_markup_override(3.0)
    db = _StubMarkupDB(raise_on_get=True)
    result = await refresh_markup_override_from_db(db)
    assert result == 3.0
    assert get_markup_override() == 3.0
    assert get_markup() == 3.0


@pytest.mark.asyncio
async def test_refresh_markup_override_from_db_no_db():
    """``db is None`` (dev-mode) must not raise; just returns the cached value."""
    set_markup_override(2.5)
    result = await refresh_markup_override_from_db(None)
    assert result == 2.5


@pytest.mark.asyncio
async def test_refresh_markup_override_strips_below_minimum(monkeypatch):
    """Stored ``0.5`` (below MARKUP_MINIMUM) must not be applied.

    A markup below 1.0 means charging less than cost. The override
    layer refuses it, falling through to env / default. Without this
    guard, an admin form fat-finger that bypassed the form-side
    minimum would leak money on every paid request.
    """
    monkeypatch.setenv("COST_MARKUP", "1.5")
    db = _StubMarkupDB(value="0.5")
    result = await refresh_markup_override_from_db(db)
    assert result is None
    assert get_markup() == 1.5


def test_apply_markup_uses_override(monkeypatch):
    """End-to-end: setting an override changes what a paid request charges."""
    monkeypatch.setenv("COST_MARKUP", "2.0")
    base_cost = calculate_cost("openai/gpt-4o-mini", 1_000_000, 1_000_000)
    # Raw cost for gpt-4o-mini at 1M input + 1M output: 0.75. * 2.0 = 1.5.
    assert base_cost == pytest.approx(1.5)

    set_markup_override(4.0)
    overridden_cost = calculate_cost("openai/gpt-4o-mini", 1_000_000, 1_000_000)
    assert overridden_cost == pytest.approx(3.0)


def test_apply_markup_to_price_uses_override(monkeypatch):
    """The model picker's ``apply_markup_to_price`` honours the DB override
    just like the per-call billing path."""
    monkeypatch.setenv("COST_MARKUP", "2.0")
    price = MODEL_PRICES["openai/gpt-4o-mini"]
    set_markup_override(4.0)
    displayed = apply_markup_to_price(price)
    assert displayed.input_per_1m_usd == pytest.approx(0.15 * 4.0)
    assert displayed.output_per_1m_usd == pytest.approx(0.60 * 4.0)
