"""DB-backed override layer for the operator's refund-reason presets.

Stage-15-Step-E #10b row 28. The transactions-page refund form
takes a free-text ``reason`` today; an operator who issues
hundreds of refunds during a gateway flap ends up writing the
same boilerplate ("duplicate payment", "user cancellation",
"stuck invoice") over and over. This module surfaces a small,
operator-curated list of canned reasons that the refund form's
new ``<select>`` populates so a click + save replaces a typing
session.

Storage shape mirrors the rest of the §10b knobs:

* In-process override cache ``_REFUND_PRESETS_OVERRIDE`` with
  the standard ``set / clear / get / refresh_from_db``
  surface, populated from
  ``system_settings.REFUND_PRESETS`` at boot (in ``main.py``)
  and on every ``/admin/refund-presets`` GET.
* Env var ``REFUND_PRESETS`` accepts a ``\\n`` (or ``|``)
  separated list — same delimiter the editor textarea uses,
  so an operator can paste from .env into the textarea or
  vice-versa without reformatting.
* Compile-time default (:data:`DEFAULT_REFUND_PRESETS`) covers
  the common cases the operator will hit on day one — duplicate
  payment, user-requested cancellation, stuck invoice, fraud
  / chargeback, bot or platform error. The default is shipped
  empty-list-aware: an operator can save a 0-preset list to
  hide the dropdown entirely (some deploys want the textarea
  back).

Resolution order on read:

1. In-process override (set by the admin form / boot warm-up).
2. ``REFUND_PRESETS`` env var (newline- / pipe-separated).
3. :data:`DEFAULT_REFUND_PRESETS` compile-time fallback.

Validation:

* Each preset is stripped, NUL-stripped, and capped at
  :data:`MAX_PRESET_LENGTH`. Any preset that empties out
  after strip is dropped silently rather than failing the
  whole save (fits the operator-paste-from-spreadsheet flow
  where a stray blank line is normal).
* :data:`MAX_PRESET_COUNT` upper bound on list length so
  the JSON payload fits the 255-char ``system_settings``
  column comfortably even with the largest preset strings.
* Duplicates are de-duplicated case-insensitively to match
  the operator's intuition that "Duplicate Payment" and
  "duplicate payment" are the same preset.

Status: **first slice.** A future enhancement could add a
per-preset "default amount %" field (refund 50% / 100% /
custom) so the dropdown wires both the reason AND the amount,
but the current refund form only takes the full credited
amount and that's the right default for >95% of refunds — so
adding the amount field would be column-bloat for marginal
operator benefit. Tracked as a Step-E #10b row 28 follow-up.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from database import Database

log = logging.getLogger("bot.refund_presets")


# ----------------------------------------------------------------------
# Defaults & bounds
# ----------------------------------------------------------------------

# Compile-time fallback list. Order matters — the dropdown renders in
# this order, so the most-common refund case is at the top so the
# operator's mouse hits it first. Empty strings would let an operator
# accidentally save a "blank" preset that auto-submits with no
# reason; the validators in this module strip those out.
DEFAULT_REFUND_PRESETS: tuple[str, ...] = (
    "Duplicate payment",
    "User-requested cancellation",
    "Stuck invoice — never credited",
    "Fraud / chargeback",
    "Bot or platform error",
)

# Hard ceiling on list length. The serialised JSON sits in the
# ``system_settings.setting_value VARCHAR(255)`` column (enforced
# in ``Database.upsert_setting``), so the upper bound must keep
# ``json.dumps(presets, ensure_ascii=False)`` strictly under 255
# characters for every plausible mix of preset strings. With
# :data:`MAX_PRESET_LENGTH` = 40 the worst case is
# ``["aaaa…aaaa","aaaa…aaaa", …]`` — 5 entries × (40 chars + 2
# quotes) + 4 commas + 2 brackets = 216 chars, leaving comfortable
# headroom for the JSON column. Going to 6 entries × 40 chars
# would push the worst case to 259 chars and trip the column cap
# on a fully-loaded preset list, so 5 is the documented limit.
# Operators that genuinely need more should shorten individual
# preset strings or ask for a column-widening migration.
MAX_PRESET_COUNT: int = 5

# Per-preset character cap. Long enough for a fluent reason like
# "Stuck invoice — funds not credited" but short enough to keep the
# `<select>` dropdown legible on a narrow viewport. The textarea
# editor enforces this client-side via ``maxlength`` so the
# operator gets immediate feedback rather than a silent truncate.
MAX_PRESET_LENGTH: int = 40

REFUND_PRESETS_SETTING_KEY: str = "REFUND_PRESETS"

# Process-local cache populated from ``system_settings`` at boot and
# on every admin-form load. ``None`` means "no DB override; fall
# through to env / default". An empty list ``[]`` is **not** the
# same as ``None`` — it's the operator's explicit choice to hide the
# dropdown entirely (see module docstring).
_REFUND_PRESETS_OVERRIDE: list[str] | None = None


# ----------------------------------------------------------------------
# Coercion / validation
# ----------------------------------------------------------------------


def _coerce_one_preset(value: object) -> str | None:
    """Strip / NUL-strip / cap a single preset string.

    Returns the cleaned string on success, or ``None`` if the
    input is non-string / empty after strip. Length is clamped to
    :data:`MAX_PRESET_LENGTH` rather than rejected — an operator
    pasting a long reason gets it silently truncated to fit the
    dropdown; rejecting the whole save would be hostile.
    """
    if not isinstance(value, str):
        return None
    cleaned = value.replace("\x00", "").strip()
    if not cleaned:
        return None
    if len(cleaned) > MAX_PRESET_LENGTH:
        cleaned = cleaned[:MAX_PRESET_LENGTH].rstrip()
    if not cleaned:
        return None
    return cleaned


def coerce_refund_presets(values: Iterable[object]) -> list[str]:
    """Coerce a raw iterable into a clean preset list.

    * Each element is run through :func:`_coerce_one_preset`
      (strip + NUL-strip + length-cap).
    * Empties after cleaning are dropped silently.
    * Duplicates are removed case-insensitively (first occurrence
      wins so the operator's preferred spelling is preserved).
    * Truncates to :data:`MAX_PRESET_COUNT` items.
    """
    seen_lower: set[str] = set()
    out: list[str] = []
    for raw in values:
        cleaned = _coerce_one_preset(raw)
        if cleaned is None:
            continue
        key = cleaned.lower()
        if key in seen_lower:
            continue
        seen_lower.add(key)
        out.append(cleaned)
        if len(out) >= MAX_PRESET_COUNT:
            break
    return out


def parse_refund_presets_text(raw: str) -> list[str]:
    """Parse a multi-line / pipe-separated string into a preset list.

    Newlines and pipes (``|``) both work as separators so an
    operator can paste from a CSV-like or a vertical-list source
    without reformatting. Calls :func:`coerce_refund_presets` on
    the split result so the same dedupe / cap / strip rules apply.
    """
    if not isinstance(raw, str):
        return []
    parts: list[str] = []
    for line in raw.splitlines():
        for piece in line.split("|"):
            parts.append(piece)
    return coerce_refund_presets(parts)


def _decode_stored_presets(raw: str) -> list[str] | None:
    """Decode the JSON-encoded preset list stored in
    ``system_settings``.

    Returns the cleaned list on a parseable JSON array of strings,
    ``None`` on a parse error / wrong shape so the caller can fall
    through to env / default rather than serving a corrupted
    override.
    """
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(parsed, list):
        return None
    return coerce_refund_presets(parsed)


def encode_refund_presets_for_storage(values: list[str]) -> str:
    """Serialise a coerced preset list for the
    ``system_settings`` overlay.

    Always sorts by no-op (preserves order) — the operator's
    chosen ordering is meaningful (most-common reason first).
    Uses ``ensure_ascii=False`` so a Persian / accented preset
    round-trips losslessly. Caller is responsible for length-
    checking the encoded string against the 255-char column cap;
    in practice :data:`MAX_PRESET_COUNT` × :data:`MAX_PRESET_LENGTH`
    keeps it well under that bound.
    """
    return json.dumps(values, ensure_ascii=False)


# ----------------------------------------------------------------------
# Override get / set / clear
# ----------------------------------------------------------------------


def set_refund_presets_override(values: Iterable[object]) -> list[str]:
    """Replace the in-process refund-presets override.

    Returns the coerced list that ended up in the cache. An empty
    list is a valid override (means "hide the dropdown").
    """
    global _REFUND_PRESETS_OVERRIDE
    coerced = coerce_refund_presets(values)
    _REFUND_PRESETS_OVERRIDE = coerced
    return coerced


def clear_refund_presets_override() -> bool:
    """Drop the in-process override. Returns True if one was active."""
    global _REFUND_PRESETS_OVERRIDE
    had = _REFUND_PRESETS_OVERRIDE is not None
    _REFUND_PRESETS_OVERRIDE = None
    return had


def get_refund_presets_override() -> list[str] | None:
    """Return the current in-process override (or ``None`` if unset).

    A non-``None`` result is always a list — possibly empty. An
    empty list means the operator explicitly turned the dropdown
    off; callers should respect that and not "helpfully" fall
    through to the default.
    """
    if _REFUND_PRESETS_OVERRIDE is None:
        return None
    return list(_REFUND_PRESETS_OVERRIDE)


async def refresh_refund_presets_override_from_db(
    db: "Database | None",
) -> list[str] | None:
    """Reload the override from the ``system_settings`` overlay.

    Mirrors the rest of the §10b refresh helpers: a transient DB
    error keeps the previous cache in place rather than reverting
    to env / default mid-incident; a malformed stored value
    (non-JSON / wrong shape) clears the override and logs.
    """
    global _REFUND_PRESETS_OVERRIDE
    if db is None:
        return _REFUND_PRESETS_OVERRIDE
    try:
        raw = await db.get_setting(REFUND_PRESETS_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_refund_presets_override_from_db: "
            "get_setting failed; keeping previous cache"
        )
        return _REFUND_PRESETS_OVERRIDE
    if raw is None:
        _REFUND_PRESETS_OVERRIDE = None
        return None
    decoded = _decode_stored_presets(raw)
    if decoded is None:
        log.warning(
            "refresh_refund_presets_override_from_db: rejected stored "
            "value %r; clearing override",
            raw,
        )
        _REFUND_PRESETS_OVERRIDE = None
        return None
    _REFUND_PRESETS_OVERRIDE = decoded
    return list(decoded)


# ----------------------------------------------------------------------
# Public lookup
# ----------------------------------------------------------------------


def _read_env_presets() -> list[str] | None:
    """Parse the ``REFUND_PRESETS`` env var. Returns ``None`` when
    the var is unset (empty string also treated as unset)."""
    raw = os.getenv("REFUND_PRESETS")
    if raw is None:
        return None
    parsed = parse_refund_presets_text(raw)
    if not parsed:
        # Distinguish "env unset" from "env present but every entry
        # blanked out" — a misconfigured env should fall through to
        # the default rather than serving a hidden-dropdown shape.
        return None
    return parsed


def get_refund_presets() -> list[str]:
    """Return the resolved preset list for the refund-form dropdown.

    Resolution order:
        1. In-process DB override (possibly an explicit empty list).
        2. ``REFUND_PRESETS`` env var (parsed, non-empty).
        3. :data:`DEFAULT_REFUND_PRESETS` compile-time fallback.
    """
    if _REFUND_PRESETS_OVERRIDE is not None:
        return list(_REFUND_PRESETS_OVERRIDE)
    env_parsed = _read_env_presets()
    if env_parsed is not None:
        return env_parsed
    return list(DEFAULT_REFUND_PRESETS)


def get_refund_presets_source() -> str:
    """Return ``"db" / "env" / "default"`` for the active list.

    Used by the ``/admin/refund-presets`` panel to render the same
    "effective / db / env / default" badge the rest of the §10b
    knobs surface.
    """
    if _REFUND_PRESETS_OVERRIDE is not None:
        return "db"
    if _read_env_presets() is not None:
        return "env"
    return "default"
