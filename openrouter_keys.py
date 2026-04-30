"""Multi-key OpenRouter load balancing.

Stage-14-Step-C. Supports up to 10 API keys via numbered env vars:

    OPENROUTER_API_KEY=sk-or-...          # legacy single-key (always works)
    OPENROUTER_API_KEY_1=sk-or-...        # numbered slots 1-10
    OPENROUTER_API_KEY_2=sk-or-...
    ...

Keys are loaded **lazily** on the first call to :func:`key_for_user`
or :func:`key_count` (Stage-15-Step-D #2). Earlier revisions ran
``load_keys()`` at module import time; that was harmless in
production but had two latent issues for tests / scripts that
import ``openrouter_keys`` without the OpenRouter env vars set:

* Every cold import emitted a spurious
  ``"No OPENROUTER_API_KEY* env vars found."`` WARNING into the log
  even when the importer never intended to call OpenRouter.
* Tests that ``monkeypatch.setenv("OPENROUTER_API_KEY", ...)`` had
  to manually call ``openrouter_keys.load_keys()`` after the
  monkeypatch to repopulate ``_keys``, because the eager-at-import
  load had already snapshotted the env *before* the monkeypatch
  ran.

Lazy initialisation fixes both: tests can monkeypatch and call
``key_for_user`` directly, and an importer that never needs an
OpenRouter key (e.g. a small DB-only script) gets a silent import.

Loading rules:

* Keys with a numbered suffix (``_1`` through ``_10``) are added
  first in order.
* If no numbered keys exist, the bare ``OPENROUTER_API_KEY`` is
  used as a single-element list (backward-compatible).
* If both exist, the bare key is ignored and a warning is logged —
  the numbered keys take precedence.

Key selection for a given user is **sticky**: the user's Telegram
id is hashed into a pool index, so the same user always routes to
the same key. This keeps conversation context (on OpenRouter's
side) consistent and avoids mid-conversation key switches.

Stage-15-Step-E #4 first slice — per-key 429 cooldown:

When OpenRouter returns 429 for a specific key (the upstream
provider rate-limited it, or the key itself hit its OpenRouter
plan ceiling), the call site can call :func:`mark_key_rate_limited`
to put that key in a short cooldown. Subsequent
:func:`key_for_user` calls during the cooldown skip the bad key
and route the user to the next available pool member instead —
keeping the user's conversation moving rather than returning a
"rate-limited" error every time their sticky key is the one
under pressure. When ALL keys are in cooldown the function falls
back to the sticky pick (with a warning) so the user gets at
least an attempt rather than a hard "no service" error.

Cooldown state is **process-local** — there's no Redis / DB
coordination. Two replicas of the bot will track their own
cooldowns independently. Acceptable trade-off for the first
slice: the cooldown is short (60s default) so an out-of-sync
replica catches up within minutes; durable cross-replica state
is on the follow-up list in HANDOFF §5.

Public surface:

* :func:`load_keys` — re-read the env and refill ``_keys``. Tests
  and operators with hot-reload tooling can call this; production
  paths don't need to.
* :func:`key_for_user` — returns the API key string for a given
  ``telegram_id``. Lazy-loads on first call. Skips keys in
  cooldown when at least one key is available. Raises
  ``RuntimeError`` if no keys are configured at first-call time.
* :func:`key_count` — number of keys in the pool. Lazy-loads on
  first call.
* :func:`mark_key_rate_limited` — put a key in cooldown for
  ``retry_after_secs`` (default 60).
* :func:`is_key_rate_limited` — check if a key is currently in
  cooldown. Useful for tests and ops dashboards.
* :func:`available_key_count` — number of keys that aren't in
  cooldown right now.
* :func:`key_status_snapshot` — per-key dict for diagnostics
  (admin panel / metrics).
* :func:`clear_all_cooldowns` — wipe the cooldown table. Tests +
  ops "force everything back online" recovery.
"""

from __future__ import annotations

import logging
import math
import os
import time

log = logging.getLogger("bot.openrouter_keys")

_keys: list[str] = []
_loaded: bool = False

# Per-key cooldown table: ``api_key -> deadline_monotonic_seconds``.
# Read on every ``key_for_user`` call; entries past the deadline
# are pruned lazily by :func:`_drop_expired_cooldowns`. Bounded by
# the size of the key pool (max 10 entries) so there's no need for
# a separate eviction policy.
_cooldowns: dict[str, float] = {}

# Default cooldown when the call site doesn't supply a Retry-After.
# 60s matches OpenRouter's typical per-minute rate-limit window for
# free models and is short enough that a transient blip clears
# without operator intervention.
DEFAULT_COOLDOWN_SECS: float = 60.0

# Hard cap on the cooldown duration. A misconfigured upstream
# Retry-After (e.g. ``Retry-After: 86400`` from a misbehaving CDN)
# would otherwise pin a key out of rotation for a day. Cap at 1
# hour so even pathological values still recover by the next
# operator shift.
MAX_COOLDOWN_SECS: float = 3600.0


def load_keys() -> None:
    """(Re-)load API keys from the environment.

    Resets the lazy-load flag so subsequent calls to
    :func:`key_for_user` / :func:`key_count` see the freshly
    populated ``_keys`` list (i.e. tests calling ``load_keys()``
    after a ``monkeypatch.setenv`` see the new keys immediately).
    """
    global _keys, _loaded
    numbered: list[str] = []
    for i in range(1, 11):
        val = os.getenv(f"OPENROUTER_API_KEY_{i}", "").strip()
        if val:
            numbered.append(val)

    bare = os.getenv("OPENROUTER_API_KEY", "").strip()

    if numbered:
        if bare:
            log.warning(
                "Both OPENROUTER_API_KEY and numbered OPENROUTER_API_KEY_N "
                "vars found. Using the %d numbered key(s); ignoring "
                "OPENROUTER_API_KEY.",
                len(numbered),
            )
        _keys = numbered
        log.info("Loaded %d numbered OpenRouter API key(s).", len(_keys))
    elif bare:
        _keys = [bare]
        log.info("Using single OPENROUTER_API_KEY.")
    else:
        _keys = []
        log.warning("No OPENROUTER_API_KEY* env vars found.")
    _loaded = True


def _ensure_loaded() -> None:
    """Trigger lazy-load if it hasn't run yet this process lifetime."""
    if not _loaded:
        load_keys()


def _drop_expired_cooldowns(now: float | None = None) -> None:
    """Prune any cooldown entries whose deadline has elapsed.

    Called from the membership-test paths so the cooldown table
    self-cleans without needing a background sweeper task. ``now``
    is parameterised purely for tests; production code passes
    ``None`` and the function reads ``time.monotonic()`` itself.
    """
    deadline_now = time.monotonic() if now is None else now
    expired = [
        api_key
        for api_key, deadline in _cooldowns.items()
        if deadline <= deadline_now
    ]
    for api_key in expired:
        _cooldowns.pop(api_key, None)


def is_key_rate_limited(api_key: str) -> bool:
    """True iff *api_key* is currently in cooldown.

    Side effect: drops *api_key*'s entry from the cooldown table
    when the deadline has passed (lazy expiry). The membership
    check is therefore monotonic — once a deadline elapses, every
    subsequent call sees the key as available.
    """
    if not api_key:
        return False
    deadline = _cooldowns.get(api_key)
    if deadline is None:
        return False
    if deadline <= time.monotonic():
        _cooldowns.pop(api_key, None)
        return False
    return True


def mark_key_rate_limited(
    api_key: str, retry_after_secs: float | None = None
) -> None:
    """Put *api_key* in cooldown for *retry_after_secs* seconds.

    Cooldown duration:

    * ``None`` (default) — :data:`DEFAULT_COOLDOWN_SECS` (60s).
    * Non-finite or non-positive — falls back to the default
      and logs a warning. (A negative Retry-After would unset
      the cooldown immediately; an Inf would pin the key out
      of rotation forever.)
    * Above :data:`MAX_COOLDOWN_SECS` — clamped down. A
      misbehaving CDN that sends ``Retry-After: 86400`` shouldn't
      lock a key out for a day.

    No-op if ``api_key`` is empty / not in the configured pool —
    we don't want to inflate the cooldown dict with junk that
    no caller will ever read back.
    """
    if not api_key:
        return
    _ensure_loaded()
    if api_key not in _keys:
        # Caller asked us to cool down a key the pool doesn't
        # know about — almost certainly a bug or a stale
        # reference (e.g. a delayed 429 for a key that was
        # rotated out via load_keys()). Don't silently grow the
        # cooldown table; log loud-and-once and ignore.
        log.warning(
            "mark_key_rate_limited called for an unknown key (len=%d). "
            "Ignoring; either the key was rotated out or the caller "
            "passed a stale reference.",
            len(api_key),
        )
        return
    secs: float
    if retry_after_secs is None:
        secs = DEFAULT_COOLDOWN_SECS
    else:
        try:
            candidate = float(retry_after_secs)
        except (TypeError, ValueError):
            log.warning(
                "mark_key_rate_limited received non-numeric "
                "retry_after_secs=%r; using default %.1fs.",
                retry_after_secs,
                DEFAULT_COOLDOWN_SECS,
            )
            candidate = DEFAULT_COOLDOWN_SECS
        # math.isfinite catches NaN / ±Inf; also reject negatives.
        if not math.isfinite(candidate) or candidate <= 0.0:
            log.warning(
                "mark_key_rate_limited received unusable "
                "retry_after_secs=%r; using default %.1fs.",
                retry_after_secs,
                DEFAULT_COOLDOWN_SECS,
            )
            secs = DEFAULT_COOLDOWN_SECS
        else:
            secs = min(candidate, MAX_COOLDOWN_SECS)

    deadline = time.monotonic() + secs
    # If a previous cooldown is still active and would extend
    # further out than the new one, KEEP the longer deadline —
    # we never want a fresh 429 with a small Retry-After to
    # *shorten* a still-running cooldown that came from a
    # bigger Retry-After. (OpenRouter sometimes sends two 429s
    # back-to-back with different windows.)
    existing = _cooldowns.get(api_key)
    if existing is None or existing < deadline:
        _cooldowns[api_key] = deadline
    log.warning(
        "OpenRouter key (len=%d) put in cooldown for %.1fs "
        "(pool size=%d, available=%d).",
        len(api_key),
        secs,
        len(_keys),
        available_key_count(),
    )


def available_key_count() -> int:
    """Number of pool keys that aren't in cooldown right now.

    Used by :func:`key_for_user` to decide whether to fall back
    to the sticky pick (when every key is rate-limited) and by
    the diagnostic snapshot below.
    """
    _ensure_loaded()
    if not _keys:
        return 0
    _drop_expired_cooldowns()
    return sum(1 for k in _keys if k not in _cooldowns)


def clear_all_cooldowns() -> None:
    """Wipe the cooldown table.

    Tests use this to start each case from a known state.
    Operators with a "force everything back online right now"
    button can call it to recover from an over-aggressive
    Retry-After without restarting the bot.
    """
    _cooldowns.clear()


def key_status_snapshot() -> list[dict[str, object]]:
    """Return one dict per pool key for diagnostics.

    Each dict has shape::

        {
            "index": int,                  # 0-based pool index
            "rate_limited": bool,          # in cooldown?
            "cooldown_remaining_secs": float | None,
        }

    The actual key string is **not** included — the snapshot is
    intended for ops dashboards / metrics, and leaking the key
    material into a render path that might end up in a log
    aggregator is a needless risk. Callers that genuinely need
    to correlate a snapshot row to a specific key can use the
    ``index`` field together with their own
    ``openrouter_keys._keys`` reference.
    """
    _ensure_loaded()
    _drop_expired_cooldowns()
    snapshot: list[dict[str, object]] = []
    now = time.monotonic()
    for idx, api_key in enumerate(_keys):
        deadline = _cooldowns.get(api_key)
        if deadline is None:
            snapshot.append(
                {
                    "index": idx,
                    "rate_limited": False,
                    "cooldown_remaining_secs": None,
                }
            )
        else:
            remaining = max(0.0, deadline - now)
            snapshot.append(
                {
                    "index": idx,
                    "rate_limited": True,
                    "cooldown_remaining_secs": remaining,
                }
            )
    return snapshot


def key_for_user(telegram_id: int) -> str:
    """Return the API key to use for *telegram_id*.

    Selection policy:

    1. Compute the user's sticky pool index (``telegram_id % N``).
    2. If the sticky key isn't in cooldown, return it.
    3. Otherwise, walk forward through the pool from the sticky
       index, returning the first key that isn't in cooldown.
    4. If **every** key is in cooldown, return the sticky pick
       anyway (with a warning) so the user gets at least an
       attempt rather than a hard "all keys exhausted" error.
       The caller will see another 429 and re-mark — but at
       least the user's request was made.

    Raises ``RuntimeError`` if no keys are configured.
    """
    _ensure_loaded()
    if not _keys:
        raise RuntimeError(
            "No OpenRouter API keys configured. Set OPENROUTER_API_KEY "
            "or OPENROUTER_API_KEY_1..10 in your environment."
        )
    _drop_expired_cooldowns()
    n = len(_keys)
    sticky_idx = telegram_id % n
    if _keys[sticky_idx] not in _cooldowns:
        return _keys[sticky_idx]
    # Sticky key is hot — walk forward through the pool. ``range
    # (1, n)`` skips the sticky offset itself (already checked
    # above), so we examine each *other* slot exactly once.
    for offset in range(1, n):
        idx = (sticky_idx + offset) % n
        if _keys[idx] not in _cooldowns:
            log.info(
                "Routing user %d off cooldown'd sticky key "
                "(idx=%d) to fallback idx=%d.",
                telegram_id,
                sticky_idx,
                idx,
            )
            return _keys[idx]
    # Every key is in cooldown. Best-effort: return the sticky
    # pick anyway so the request gets a chance. The caller will
    # see another 429 (or a 200 if the cooldown was conservative)
    # and either way we won't have silently dropped the request.
    log.warning(
        "All %d OpenRouter key(s) in cooldown — falling back to "
        "sticky pick (idx=%d) for user %d. The request may still "
        "be rate-limited.",
        n,
        sticky_idx,
        telegram_id,
    )
    return _keys[sticky_idx]


def key_count() -> int:
    """Number of keys in the pool."""
    _ensure_loaded()
    return len(_keys)
