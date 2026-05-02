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

# ── Per-key Prometheus counters (Stage-15-Step-E #4 follow-up) ────
#
# Two monotonically-increasing counters keyed by 0-based pool
# index. Indexed by *index* rather than by api_key string to keep
# the api_key material out of the rendered ``/metrics`` body — same
# discipline ``key_status_snapshot`` already follows. Index is
# stable across a single process lifetime (the pool only changes
# on a deliberate ``load_keys()`` call); a key rotation event
# resets the counters via ``_reset_key_counters_on_load`` to avoid
# carrying stale meaning from a key that no longer exists.
#
# * ``_429_total`` — incremented every time
#   :func:`mark_key_rate_limited` registers a fresh cooldown for
#   a given pool key. Re-marking an already-cooled key still
#   counts (each 429 is a separate event the operator wants to
#   see in the rate plot).
# * ``_fallback_total`` — incremented every time
#   :func:`key_for_user` would have returned the user's sticky
#   key but the sticky was in cooldown, so we walked forward to
#   a different pool member. Tracked against the *fallback* index
#   (the slot that absorbed the traffic), not the sticky index,
#   so a dashboard "fallback rate per key" plot answers the
#   question "which key is taking the load when others go hot".
#
# Public-but-private: read-back accessors expose both as plain
# dicts for ``metrics.render_metrics`` and tests; mutation goes
# through :func:`_increment_key_429` and
# :func:`_increment_key_fallback` so a future migration to a
# different counter backend (e.g. prometheus_client) only
# touches three call sites.
_KEY_429_COUNTERS: dict[int, int] = {}
_KEY_FALLBACK_COUNTERS: dict[int, int] = {}

# ── Stage-15-Step-E #4 follow-up #2: per-key request counter ──────
#
# Bumped every time :func:`key_for_user` returns a key — both for
# the sticky pick and the fallback walk. Lets the admin panel
# answer "how much of today's traffic is hitting each key?" without
# the heavier per-token / per-cost plumbing (a future PR can layer
# that on top of ``usage_logs``). Reset on every ``load_keys()``
# reload alongside the existing 429/fallback counters so a key
# rotation doesn't carry stale meaning forward.
_KEY_REQUEST_COUNTERS: dict[int, int] = {}

# ── Stage-15-Step-E #4 follow-up #2: per-key metadata ─────────────
#
# Parallel-indexed metadata for ``_keys``: ``_KEY_META[i]`` describes
# ``_keys[i]``. Populated by ``refresh_from_db`` (DB-loaded keys carry
# a label / db_id / source="db"); env-loaded keys get a stub entry
# ``{"source": "env"}``. This dict stays small (<= len(_keys)) and
# is rebuilt from scratch on every load so no eviction policy is
# needed.
_KEY_META: dict[int, dict[str, object]] = {}


def _increment_key_429(idx: int) -> None:
    """Bump the 429 counter for pool index *idx*.

    Tolerates a negative or out-of-range *idx* by silently
    no-oping — a future caller that resolves an index from a
    stale snapshot shouldn't be able to poison the counter
    table with a negative slot.
    """
    if idx < 0:
        return
    _KEY_429_COUNTERS[idx] = _KEY_429_COUNTERS.get(idx, 0) + 1


def _increment_key_fallback(idx: int) -> None:
    """Bump the fallback counter for pool index *idx*."""
    if idx < 0:
        return
    _KEY_FALLBACK_COUNTERS[idx] = _KEY_FALLBACK_COUNTERS.get(idx, 0) + 1


def get_key_429_counters() -> dict[int, int]:
    """Read-only snapshot of the per-key 429 counters.

    Returns a fresh ``dict`` so the caller can't mutate the
    in-process registry (``metrics.render_metrics`` iterates over
    this in a hot path; same shallow-copy discipline the IPN-drop
    accessors follow).
    """
    return dict(_KEY_429_COUNTERS)


def get_key_fallback_counters() -> dict[int, int]:
    """Read-only snapshot of the per-key fallback counters."""
    return dict(_KEY_FALLBACK_COUNTERS)


def _increment_key_request(idx: int) -> None:
    """Bump the per-key request counter for pool index *idx*."""
    if idx < 0:
        return
    _KEY_REQUEST_COUNTERS[idx] = _KEY_REQUEST_COUNTERS.get(idx, 0) + 1


def get_key_request_counters() -> dict[int, int]:
    """Read-only snapshot of the per-key request counters."""
    return dict(_KEY_REQUEST_COUNTERS)


def reset_key_counters_for_tests() -> None:
    """Tests-only — wipe all per-key counter dicts.

    Production paths never call this; tests use it to start each
    case from a known zero state. Mirrors
    ``clear_all_cooldowns`` for the cooldown table.
    """
    _KEY_429_COUNTERS.clear()
    _KEY_FALLBACK_COUNTERS.clear()
    _KEY_REQUEST_COUNTERS.clear()


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
    # The per-key counters are keyed by pool index. A reload that
    # changes pool composition (numbered → bare, or a key
    # rotated out) makes the old indices ambiguous: idx 0 used to
    # mean "first numbered key", but after the reload it means
    # "the bare key" — keeping the old counts would mislabel the
    # dashboard's per-key plot. Cheap to reset; production reloads
    # are rare (tests, manual hot-reload).
    _KEY_429_COUNTERS.clear()
    _KEY_FALLBACK_COUNTERS.clear()
    _KEY_REQUEST_COUNTERS.clear()
    _KEY_META.clear()
    # Tag every env-loaded slot so the panel can render a "source"
    # column even on env-only deploys.
    for i in range(len(_keys)):
        _KEY_META[i] = {"source": "env"}
    # Bundled bug fix (Stage-15-Step-E #4 follow-up #2): drop any
    # cooldown entries whose api_key is no longer in the new pool.
    # Pre-fix, ``load_keys()`` left stale cooldown entries in
    # ``_cooldowns`` after a hot key rotation — the entries
    # referenced api_key strings that no longer existed in
    # ``_keys`` and could only ever expire passively via
    # :func:`_drop_expired_cooldowns`. Within the cap of
    # :data:`MAX_COOLDOWN_SECS` (1 h) the table self-cleaned, but
    # while they sat there the cooldown dict's size was no longer
    # bounded by ``len(_keys)`` (the invariant the comment near
    # ``_cooldowns``'s definition explicitly promises). On a tight
    # rotation cycle (operator script that swaps keys every minute
    # to dodge upstream throttling) the cooldown table grew
    # unbounded for the first hour after every swap before
    # eventually settling. The fix prunes any entry whose key
    # string isn't in the freshly-loaded ``_keys`` list — the
    # remaining cooldowns are still legitimate (their key is
    # still in the pool, the cooldown still has time on its
    # deadline). Tested in
    # ``test_load_keys_evicts_stale_cooldown_entries``.
    pool = set(_keys)
    stale = [k for k in _cooldowns if k not in pool]
    for k in stale:
        _cooldowns.pop(k, None)


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
    # Increment the per-key 429 counter against the pool index
    # of *this* api_key. ``index`` lookup is O(N) but N <= 10
    # so it doesn't matter; we resolve here so the counter
    # stays index-keyed (key string never leaves this module).
    try:
        idx = _keys.index(api_key)
    except ValueError:  # pragma: no cover — guarded above
        idx = -1
    _increment_key_429(idx)
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
        _increment_key_request(sticky_idx)
        return _keys[sticky_idx]
    # Sticky key is hot — walk forward through the pool. ``range
    # (1, n)`` skips the sticky offset itself (already checked
    # above), so we examine each *other* slot exactly once.
    for offset in range(1, n):
        idx = (sticky_idx + offset) % n
        if _keys[idx] not in _cooldowns:
            # Record which pool slot absorbed the fallback so a
            # "fallback rate per key" dashboard plot answers
            # "which key is taking the load when others go hot".
            # The sticky idx is logged below for the same incident
            # — the pair lets the operator correlate sticky→fallback
            # routes if needed.
            _increment_key_fallback(idx)
            _increment_key_request(idx)
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
    _increment_key_request(sticky_idx)
    return _keys[sticky_idx]


# ── Stage-15-Step-E #4 follow-up #2: DB-backed key registry ───────


async def refresh_from_db(db) -> int:
    """(Re-)load the pool from env + the DB-backed registry.

    Reads the current env keys (via the same parser
    :func:`load_keys` uses) plus every enabled row from the
    ``openrouter_api_keys`` table, then computes the desired
    pool. If the resulting pool *exactly matches* the current
    in-process pool (same keys, same indices) the call is a
    no-op — counters and cooldowns survive untouched. This is
    what the admin GET path leans on so a page reload doesn't
    wipe the per-key request / 429 counters between visits.

    If the pool differs (a DB row was added / removed / a
    label changed) the function rebuilds the pool from scratch
    via :func:`load_keys` (which resets counters — matching the
    discipline the existing 429/fallback counters already
    follow on a key rotation) and then re-appends DB rows.

    Returns the total pool size after the refresh.

    *db* must expose
    ``list_enabled_openrouter_keys_with_secret()`` returning a
    list of ``{id, label, api_key}`` dicts. A ``None`` *db*
    triggers a pure env load.

    The whole call is wrapped so a transient DB error doesn't
    blank the env-loaded pool: on failure the env pool stays in
    place and the caller logs the exception.
    """
    global _keys, _loaded

    if db is None:
        load_keys()
        return len(_keys)

    try:
        rows = await db.list_enabled_openrouter_keys_with_secret()
    except Exception:
        log.exception(
            "openrouter_keys: refresh_from_db failed; keeping env pool"
        )
        if not _loaded:
            load_keys()
        return len(_keys)
    if not isinstance(rows, list):
        log.warning(
            "openrouter_keys: list_enabled_openrouter_keys_with_secret "
            "returned %r (not a list); keeping env pool",
            type(rows).__name__,
        )
        if not _loaded:
            load_keys()
        return len(_keys)

    # Build the desired pool: env keys (in current order) + DB
    # keys (in id order, dedup'd against env).
    env_keys = _read_env_keys()
    desired: list[str] = list(env_keys)
    desired_meta: list[dict[str, object]] = [
        {"source": "env"} for _ in env_keys
    ]
    env_set = set(desired)
    for row in rows:
        if not isinstance(row, dict):
            log.warning(
                "openrouter_keys: skipping non-dict row %r",
                type(row).__name__,
            )
            continue
        api_key = row.get("api_key")
        if not isinstance(api_key, str) or not api_key.strip():
            log.warning(
                "openrouter_keys: skipping DB row id=%r with empty api_key",
                row.get("id"),
            )
            continue
        api_key = api_key.strip()
        if api_key in env_set:
            log.warning(
                "openrouter_keys: skipping DB row id=%r — its api_key "
                "is already loaded from env.",
                row.get("id"),
            )
            continue
        env_set.add(api_key)
        desired.append(api_key)
        label = row.get("label")
        db_id = row.get("id")
        desired_meta.append(
            {
                "source": "db",
                "label": str(label) if label is not None else None,
                "db_id": int(db_id) if db_id is not None else None,
            }
        )

    # No-op fast path: the in-process pool already matches.
    # Preserves counters across page loads.
    current_meta = [_KEY_META.get(i, {"source": "env"}) for i in range(len(_keys))]
    if _loaded and _keys == desired and current_meta == desired_meta:
        return len(_keys)

    # Pool differs — rebuild from scratch. ``load_keys`` resets
    # env-side state (and counters); we then append the DB tail.
    load_keys()
    for i, key in enumerate(desired[len(_keys):], start=len(_keys)):
        _keys.append(key)
        _KEY_META[i] = desired_meta[i]
    log.info(
        "openrouter_keys: refresh_from_db rebuilt pool to size=%d "
        "(env=%d, db=%d)",
        len(_keys),
        len(env_keys),
        len(desired) - len(env_keys),
    )
    return len(_keys)


def _read_env_keys() -> list[str]:
    """Return the env-configured key list in canonical order.

    Mirrors the env-parsing branch of :func:`load_keys` but
    without the side-effects (no global mutation, no counter
    reset). Used by :func:`refresh_from_db` to compute the
    "desired pool" before deciding whether a rebuild is needed.
    """
    keys: list[str] = []
    primary = os.getenv("OPENROUTER_API_KEY", "").strip()
    if primary:
        keys.append(primary)
    for n in range(1, 11):
        candidate = os.getenv(f"OPENROUTER_API_KEY_{n}", "").strip()
        if candidate and candidate not in keys:
            keys.append(candidate)
    return keys


def get_key_meta_snapshot() -> list[dict[str, object]]:
    """Read-only copy of the per-pool-index metadata table.

    Returns a list of length ``len(_keys)`` where index ``i``
    matches ``_keys[i]``. Each entry is a dict with at least
    ``source`` (``"env"`` or ``"db"``); DB-loaded entries also
    have ``label`` and ``db_id``.

    Defensive against a partially-populated meta table (e.g. a
    test that monkeypatches ``_keys`` directly without going
    through ``load_keys``): missing entries fall back to
    ``{"source": "env"}``.
    """
    _ensure_loaded()
    return [
        dict(_KEY_META.get(i, {"source": "env"}))
        for i in range(len(_keys))
    ]


def key_count() -> int:
    """Number of keys in the pool."""
    _ensure_loaded()
    return len(_keys)
