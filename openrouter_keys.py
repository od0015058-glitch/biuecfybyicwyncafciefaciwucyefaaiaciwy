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

Stage-15-Step-E #4 follow-up #4 — per-(key, model) cooldown:

The first slice cooled the **whole key** on every 429. That was
over-aggressive: OpenRouter typically 429s a *specific* model
(usually a ``:free`` slug whose upstream provider is throttling
the burst), not the API key itself. Cooling the whole key
blocked every other model on that key — including paid models
the user was actively spending real money on — for the entire
60s window. A user routed to the cooled key paying for
``anthropic/claude-3.5-sonnet`` got an unhelpful
``ai_provider_unavailable`` reply because someone *else* (or
even the same user a moment ago) had hit a free-tier limit on
``google/gemini-2.0-flash-exp:free``.

The follow-up adds a **second cooldown table keyed by**
``(api_key, model_id)`` alongside the existing whole-key table.
The call site (``ai_engine.chat_with_model``) now passes
``model=active_model`` through to both
:func:`mark_key_rate_limited` and :func:`key_for_user`. A 429
caused by a specific model goes into the per-(key, model) table
and only blocks *that* model on *that* key — every other model
on the same key keeps serving. The whole-key table is reserved
for cases where the call site has no model context (or wants to
deliberately blacklist the whole key, e.g. a 401 / 403 from an
expired key).

Membership semantics: :func:`is_key_rate_limited` returns True
if **either** the whole-key cooldown is active **or**, when
``model=`` is provided, the per-(key, model) cooldown is
active. The picker (:func:`key_for_user`) then walks past any
slot for which ``is_key_rate_limited(slot, model=model)`` is
True, just like the first slice did with the whole-key table.

Both tables share the same ``DEFAULT_COOLDOWN_SECS`` /
``MAX_COOLDOWN_SECS`` semantics, the same ``Retry-After``
parsing rules, the same lazy expiry on read, and the same
"keep the longer existing deadline" behaviour. The pruning
helpers (:func:`_drop_expired_cooldowns`, the ``load_keys``
stale-eviction sweep, :func:`clear_all_cooldowns`) operate on
both tables in lockstep so the per-model entries never
out-live the whole-key entries' rotation discipline.

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
  ``retry_after_secs`` (default 60). Pass ``model="<slug>"`` to
  cool only the (key, model) pair instead of the whole key.
* :func:`is_key_rate_limited` — check if a key is currently in
  cooldown. Pass ``model="<slug>"`` to also consult the
  per-(key, model) table. Useful for tests and ops dashboards.
* :func:`available_key_count` — number of keys that aren't in
  cooldown right now. Pass ``model="<slug>"`` for "available
  for this specific model" rather than the global tally.
* :func:`key_status_snapshot` — per-key dict for diagnostics
  (admin panel / metrics). Pass ``model="<slug>"`` to fold the
  per-(key, model) cooldown into the per-slot view.
* :func:`per_model_cooldown_snapshot` — every active
  per-(key, model) cooldown row. Used by the admin panel and
  the Prometheus exposition.
* :func:`clear_all_cooldowns` — wipe BOTH the whole-key and the
  per-(key, model) cooldown tables. Tests + ops "force everything
  back online" recovery.
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

# Per-(key, model) cooldown table: ``(api_key, model_id) ->
# deadline_monotonic_seconds``. Stage-15-Step-E #4 follow-up #4.
# Populated when :func:`mark_key_rate_limited` is called with an
# explicit ``model=`` kwarg (the call site has model context for
# the 429). Entries are pruned by the same lazy-expiry discipline
# the whole-key ``_cooldowns`` table follows. Bounded in the
# worst case by ``len(_keys) * len(distinct_models_in_use)``;
# in practice most deploys have <= 5 keys and <= 20 active
# models so the table stays well under a hundred entries even at
# saturation.
_per_model_cooldowns: dict[tuple[str, str], float] = {}

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

# ── Stage-15-Step-E #4 follow-up #5: one-shot retry on 429 ────────
#
# Aggregate (pool-wide, NOT per-key) counter for the one-shot retry
# path :func:`ai_engine.chat_with_model` runs after a 429 from the
# initially-picked key. Outcome labels:
#
# * ``"attempted"`` — every 429 that triggered a retry (regardless
#   of how the retry ended). The denominator for retry-success rate
#   plots; ``succeeded / attempted`` is the headline metric.
# * ``"succeeded"`` — retry POST returned a 200 with a parseable
#   chat-completion body. The user got an AI reply on the second
#   try; the only operator-visible signal is the bumped counter.
# * ``"second_429"`` — retry POST also returned 429. The user gets
#   the existing ``ai_rate_limited`` / ``ai_rate_limited_free``
#   localised string. The retry burned one extra POST request
#   against the alternate key but didn't help — this counter
#   measures "is the rate-limit pool-wide or per-key?".
# * ``"second_other_status"`` — retry POST returned a non-200,
#   non-429 (a 5xx, a 401 from a key that just got revoked, etc).
#   The user sees ``ai_provider_unavailable``; the retry didn't
#   recover but at least we surfaced the alternate-key error.
# * ``"transport_error"`` — retry POST raised a ``ClientError`` /
#   ``TimeoutError`` / similar. The user sees the existing outer
#   ``ai_transient_error`` (the exception is re-raised after the
#   counter bump).
# * ``"no_alternate_key"`` — 429 from the only key in the pool
#   (or every alternate is also in cooldown for this model). No
#   retry attempted; the counter is bumped so an operator with a
#   single-key deploy who's been hitting 429s can correlate the
#   counter against their pool size and decide whether to add a
#   second key.
#
# Keyed by the outcome label string (a small fixed alphabet) so a
# Prometheus histogram-style "retry funnel" plot can be rendered
# without a labelled counter family for each outcome. Reset on
# ``load_keys()`` and ``reset_key_counters_for_tests`` like the
# per-key counters.
_ONE_SHOT_RETRY_COUNTERS: dict[str, int] = {}

_ONE_SHOT_RETRY_OUTCOMES: tuple[str, ...] = (
    "attempted",
    "succeeded",
    "second_429",
    "second_other_status",
    "transport_error",
    "no_alternate_key",
)

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


def _increment_oneshot_retry(outcome: str) -> None:
    """Bump the pool-wide one-shot-retry counter for *outcome*.

    Tolerates an unknown *outcome* label by silently no-oping —
    a future caller passing a typoed label shouldn't poison the
    counter table with an unindexed key. The fixed alphabet is
    pinned in :data:`_ONE_SHOT_RETRY_OUTCOMES` and the label is
    validated against it.
    """
    if outcome not in _ONE_SHOT_RETRY_OUTCOMES:
        return
    _ONE_SHOT_RETRY_COUNTERS[outcome] = (
        _ONE_SHOT_RETRY_COUNTERS.get(outcome, 0) + 1
    )


def get_oneshot_retry_counters() -> dict[str, int]:
    """Read-only snapshot of the one-shot-retry outcome counters.

    Returns a fresh ``dict`` keyed by outcome label
    (``"attempted"`` / ``"succeeded"`` / ``"second_429"`` /
    ``"second_other_status"`` / ``"transport_error"`` /
    ``"no_alternate_key"``). Outcomes that have never fired are
    absent — the metrics layer renders absent labels as zero
    counters so a deploy that's never seen a 429 retry doesn't
    show empty rows in the exposition.
    """
    return dict(_ONE_SHOT_RETRY_COUNTERS)


# ── Stage-15-Step-E #4 follow-up #3: per-key 24h usage tracker ────
#
# Rolling buffer of ``(timestamp, cost_usd)`` per pool index. Lets
# the admin panel answer "how much traffic — and how much $ — is
# this key handling right now?" without a Prometheus stack. The
# existing ``_KEY_REQUEST_COUNTERS`` is process-start-relative
# (resets on restart, no time window); this tracker maintains a
# 24-hour window that survives normal runtime variation.
#
# Buffer shape: ``{idx: list[(timestamp_seconds, cost_usd)]}``.
# Append on every ``record_key_usage`` call. Trim on every read
# (lazy expiry — simpler than a background sweeper, and the panel
# is the only reader so the trim cost is paid by the operator,
# not the hot AI path). A safety cap of
# ``_KEY_USAGE_MAX_ENTRIES`` per index protects against runaway
# growth on a buggy caller (or a real, sustained 1000 RPS to a
# single key for 24h, which wouldn't fit anyway).
#
# Reset on ``load_keys()`` alongside the existing counters so a
# key rotation doesn't carry stale meaning forward — the new
# pool member at index N has no history under that index.
_KEY_USAGE_BUCKETS: dict[int, list[tuple[float, float]]] = {}
_KEY_USAGE_WINDOW_SECONDS: float = 86_400.0
_KEY_USAGE_MAX_ENTRIES: int = 100_000


def _idx_for_api_key(api_key: str) -> int | None:
    """Reverse-lookup ``_keys`` by api_key string, or ``None``.

    O(N) walk (N ≤ 10 in practice) so no need for a separate
    index. Returns ``None`` for an api_key that isn't in the
    current pool — a stale reference (key rotated out between
    request start and finish) won't poison the usage buffer
    with a phantom index.
    """
    if not api_key:
        return None
    try:
        return _keys.index(api_key)
    except ValueError:
        return None


def _record_usage_at_idx(
    idx: int, cost_usd: float, ts: float | None = None,
) -> None:
    """Append a usage entry for pool index *idx*.

    Internal helper: callers go through :func:`record_key_usage`
    so we always reverse-lookup from the api_key (keeps the
    panel index-keyed; matches the existing 429/fallback
    counters' discipline). Tolerates ``cost_usd`` being NaN /
    -Inf / +Inf by coercing to 0.0 — a poisoned cost shouldn't
    permanently corrupt the 24h sum the panel renders.
    """
    if idx < 0:
        return
    timestamp = time.time() if ts is None else ts
    safe_cost: float = 0.0
    try:
        c = float(cost_usd)
    except (TypeError, ValueError):
        c = 0.0
    if math.isfinite(c) and c >= 0.0:
        safe_cost = c
    bucket = _KEY_USAGE_BUCKETS.setdefault(idx, [])
    bucket.append((timestamp, safe_cost))
    # Safety cap: keep the most recent ``_KEY_USAGE_MAX_ENTRIES``
    # entries. The window-trim in :func:`get_key_24h_usage` will
    # also evict expired entries on read — the cap is just a
    # defensive belt for the worst case where reads don't happen
    # frequently enough to keep the list small.
    if len(bucket) > _KEY_USAGE_MAX_ENTRIES:
        # Drop the oldest 10% to amortise the eviction cost so
        # we're not popping(0) on every append once we hit the cap.
        drop = max(1, len(bucket) - _KEY_USAGE_MAX_ENTRIES + (
            _KEY_USAGE_MAX_ENTRIES // 10
        ))
        del bucket[:drop]


async def record_key_usage(
    api_key: str, cost_usd: float, *, db=None,
) -> None:
    """Record one successful OpenRouter call against *api_key*.

    Bumps the in-process 24h rolling buffer for the pool index
    matching *api_key* (silently no-ops if the key isn't in the
    current pool — see :func:`_idx_for_api_key`).

    If *db* is supplied AND the key was loaded from the DB-backed
    registry (``_KEY_META[idx]["source"] == "db"``), also bumps
    ``last_used_at`` on the matching DB row via
    ``db.mark_openrouter_key_used``. Pre-fix, the registry's
    ``last_used_at`` column was never updated — the panel's
    "Last used" column always rendered ``—`` even for actively-
    used keys. Bundled bug fix.

    DB error handling: a transient DB blip on the
    ``mark_openrouter_key_used`` side is logged and swallowed so
    the user-facing AI reply isn't blocked by a slow
    ``last_used_at`` UPDATE. The in-memory 24h buffer is
    populated regardless.
    """
    idx = _idx_for_api_key(api_key)
    if idx is None:
        return
    _record_usage_at_idx(idx, cost_usd)

    if db is None:
        return
    meta = _KEY_META.get(idx, {})
    if meta.get("source") != "db":
        return
    db_id = meta.get("db_id")
    if not isinstance(db_id, int):
        return
    try:
        await db.mark_openrouter_key_used(db_id)
    except Exception:
        log.exception(
            "openrouter_keys.record_key_usage: "
            "mark_openrouter_key_used(%d) failed; in-memory 24h "
            "buffer was still updated.",
            db_id,
        )


def get_key_24h_usage() -> dict[int, dict[str, float]]:
    """24h-windowed per-key usage snapshot.

    Returns ``{idx: {"requests": int, "cost_usd": float}}`` for
    every pool index that has at least one entry in the rolling
    buffer. Indices with zero entries are omitted — callers
    (the panel) default to zero for missing indices.

    Side effect: trims expired entries (older than 24h) on each
    call. Lazy expiry — see the buffer's docstring above.
    Empty-but-stale buckets are also evicted so the dict stays
    bounded by the active key set rather than the historical key
    set.
    """
    cutoff = time.time() - _KEY_USAGE_WINDOW_SECONDS
    result: dict[int, dict[str, float]] = {}
    stale_idxs: list[int] = []
    for idx, entries in _KEY_USAGE_BUCKETS.items():
        # Locate the first non-expired entry. Buffer is append-only
        # in monotonic-ish time order, so a simple scan from the
        # front finds the cutoff. (We don't bisect because the
        # safety cap already keeps the list bounded; bisect would
        # only matter past 1M entries.)
        keep_from = 0
        for ts, _cost in entries:
            if ts >= cutoff:
                break
            keep_from += 1
        if keep_from > 0:
            del entries[:keep_from]
        if not entries:
            stale_idxs.append(idx)
            continue
        total_cost = 0.0
        for _ts, cost in entries:
            total_cost += cost
        result[idx] = {
            "requests": float(len(entries)),
            "cost_usd": total_cost,
        }
    for idx in stale_idxs:
        _KEY_USAGE_BUCKETS.pop(idx, None)
    return result


def reset_key_counters_for_tests() -> None:
    """Tests-only — wipe all per-key counter dicts.

    Production paths never call this; tests use it to start each
    case from a known zero state. Mirrors
    ``clear_all_cooldowns`` for the cooldown table.
    """
    _KEY_429_COUNTERS.clear()
    _KEY_FALLBACK_COUNTERS.clear()
    _KEY_REQUEST_COUNTERS.clear()
    _KEY_USAGE_BUCKETS.clear()
    _ONE_SHOT_RETRY_COUNTERS.clear()


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
    _KEY_USAGE_BUCKETS.clear()
    _ONE_SHOT_RETRY_COUNTERS.clear()
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
    # Stage-15-Step-E #4 follow-up #4: same eviction discipline for
    # the per-(key, model) cooldown table. A pool rotation that
    # drops api_key X from ``_keys`` should evict every
    # ``(X, model)`` entry in lockstep so the per-model dict
    # stays bounded by ``len(_keys) * len(distinct_models)``.
    stale_pairs = [
        pair for pair in _per_model_cooldowns if pair[0] not in pool
    ]
    for pair in stale_pairs:
        _per_model_cooldowns.pop(pair, None)


def _ensure_loaded() -> None:
    """Trigger lazy-load if it hasn't run yet this process lifetime."""
    if not _loaded:
        load_keys()


def _normalise_model(model: str | None) -> str | None:
    """Return a stripped, non-empty model id or ``None``.

    Stage-15-Step-E #4 follow-up #4. The per-(key, model) cooldown
    table keys on the model id verbatim, so we want to canonicalise
    the input *once* at the entry points and then trust the
    canonical form everywhere else. Whitespace-only and ``None``
    inputs collapse to ``None`` which the call sites read as
    "no model context — fall back to whole-key cooldown" and is
    semantically equivalent to "the caller never passed a model".

    NOT lower-cased: OpenRouter model ids are case-sensitive
    (``openrouter/auto`` vs ``openrouter/Auto`` are different
    routes — the first is the autopicker, the second 404s) so
    folding case here would silently merge two distinct slugs
    into one cooldown entry.
    """
    if model is None:
        return None
    if not isinstance(model, str):
        return None
    stripped = model.strip()
    if not stripped:
        return None
    return stripped


def _drop_expired_cooldowns(now: float | None = None) -> None:
    """Prune any cooldown entries whose deadline has elapsed.

    Called from the membership-test paths so the cooldown table
    self-cleans without needing a background sweeper task. ``now``
    is parameterised purely for tests; production code passes
    ``None`` and the function reads ``time.monotonic()`` itself.

    Stage-15-Step-E #4 follow-up #4: also prunes the
    per-(key, model) cooldown table in the same call so the two
    tables stay in lockstep. A test that wants to drive only one
    side at a time can use :func:`clear_all_cooldowns` followed
    by a fresh :func:`mark_key_rate_limited`.
    """
    deadline_now = time.monotonic() if now is None else now
    expired = [
        api_key
        for api_key, deadline in _cooldowns.items()
        if deadline <= deadline_now
    ]
    for api_key in expired:
        _cooldowns.pop(api_key, None)
    expired_pairs = [
        pair
        for pair, deadline in _per_model_cooldowns.items()
        if deadline <= deadline_now
    ]
    for pair in expired_pairs:
        _per_model_cooldowns.pop(pair, None)


def is_key_rate_limited(api_key: str, *, model: str | None = None) -> bool:
    """True iff *api_key* is currently in cooldown.

    With ``model=None`` (the default — back-compat for callers
    that haven't been updated) only the whole-key cooldown table
    is consulted. With ``model="<slug>"`` the call returns True
    if **either** the whole-key cooldown is active **or** the
    per-(api_key, model) cooldown is active — the picker treats
    a per-model block the same as a whole-key block from its
    perspective (the slot can't serve this user's current model
    so walk to the next one).

    Side effect: drops the matching entries from each table when
    their deadline has passed (lazy expiry). The membership check
    is therefore monotonic — once a deadline elapses, every
    subsequent call sees that table as available for this lookup.
    """
    if not api_key:
        return False
    now = time.monotonic()
    deadline = _cooldowns.get(api_key)
    if deadline is not None:
        if deadline <= now:
            _cooldowns.pop(api_key, None)
        else:
            return True
    norm_model = _normalise_model(model)
    if norm_model is None:
        return False
    pair = (api_key, norm_model)
    pair_deadline = _per_model_cooldowns.get(pair)
    if pair_deadline is None:
        return False
    if pair_deadline <= now:
        _per_model_cooldowns.pop(pair, None)
        return False
    return True


def mark_key_rate_limited(
    api_key: str,
    retry_after_secs: float | None = None,
    *,
    model: str | None = None,
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

    Cooldown scope (Stage-15-Step-E #4 follow-up #4):

    * ``model=None`` (default) — the **whole key** is cooled.
      Every model routed to this key is blocked for the duration.
      Used when the call site has no model context, or the 429
      / 401 / 403 isn't tied to a specific model (e.g. the key
      itself is exhausted / revoked).
    * ``model="<slug>"`` — only **(api_key, slug)** is cooled.
      Other models on the same key keep serving. This is the
      common case: OpenRouter typically 429s a specific
      ``:free`` model whose upstream provider is throttling,
      not the API key as a whole.

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
    norm_model = _normalise_model(model)
    if norm_model is None:
        # Whole-key cooldown path (back-compat).
        # If a previous cooldown is still active and would extend
        # further out than the new one, KEEP the longer deadline —
        # we never want a fresh 429 with a small Retry-After to
        # *shorten* a still-running cooldown that came from a
        # bigger Retry-After. (OpenRouter sometimes sends two 429s
        # back-to-back with different windows.)
        existing = _cooldowns.get(api_key)
        if existing is None or existing < deadline:
            _cooldowns[api_key] = deadline
    else:
        # Per-(key, model) cooldown path. Same "keep the longer
        # deadline" discipline as the whole-key table — back-to-back
        # 429s for the same model with different Retry-After
        # windows shouldn't shorten the lockout.
        pair = (api_key, norm_model)
        existing_pair = _per_model_cooldowns.get(pair)
        if existing_pair is None or existing_pair < deadline:
            _per_model_cooldowns[pair] = deadline
    # Increment the per-key 429 counter against the pool index
    # of *this* api_key. ``index`` lookup is O(N) but N <= 10
    # so it doesn't matter; we resolve here so the counter
    # stays index-keyed (key string never leaves this module).
    # The 429 counter aggregates across whole-key and per-model
    # cooldowns — a 429 is a 429 from the per-key 429-rate
    # alerting POV, regardless of which table absorbed it.
    try:
        idx = _keys.index(api_key)
    except ValueError:  # pragma: no cover — guarded above
        idx = -1
    _increment_key_429(idx)
    if norm_model is None:
        log.warning(
            "OpenRouter key (len=%d) put in cooldown for %.1fs "
            "(pool size=%d, available=%d).",
            len(api_key),
            secs,
            len(_keys),
            available_key_count(),
        )
    else:
        log.warning(
            "OpenRouter (key len=%d, model=%r) put in cooldown for "
            "%.1fs (pool size=%d, available_for_model=%d).",
            len(api_key),
            norm_model,
            secs,
            len(_keys),
            available_key_count(model=norm_model),
        )


def available_key_count(*, model: str | None = None) -> int:
    """Number of pool keys that aren't in cooldown right now.

    Used by :func:`key_for_user` to decide whether to fall back
    to the sticky pick (when every key is rate-limited) and by
    the diagnostic snapshot below.

    With ``model="<slug>"`` the count reflects availability for
    *that specific model* — a slot blocked by the per-(key, model)
    table is excluded, but a slot only cooled for a *different*
    model on the same key is still counted as available. Stage-15
    -Step-E #4 follow-up #4 — used by ``ai_engine.chat_with_model``
    so the user-facing "all keys cooled" warning fires only when
    the *user's actual model* really has no available slot.
    """
    _ensure_loaded()
    if not _keys:
        return 0
    _drop_expired_cooldowns()
    norm_model = _normalise_model(model)
    if norm_model is None:
        return sum(1 for k in _keys if k not in _cooldowns)
    return sum(
        1
        for k in _keys
        if k not in _cooldowns
        and (k, norm_model) not in _per_model_cooldowns
    )


def clear_all_cooldowns() -> None:
    """Wipe the cooldown table.

    Tests use this to start each case from a known state.
    Operators with a "force everything back online right now"
    button can call it to recover from an over-aggressive
    Retry-After without restarting the bot.

    Stage-15-Step-E #4 follow-up #4: also wipes the
    per-(key, model) cooldown table so the ops "force back online"
    button doesn't leave a half-cleared state where individual
    (key, model) pairs are still locked while the whole-key
    side has been cleared.
    """
    _cooldowns.clear()
    _per_model_cooldowns.clear()


def key_status_snapshot(
    *, model: str | None = None
) -> list[dict[str, object]]:
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

    With ``model="<slug>"`` the snapshot reflects availability
    *for that specific model*. ``rate_limited`` is True when
    either the whole-key cooldown is active OR the (key, model)
    cooldown is active. ``cooldown_remaining_secs`` is the
    larger of the two (whichever expires later — that's when
    the slot becomes usable for this model). Stage-15-Step-E #4
    follow-up #4.
    """
    _ensure_loaded()
    _drop_expired_cooldowns()
    snapshot: list[dict[str, object]] = []
    now = time.monotonic()
    norm_model = _normalise_model(model)
    for idx, api_key in enumerate(_keys):
        whole_deadline = _cooldowns.get(api_key)
        pair_deadline = (
            _per_model_cooldowns.get((api_key, norm_model))
            if norm_model is not None
            else None
        )
        # Pick whichever deadline is later — that's the time at
        # which the slot becomes usable for the requested model.
        deadline: float | None
        if whole_deadline is None and pair_deadline is None:
            deadline = None
        elif whole_deadline is None:
            deadline = pair_deadline
        elif pair_deadline is None:
            deadline = whole_deadline
        else:
            deadline = max(whole_deadline, pair_deadline)
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


def per_model_cooldown_snapshot() -> list[dict[str, object]]:
    """Return one dict per active (key, model) cooldown for diagnostics.

    Stage-15-Step-E #4 follow-up #4. Used by the
    ``/admin/openrouter-keys`` panel to render the per-(key, model)
    cooldown table alongside the existing whole-key view, and by
    the Prometheus exposition to emit the new
    ``meowassist_openrouter_key_model_cooldown_remaining_seconds``
    labelled-gauge family.

    Each dict has shape::

        {
            "index": int,                       # pool index of the key
            "model": str,                       # OpenRouter model slug
            "cooldown_remaining_secs": float,   # always > 0 (expired
                                                # entries are pruned
                                                # before the snapshot)
        }

    Entries for keys no longer in the pool (rotated out between
    the cooldown landing and the snapshot read) are filtered
    out — they'd be misleading to render against an index that
    no longer exists. Same discipline as
    :func:`get_key_24h_usage`.
    """
    _ensure_loaded()
    _drop_expired_cooldowns()
    snapshot: list[dict[str, object]] = []
    now = time.monotonic()
    # Build a quick api_key -> idx lookup so the O(per_model_pairs)
    # render isn't O(per_model_pairs * len(_keys)) for the index
    # resolution.
    idx_by_key = {k: i for i, k in enumerate(_keys)}
    for (api_key, model_id), deadline in _per_model_cooldowns.items():
        if deadline <= now:
            # Defence-in-depth: _drop_expired_cooldowns above
            # should have pruned it but double-check so we don't
            # render a row with cooldown_remaining_secs == 0.0.
            continue
        idx = idx_by_key.get(api_key)
        if idx is None:
            continue
        snapshot.append(
            {
                "index": idx,
                "model": model_id,
                "cooldown_remaining_secs": max(0.0, deadline - now),
            }
        )
    # Sort by (index, model) for deterministic rendering in tests
    # and for stable Prometheus label ordering across scrapes.
    snapshot.sort(key=lambda row: (row["index"], row["model"]))
    return snapshot


def key_for_user(telegram_id: int, *, model: str | None = None) -> str:
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

    With ``model="<slug>"`` (Stage-15-Step-E #4 follow-up #4)
    "in cooldown" expands to "whole-key cooldown active OR
    per-(key, model) cooldown active for *this* model". A slot
    cooled for a *different* model on the same key is treated
    as available — the user's current model has nothing to do
    with that other model's 429 history.

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
    norm_model = _normalise_model(model)
    sticky_idx = telegram_id % n

    def _slot_available(slot_idx: int) -> bool:
        """Cooldown-aware check for a single pool slot.

        Lifted out of the inline ``in _cooldowns`` checks so the
        sticky-pick and the fallback-walk paths share the exact
        same logic — keeping a slot "available for this user's
        current model" predicate consistent across both branches.
        """
        api_key = _keys[slot_idx]
        if api_key in _cooldowns:
            return False
        if (
            norm_model is not None
            and (api_key, norm_model) in _per_model_cooldowns
        ):
            return False
        return True

    if _slot_available(sticky_idx):
        _increment_key_request(sticky_idx)
        return _keys[sticky_idx]
    # Sticky key is hot — walk forward through the pool. ``range
    # (1, n)`` skips the sticky offset itself (already checked
    # above), so we examine each *other* slot exactly once.
    for offset in range(1, n):
        idx = (sticky_idx + offset) % n
        if _slot_available(idx):
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
                "(idx=%d) to fallback idx=%d (model=%r).",
                telegram_id,
                sticky_idx,
                idx,
                norm_model,
            )
            return _keys[idx]
    # Every key is in cooldown. Best-effort: return the sticky
    # pick anyway so the request gets a chance. The caller will
    # see another 429 (or a 200 if the cooldown was conservative)
    # and either way we won't have silently dropped the request.
    log.warning(
        "All %d OpenRouter key(s) in cooldown for model=%r — falling "
        "back to sticky pick (idx=%d) for user %d. The request may "
        "still be rate-limited.",
        n,
        norm_model,
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

    Bug fix (Stage-15-Step-F follow-up #5): the prior implementation
    differed from :func:`load_keys` when *both* the bare
    ``OPENROUTER_API_KEY`` *and* numbered slots were set —
    ``load_keys`` ignores the bare value (and logs a warning) and
    uses only the numbered slots, but ``_read_env_keys`` was
    pushing the bare value into the desired pool first and then
    appending each numbered slot dedup'd against it. Result: the
    "desired pool" computed here had ``len(numbered) + 1`` entries
    while the post-``load_keys`` ``_keys`` had ``len(numbered)``
    entries, so :func:`refresh_from_db`'s no-op fast path NEVER
    fired even when the env was unchanged, and the rebuild branch
    then duplicated the last numbered slot into the pool (because
    the slice ``desired[len(_keys):]`` over-shot by one). The fix
    matches ``load_keys`` exactly: numbered slots win, the bare
    value is honoured only when no numbered slot is set.
    """
    numbered: list[str] = []
    for n in range(1, 11):
        candidate = os.getenv(f"OPENROUTER_API_KEY_{n}", "").strip()
        if candidate and candidate not in numbered:
            numbered.append(candidate)
    if numbered:
        return numbered
    primary = os.getenv("OPENROUTER_API_KEY", "").strip()
    if primary:
        return [primary]
    return []


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
