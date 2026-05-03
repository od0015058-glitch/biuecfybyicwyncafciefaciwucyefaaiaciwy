"""Stage-16 row 20 — abuse / spam detection middleware.

Three independent layers, each layered onto a different choke
point of the chat hot-path:

1.  **Length cap** (``is_oversized``) — hard reject prompts longer
    than ``ABUSE_MAX_PROMPT_CHARS`` (default 50 000). Nobody pastes
    a 50K-char novel into a Telegram chat legitimately, and the
    longest legitimate "paste my entire codebase" use case fits
    comfortably under that ceiling. Cheap O(1) check on
    ``len(text)``.

2.  **Pattern scan** (``looks_like_injection``) — regex match
    against the most common probe payloads (SQL injection,
    HTML/JS injection, prompt-injection markers, excessive
    character repetition). Conservative — only fires on
    high-confidence patterns that have *no* legitimate use in a
    Persian/English chat with an LLM. False-positive risk is
    minimised by requiring multiple signals or an exact-token
    match (e.g. ``UNION SELECT`` and ``DROP TABLE`` are matched as
    case-insensitive multi-token sequences, not just the word
    ``select``).

3.  **Spend-spike tracker** (``SpendSpikeTracker``) — sliding
    window of per-user spend over the last
    ``ABUSE_SPEND_SPIKE_WINDOW_SECONDS`` (default 600 = 10
    minutes). When a user's window total crosses
    ``ABUSE_SPEND_SPIKE_THRESHOLD_USD`` (default $5), record a DM
    to admins exactly once until the user's window drops back
    below threshold. Alert-first: legitimate heavy users
    (someone churning through a $100 wallet on Claude Opus) get
    *one* admin DM, not a chat-blocking ban.

The first two layers go through the chat handler before
``consume_chat_token`` so they can short-circuit billing entirely
on an obvious abuse signal. The third layer hooks into
``ai_engine.chat_with_model`` after a paid settlement so the
counter only includes actual spend, not free-tier prompts.

All three layers are individually toggle-able via env so a
deployment that runs into a false-positive can disable a single
check without losing the other two:

* ``ABUSE_DETECTION_ENABLED`` — master switch (default ``true``).
* ``ABUSE_PATTERNS_ENABLED`` — regex layer only (default ``true``).
* ``ABUSE_SPEND_SPIKE_ENABLED`` — spike tracker only (default ``true``).

The patterns module never *blocks* by itself — it returns a
classification (``"ok"`` / ``"oversized"`` / ``"injection_probe"`` /
``"spam_repetition"``) and lets the caller decide what to do. By
default the chat handler treats every non-``"ok"`` classification
as a soft drop with a generic "you're sending too fast" reply
(the *same* reply used for the local rate limit) so the attacker
can't probe which pattern fired and refine their payload — the
response is the same for "your message is too long" as for "your
message looks like a SQL injection". Logs at WARNING include the
full classification + a 200-char prefix of the offending message
for ops review.

Bundled bug fix: the photo handler (``handlers.process_photo``)
flowed ``message.caption`` straight through to
:func:`ai_engine.chat_with_model` without checking caption length.
The existing rate-limit gate caps prompts-per-second but not
prompt *size* per turn, so an attacker could pair a 1 MB caption
with a single photo upload — one rate-limit slot, one OpenRouter
prompt-token bill measured in millions. Routing the caption
through :func:`classify` in the same place the text path now
checks ``message.text`` closes that hole at zero extra cost.
"""

from __future__ import annotations

import logging
import os
import re
import time
from collections import defaultdict, deque
from typing import Final, Literal

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

from admin import get_admin_user_ids


log = logging.getLogger("bot.abuse_detection")


# ── Knobs ──────────────────────────────────────────────────────────

_DEFAULT_MAX_PROMPT_CHARS: Final[int] = 50_000
_DEFAULT_SPIKE_WINDOW_SECONDS: Final[int] = 600
_DEFAULT_SPIKE_THRESHOLD_USD: Final[float] = 5.0
# Hard floor: a deployment that wants to *disable* the length cap
# can set ``ABUSE_DETECTION_ENABLED=false`` instead of trying to
# raise the cap to "infinity"; values <= 0 fall back to the default
# rather than letting an env typo silently disable the limit.
_MIN_PROMPT_CHARS_FLOOR: Final[int] = 1_000


def _env_bool(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _env_int(name: str, default: int, *, floor: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        v = int(raw.strip())
    except (ValueError, AttributeError):
        log.warning(
            "abuse_detection: invalid integer in %s=%r — falling back to %d",
            name, raw, default,
        )
        return default
    if floor is not None and v < floor:
        log.warning(
            "abuse_detection: %s=%d is below floor %d — using floor instead. "
            "If you intended to disable this layer, set the corresponding "
            "*_ENABLED=false env var.", name, v, floor,
        )
        return floor
    return v


def _env_float(name: str, default: float, *, floor: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        v = float(raw.strip())
    except (ValueError, AttributeError):
        log.warning(
            "abuse_detection: invalid float in %s=%r — falling back to %.4f",
            name, raw, default,
        )
        return default
    if floor is not None and v < floor:
        log.warning(
            "abuse_detection: %s=%.4f is below floor %.4f — using floor instead.",
            name, v, floor,
        )
        return floor
    return v


def is_enabled() -> bool:
    """Master switch. ``ABUSE_DETECTION_ENABLED`` (default ``true``)."""
    return _env_bool("ABUSE_DETECTION_ENABLED", True)


def patterns_enabled() -> bool:
    """Regex-pattern layer. ``ABUSE_PATTERNS_ENABLED`` (default ``true``)."""
    return _env_bool("ABUSE_PATTERNS_ENABLED", True)


def spend_spike_enabled() -> bool:
    """Spike tracker. ``ABUSE_SPEND_SPIKE_ENABLED`` (default ``true``)."""
    return _env_bool("ABUSE_SPEND_SPIKE_ENABLED", True)


def max_prompt_chars() -> int:
    """``ABUSE_MAX_PROMPT_CHARS`` — hard upper bound on user prompts.

    Defaults to 50 000. Values below the floor of 1 000 are clamped
    so an env typo (``ABUSE_MAX_PROMPT_CHARS=-1``) can't silently
    disable the limit; use ``ABUSE_DETECTION_ENABLED=false`` to
    actually disable.
    """
    return _env_int(
        "ABUSE_MAX_PROMPT_CHARS",
        _DEFAULT_MAX_PROMPT_CHARS,
        floor=_MIN_PROMPT_CHARS_FLOOR,
    )


def spike_window_seconds() -> int:
    """``ABUSE_SPEND_SPIKE_WINDOW_SECONDS`` — sliding-window length.

    Defaults to 600 (10 minutes). Floor 60 — anything shorter is
    too noisy to be useful (legitimate users routinely fire 10
    prompts in a minute).
    """
    return _env_int(
        "ABUSE_SPEND_SPIKE_WINDOW_SECONDS",
        _DEFAULT_SPIKE_WINDOW_SECONDS,
        floor=60,
    )


def spike_threshold_usd() -> float:
    """``ABUSE_SPEND_SPIKE_THRESHOLD_USD`` — alert trigger.

    Defaults to $5 — chosen so a normal Claude Opus user (who can
    burn $0.50–$1 on a long answer) doesn't hit the threshold from
    one heavy turn, but a runaway script firing 50 prompts in
    10 minutes does.
    """
    return _env_float(
        "ABUSE_SPEND_SPIKE_THRESHOLD_USD",
        _DEFAULT_SPIKE_THRESHOLD_USD,
        floor=0.01,
    )


# ── Pattern classifier ─────────────────────────────────────────────

Classification = Literal[
    "ok", "oversized", "injection_probe", "spam_repetition"
]


# Conservative regex — every pattern below has *no* legitimate use
# in a Persian/English chat. The classifier is intentionally narrow
# (false positives are worse than false negatives — abuse detection
# is an alert layer, not a strict gate).
_INJECTION_PATTERNS: tuple[re.Pattern[str], ...] = (
    # SQL injection: UNION SELECT and DROP/TRUNCATE TABLE — both
    # require the multi-token sequence so the word ``select`` alone
    # in normal English doesn't trigger.
    re.compile(r"\bunion\s+(?:all\s+)?select\b", re.IGNORECASE),
    re.compile(
        r"\b(?:drop|truncate|alter)\s+table\b", re.IGNORECASE
    ),
    re.compile(r"\bor\s+1\s*=\s*1\b", re.IGNORECASE),
    re.compile(r";\s*--", re.IGNORECASE),
    # XSS / HTML injection. ``<script>`` is a virtual giveaway —
    # nobody legitimately asks an LLM to render raw JS via Telegram.
    re.compile(r"<\s*script\b", re.IGNORECASE),
    re.compile(r"\bjavascript\s*:", re.IGNORECASE),
    re.compile(r"\bonerror\s*=", re.IGNORECASE),
    # Common shell-injection probes — also no legitimate use here.
    re.compile(r"\$\(\s*curl\b", re.IGNORECASE),
    re.compile(r"\bwget\s+http", re.IGNORECASE),
)


# Excessive character repetition: 200+ of the same char in a row.
# Threshold high enough that a code block of dashes / equals signs
# (typical separator) doesn't false-positive. Keyboard-mashing
# spam regularly hits 1000+.
_REPETITION_THRESHOLD: Final[int] = 200
_REPETITION_PATTERN: re.Pattern[str] = re.compile(
    r"(.)\1{" + str(_REPETITION_THRESHOLD - 1) + r",}"
)


def is_oversized(text: str | None, *, max_chars: int | None = None) -> bool:
    """Return ``True`` if ``text`` is longer than the configured cap.

    ``None`` and empty strings are *not* oversized — they're normal
    no-text messages (a stickers / photo-only post).
    """
    if text is None:
        return False
    cap = max_chars if max_chars is not None else max_prompt_chars()
    return len(text) > cap


def classify(text: str | None, *, max_chars: int | None = None) -> Classification:
    """Classify a user prompt against every enabled layer.

    Returns:
        * ``"ok"``               — passed every check.
        * ``"oversized"``        — length cap breach.
        * ``"injection_probe"``  — high-confidence regex match.
        * ``"spam_repetition"``  — extreme character repetition.

    Layers run in priority order — the first non-``ok`` classification
    short-circuits, so an oversized payload that *also* contains an
    injection probe is reported as ``"oversized"`` (the bigger
    operational concern).
    """
    if not is_enabled():
        return "ok"
    if text is None or text == "":
        return "ok"

    if is_oversized(text, max_chars=max_chars):
        return "oversized"

    if patterns_enabled():
        for pat in _INJECTION_PATTERNS:
            if pat.search(text):
                return "injection_probe"
        if _REPETITION_PATTERN.search(text):
            return "spam_repetition"

    return "ok"


# ── Spend-spike tracker ────────────────────────────────────────────


class SpendSpikeTracker:
    """Per-user sliding-window spend tracker.

    In-process and lock-free — atomicity is guaranteed by Python's
    GIL for the deque operations and float arithmetic this class
    does. A multi-replica deployment will track per-replica spend
    rather than aggregate (intentional — a user routed exclusively
    to replica A can hit the threshold even if replica B sees no
    traffic from them; the alert tells admins which replica saw
    the spike).

    The tracker keeps each user's spend events as a deque of
    ``(timestamp, amount_usd)`` tuples. ``record_spend`` evicts
    everything older than the window from the head and appends the
    new event to the tail, then sums the remaining tail to compute
    the rolling total. Eviction is amortised O(1) — each event is
    appended once and evicted once over its lifetime.

    Bounded memory: each user's deque is hard-capped at
    ``_MAX_EVENTS_PER_USER`` so a runaway user can't OOM the
    tracker. When the cap is hit we evict the oldest entry first
    (FIFO) — the rolling window is approximate at that point but
    still correct for the "this user is spending money fast" alert.

    ``should_alert(user_id, total_usd)`` returns the breach amount
    (the rolling total) when both:
        * the rolling total just crossed the threshold, AND
        * we haven't already alerted on this user since the total
          last fell below the threshold.

    The de-bounce avoids spamming admins with one DM per prompt
    once a heavy user goes over the line.
    """

    _MAX_EVENTS_PER_USER: Final[int] = 1_000

    def __init__(self) -> None:
        self._events: dict[int, deque[tuple[float, float]]] = defaultdict(deque)
        self._alerted: set[int] = set()
        # Per-user pending alert detail. Populated by
        # ``record_paid_spend`` when the latch flips from "below"
        # to "above" the threshold; drained by ``pop_pending_alert``
        # in the chat handler. The tuple is ``(rolling_total,
        # last_call_cost)`` so the alert text can name both
        # numbers without re-querying the tracker (and racing).
        self._pending_alerts: dict[int, tuple[float, float]] = {}

    def reset_for_tests(self) -> None:
        self._events.clear()
        self._alerted.clear()
        self._pending_alerts.clear()

    def record_spend(
        self,
        user_id: int,
        amount_usd: float,
        *,
        now: float | None = None,
        window_seconds: int | None = None,
    ) -> float:
        """Record a paid call for ``user_id``. Returns the new rolling total."""
        if amount_usd <= 0 or not _is_finite(amount_usd):
            # Free-tier calls (cost=0) and degenerate values don't
            # contribute to the spike total — only real wallet
            # debits move the needle.
            return self._current_total(
                user_id, now=now, window_seconds=window_seconds
            )

        ts = now if now is not None else time.time()
        win = (
            window_seconds
            if window_seconds is not None
            else spike_window_seconds()
        )
        cutoff = ts - win

        events = self._events[user_id]
        # Evict expired events.
        while events and events[0][0] < cutoff:
            events.popleft()
        events.append((ts, amount_usd))
        # Bound the deque size — pop oldest first if we hit the cap.
        while len(events) > self._MAX_EVENTS_PER_USER:
            events.popleft()

        total = sum(amt for _, amt in events)

        # Reset the alert latch when the total drops back below the
        # threshold, so a *future* spike can re-alert. Done here
        # (after adding the new event) so the latch is checked
        # against the live total.
        threshold = spike_threshold_usd()
        if total < threshold and user_id in self._alerted:
            self._alerted.discard(user_id)

        return total

    def _current_total(
        self,
        user_id: int,
        *,
        now: float | None = None,
        window_seconds: int | None = None,
    ) -> float:
        ts = now if now is not None else time.time()
        win = (
            window_seconds
            if window_seconds is not None
            else spike_window_seconds()
        )
        cutoff = ts - win
        events = self._events.get(user_id)
        if not events:
            return 0.0
        return sum(amt for t, amt in events if t >= cutoff)

    def should_alert(self, user_id: int, *, total_usd: float) -> bool:
        """Return True iff this is a *new* breach for ``user_id``.

        Idempotent: the second call with the same user above the
        threshold returns False until ``record_spend`` brings the
        total back below the threshold (which clears the latch).
        """
        if total_usd < spike_threshold_usd():
            return False
        if user_id in self._alerted:
            return False
        self._alerted.add(user_id)
        return True


def _is_finite(x: float) -> bool:
    return x == x and x != float("inf") and x != float("-inf")


# Module-level singleton — the chat hot-path imports this and calls
# ``record_spend`` after every successful settlement.
TRACKER: SpendSpikeTracker = SpendSpikeTracker()


# ── Admin DM helper ────────────────────────────────────────────────


def _format_alert(
    *,
    kind: str,
    user_id: int,
    detail: str,
) -> str:
    """Render a plain-text admin DM. No Markdown — model names and
    user-supplied text aren't escaped, so we render plain text to
    avoid having to scrub ``_`` and ``*``.
    """
    return (
        "⚠️ Possible abuse detected\n"
        f"Kind: {kind}\n"
        f"User: {user_id}\n"
        f"{detail}"
    )


async def notify_admins_of_abuse(
    bot: Bot,
    *,
    kind: str,
    user_id: int,
    detail: str,
) -> int:
    """Send an abuse-alert DM to every admin. Returns successful sends.

    Per-admin fault isolation matches :func:`model_discovery.notify_admins`
    — a bot-blocked-by-admin or a transient Telegram 5xx on admin A
    doesn't stop admin B's notification. Logged at WARNING because
    abuse alerts are higher priority than a normal price-delta DM.
    """
    admin_ids = get_admin_user_ids()
    if not admin_ids:
        log.warning(
            "abuse_detection: kind=%s user_id=%d detail=%r — but "
            "ADMIN_USER_IDS is empty so nothing to notify. Set "
            "ADMIN_USER_IDS to receive these.",
            kind, user_id, detail,
        )
        return 0
    text = _format_alert(kind=kind, user_id=user_id, detail=detail)
    sent = 0
    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, text, disable_web_page_preview=True)
            sent += 1
        except TelegramForbiddenError:
            log.info(
                "abuse_detection: admin %d blocked the bot; skipping",
                admin_id,
            )
        except TelegramAPIError:
            log.exception(
                "abuse_detection: failed to send abuse alert to admin %d",
                admin_id,
            )
    return sent


def record_paid_spend(user_id: int, cost_usd: float) -> None:
    """Record a paid OpenRouter settlement against the spike tracker.

    Called from ``ai_engine.chat_with_model`` after a successful
    paid debit. Sync (no await) so it slots into the existing
    settlement code without changing the function shape. A breach
    is latched on the tracker and the *handler* (which has a
    ``Bot`` reference) drains the latch via :func:`pop_pending_alert`
    after the AI reply is sent.

    No-op when the spike tracker is disabled.
    """
    if not is_enabled() or not spend_spike_enabled():
        return
    total = TRACKER.record_spend(user_id, cost_usd)
    # ``should_alert`` itself sets the latch, so calling it here
    # is what enrolls the user for a pending DM. ``pop_pending_alert``
    # below reads the same latch.
    if TRACKER.should_alert(user_id, total_usd=total):
        TRACKER._pending_alerts[user_id] = (total, cost_usd)


def pop_pending_alert(user_id: int) -> tuple[float, float] | None:
    """Drain the pending-alert state for ``user_id``.

    Returns ``(rolling_total_usd, last_call_usd)`` if the user just
    breached the threshold and hasn't been alerted yet, otherwise
    ``None``. Idempotent — a second call returns ``None``.
    """
    return TRACKER._pending_alerts.pop(user_id, None)


async def maybe_alert_spend_spike(
    bot: Bot,
    *,
    user_id: int,
) -> bool:
    """DM admins iff ``user_id`` has a pending spike alert.

    Single entry point for the chat handler to call after the AI
    reply has been sent. Returns ``True`` iff an alert was actually
    sent (mostly for tests). Wrapped in a try/except so a transient
    Telegram error during the DM can't lose the user's reply (the
    reply has already been sent by this point — but we don't want
    a Telegram 5xx on the *admin* DM to surface as a poller-level
    crash for the requesting user).
    """
    if not is_enabled() or not spend_spike_enabled():
        return False

    pending = pop_pending_alert(user_id)
    if pending is None:
        return False

    total, cost_usd = pending
    threshold = spike_threshold_usd()
    window_min = spike_window_seconds() / 60
    detail = (
        f"Spent ${total:.2f} in the last {window_min:.0f} min "
        f"(threshold ${threshold:.2f}). Last call charged ${cost_usd:.4f}."
    )
    try:
        await notify_admins_of_abuse(
            bot, kind="spend_spike", user_id=user_id, detail=detail
        )
    except Exception:  # pragma: no cover — Telegram weirdness
        log.exception(
            "abuse_detection: notify_admins_of_abuse raised for user %d "
            "(spike $%.2f) — swallowing so the AI reply isn't lost.",
            user_id, total,
        )
        return False
    return True


# ── Test hooks ─────────────────────────────────────────────────────


def reset_for_tests() -> None:
    """Reset all module-level state. Called from per-test fixtures."""
    TRACKER.reset_for_tests()
