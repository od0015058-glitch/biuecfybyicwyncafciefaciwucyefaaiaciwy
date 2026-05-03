"""Stage-16 row 19 — user 👍 / 👎 feedback on AI replies.

After the bot answers a paid chat turn, attach a two-button
inline keyboard underneath the reply. A tap records the user's
verdict against the just-inserted ``usage_logs`` row (new
``feedback`` column added by migration 0020) and, separately, a
periodic background loop watches the trailing window for
per-model dissatisfaction-rate spikes and DMs admins when a
model crosses a configurable threshold.

Three independently togglable layers, mirroring the structure of
``abuse_detection``:

* **Keyboard layer** — ``AI_FEEDBACK_ENABLED`` (default
  ``true``). When off, ``build_feedback_keyboard`` returns
  ``None`` and the chat handler skips the keyboard. Existing
  callback-data still resolves (so a previously-attached
  keyboard can still be tapped) but new replies don't get
  buttons.

* **Persistence layer** — always on when the keyboard is on.
  ``record_feedback`` round-trips through
  :meth:`database.Database.record_usage_feedback` which itself
  enforces the per-user owner check + first-tap-wins idempotency.

* **Alert layer** — ``AI_FEEDBACK_ALERT_ENABLED`` (default
  ``true``). The background loop wakes every
  ``AI_FEEDBACK_LOOP_INTERVAL_SECONDS`` (default 300 = 5 min),
  computes per-model negative-rate over the trailing
  ``AI_FEEDBACK_DISSATISFACTION_WINDOW_SECONDS`` (default 3600
  = 1 h), and DMs admins about any model with at least
  ``AI_FEEDBACK_MIN_SAMPLES`` (default 10) rated calls AND a
  negative-rate above ``AI_FEEDBACK_DISSATISFACTION_THRESHOLD_RATIO``
  (default 0.30 = 30%). Per-model alert-cooldown of
  ``AI_FEEDBACK_ALERT_COOLDOWN_SECONDS`` (default 3600) so a
  sustained spike DMs admins at most once per cooldown window
  per model.

The ``min_samples`` floor is the most important knob — without
it, a single thumbs-down on a model's first paid call would
read as "100% dissatisfaction" and page admins. A minimum of
10 rated calls means the alert needs a sustained pattern, not
a one-off bad reply.

Callback-data shape: ``fbp:<log_id>`` for 👍, ``fbn:<log_id>``
for 👎. Telegram caps callback_data at 64 bytes; ``fbp:`` +
the largest plausible 64-bit log_id is well under the cap.
Short prefixes also keep the per-message payload compact when
multiple inline keyboards live on the same chat.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Final

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

from admin import get_admin_user_ids
from bot_health import register_loop


log = logging.getLogger("bot.ai_feedback")


# ── Knobs ──────────────────────────────────────────────────────────


_DEFAULT_DISSATISFACTION_THRESHOLD_RATIO: Final[float] = 0.30
_DEFAULT_DISSATISFACTION_WINDOW_SECONDS: Final[int] = 3600
_DEFAULT_MIN_SAMPLES: Final[int] = 10
_DEFAULT_LOOP_INTERVAL_SECONDS: Final[int] = 300
_DEFAULT_ALERT_COOLDOWN_SECONDS: Final[int] = 3600

# Hard floors. ``min_samples`` below 1 would ZeroDivision on the
# rate aggregate; the window below 60 s would chase tail noise; the
# loop interval below 30 s would hammer the DB on a tracker that's
# meant to be ambient.
_MIN_SAMPLES_FLOOR: Final[int] = 1
_WINDOW_FLOOR: Final[int] = 60
_LOOP_INTERVAL_FLOOR: Final[int] = 30


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
            "ai_feedback: invalid integer in %s=%r — falling back to %d",
            name, raw, default,
        )
        return default
    if floor is not None and v < floor:
        log.warning(
            "ai_feedback: %s=%d below floor %d — clamping to floor",
            name, v, floor,
        )
        return floor
    return v


def _env_float(
    name: str,
    default: float,
    *,
    floor: float | None = None,
    ceiling: float | None = None,
) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        v = float(raw.strip())
    except (ValueError, AttributeError):
        log.warning(
            "ai_feedback: invalid float in %s=%r — falling back to %g",
            name, raw, default,
        )
        return default
    if floor is not None and v < floor:
        log.warning(
            "ai_feedback: %s=%g below floor %g — clamping",
            name, v, floor,
        )
        return floor
    if ceiling is not None and v > ceiling:
        log.warning(
            "ai_feedback: %s=%g above ceiling %g — clamping",
            name, v, ceiling,
        )
        return ceiling
    return v


def is_enabled() -> bool:
    """Master toggle. When off, no keyboard, no alert loop work."""
    return _env_bool("AI_FEEDBACK_ENABLED", True)


def alert_enabled() -> bool:
    """Background-alert toggle. Independent of the keyboard layer."""
    return _env_bool("AI_FEEDBACK_ALERT_ENABLED", True)


def dissatisfaction_threshold_ratio() -> float:
    return _env_float(
        "AI_FEEDBACK_DISSATISFACTION_THRESHOLD_RATIO",
        _DEFAULT_DISSATISFACTION_THRESHOLD_RATIO,
        floor=0.0,
        ceiling=1.0,
    )


def dissatisfaction_window_seconds() -> int:
    return _env_int(
        "AI_FEEDBACK_DISSATISFACTION_WINDOW_SECONDS",
        _DEFAULT_DISSATISFACTION_WINDOW_SECONDS,
        floor=_WINDOW_FLOOR,
    )


def min_samples() -> int:
    return _env_int(
        "AI_FEEDBACK_MIN_SAMPLES",
        _DEFAULT_MIN_SAMPLES,
        floor=_MIN_SAMPLES_FLOOR,
    )


def loop_interval_seconds() -> int:
    return _env_int(
        "AI_FEEDBACK_LOOP_INTERVAL_SECONDS",
        _DEFAULT_LOOP_INTERVAL_SECONDS,
        floor=_LOOP_INTERVAL_FLOOR,
    )


def alert_cooldown_seconds() -> int:
    # Cooldown can legitimately equal the window (or even exceed it)
    # so the floor is just "non-zero" — a 0-cooldown defeats the
    # purpose of the latch.
    return _env_int(
        "AI_FEEDBACK_ALERT_COOLDOWN_SECONDS",
        _DEFAULT_ALERT_COOLDOWN_SECONDS,
        floor=1,
    )


# ── Callback-data + keyboard ───────────────────────────────────────


CALLBACK_PREFIX_POSITIVE: Final[str] = "fbp:"
CALLBACK_PREFIX_NEGATIVE: Final[str] = "fbn:"

# Public so the router filter can match without re-importing the
# constants individually.
CALLBACK_PREFIXES: Final[tuple[str, ...]] = (
    CALLBACK_PREFIX_POSITIVE,
    CALLBACK_PREFIX_NEGATIVE,
)


def build_feedback_keyboard(
    log_id: int,
    *,
    enabled: bool | None = None,
) -> InlineKeyboardMarkup | None:
    """Two-button (👍 / 👎) inline keyboard keyed on ``log_id``.

    Returns ``None`` when the feature is disabled (so the chat
    handler can fall back to a plain ``message.answer(text)``
    call) or when ``log_id`` is invalid (free-tier turn that
    didn't write a usage_logs row, or a refused-cost INSERT).

    ``enabled`` is the test seam — production code passes
    ``None`` and the helper consults :func:`is_enabled`.
    """
    if log_id is None or log_id <= 0:
        return None
    if enabled is None:
        enabled = is_enabled()
    if not enabled:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👍",
                    callback_data=f"{CALLBACK_PREFIX_POSITIVE}{int(log_id)}",
                ),
                InlineKeyboardButton(
                    text="👎",
                    callback_data=f"{CALLBACK_PREFIX_NEGATIVE}{int(log_id)}",
                ),
            ]
        ]
    )


def parse_feedback_callback(data: str | None) -> tuple[str, int] | None:
    """Decode a ``fbp:<id>`` / ``fbn:<id>`` callback_data.

    Returns ``(feedback_kind, log_id)`` where ``feedback_kind`` is
    ``'positive'`` / ``'negative'``, or ``None`` for any malformed /
    unknown payload (wrong prefix, non-integer id, negative id).
    Defensive against the user side-loading a hand-rolled callback
    via Telegram's keyboard API.
    """
    if not data or not isinstance(data, str):
        return None
    for prefix, kind in (
        (CALLBACK_PREFIX_POSITIVE, "positive"),
        (CALLBACK_PREFIX_NEGATIVE, "negative"),
    ):
        if data.startswith(prefix):
            tail = data[len(prefix):]
            try:
                log_id = int(tail)
            except (ValueError, TypeError):
                return None
            if log_id <= 0:
                return None
            return kind, log_id
    return None


# ── Alert layer ────────────────────────────────────────────────────


# Per-model latch: ``model_id -> last_alert_epoch_seconds``. A
# model already alerted within the cooldown is skipped. The dict
# is process-local so a restart resets the latches — same
# bootstrap-replay semantics as ``pending_alert._STATE`` (an
# operator who deployed a fix expects the alert to re-fire if the
# fix didn't actually clear the spike).
_LAST_ALERT_AT: dict[str, float] = {}


def reset_alert_state_for_tests() -> None:
    """Test hook: clear the per-model latch."""
    _LAST_ALERT_AT.clear()


def _format_alert_body(rows: list[dict]) -> str:
    """Render the admin DM body. Plain text, no Markdown — model
    names contain ``/`` and ``-`` and a 1-token vendor-prefix that
    a Markdown parser would mis-handle (e.g. ``deepseek/deepseek-r1``
    has a hyphen in the path that a v2 parser would render as a
    list item). Plain text dodges all of that.
    """
    threshold_pct = dissatisfaction_threshold_ratio() * 100
    window_min = dissatisfaction_window_seconds() / 60
    head = (
        f"⚠️ High AI dissatisfaction rate\n"
        f"Window: last {window_min:.0f} min, "
        f"threshold {threshold_pct:.0f}%\n"
    )
    lines = []
    for r in rows:
        rate_pct = r["negative_rate"] * 100
        lines.append(
            f"• {r['model']}: {r['negative']}/{r['total']} 👎 "
            f"({rate_pct:.0f}%)"
        )
    return head + "\n".join(lines)


async def _notify_admins(bot: Bot, body: str) -> int:
    """DM ``body`` to every admin. Returns successful sends.

    Per-admin fault isolation matches
    ``abuse_detection.notify_admins_of_abuse`` and
    ``model_discovery.notify_admins_of_price_deltas``: a blocked
    admin or a Telegram 5xx on admin A doesn't stop admin B's DM.
    """
    admin_ids = get_admin_user_ids()
    if not admin_ids:
        log.warning(
            "ai_feedback: dissatisfaction alert ready (%d chars) but "
            "ADMIN_USER_IDS is empty so nothing to notify. Set "
            "ADMIN_USER_IDS to receive these.",
            len(body),
        )
        return 0
    sent = 0
    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, body, disable_web_page_preview=True)
            sent += 1
        except TelegramForbiddenError:
            log.info(
                "ai_feedback: admin %d blocked the bot; skipping",
                admin_id,
            )
        except TelegramAPIError:
            log.exception(
                "ai_feedback: failed to send dissatisfaction alert to "
                "admin %d",
                admin_id,
            )
    return sent


def _filter_alertable(
    rows: list[dict],
    *,
    threshold_ratio: float,
    min_n: int,
    now: float,
    cooldown: int,
) -> list[dict]:
    """Pick the per-model rows that should fire an alert this tick.

    Selection rules (all must hold):

    * ``total >= min_n`` — enough samples for the rate to be
      meaningful.
    * ``negative_rate >= threshold_ratio`` — actually a spike,
      not just a few thumbs-downs.
    * Not in cooldown — the model wasn't already alerted within
      ``cooldown`` seconds.

    The cooldown is NOT advanced here — :func:`run_dissatisfaction_check`
    advances it AFTER a successful DM so a Telegram failure on one tick
    re-triggers next tick rather than silently dropping.
    """
    out: list[dict] = []
    for r in rows:
        if r["total"] < min_n:
            continue
        if r["negative_rate"] < threshold_ratio:
            continue
        last = _LAST_ALERT_AT.get(r["model"])
        if last is not None and now - last < cooldown:
            continue
        out.append(r)
    return out


async def run_dissatisfaction_check(
    bot: Bot,
    *,
    db,
    now: float | None = None,
) -> int:
    """Single-tick body of the alert loop. Returns alerts fired.

    Pulled out as a standalone coroutine so tests can drive one
    tick deterministically without touching the asyncio sleep loop.
    """
    if not is_enabled() or not alert_enabled():
        return 0

    threshold_ratio = dissatisfaction_threshold_ratio()
    win = dissatisfaction_window_seconds()
    min_n = min_samples()
    cooldown = alert_cooldown_seconds()
    ts = now if now is not None else time.time()

    try:
        rows = await db.get_recent_feedback_rates(
            window_seconds=win,
            min_samples=min_n,
        )
    except Exception:
        log.exception(
            "ai_feedback: get_recent_feedback_rates failed; "
            "skipping this tick"
        )
        return 0

    alertable = _filter_alertable(
        rows,
        threshold_ratio=threshold_ratio,
        min_n=min_n,
        now=ts,
        cooldown=cooldown,
    )
    if not alertable:
        return 0

    body = _format_alert_body(alertable)
    sent = await _notify_admins(bot, body)
    if sent > 0:
        for r in alertable:
            _LAST_ALERT_AT[r["model"]] = ts
    return sent


@register_loop(
    "ai_feedback_alert",
    cadence_seconds=_DEFAULT_LOOP_INTERVAL_SECONDS,
)
async def dissatisfaction_alert_loop(bot: Bot) -> None:
    """Background loop driver. Cancellable; never crashes on a tick.

    Records a ``record_loop_tick("ai_feedback_alert")`` heartbeat
    after each successful pass so Prometheus' "loop is stuck"
    alert can detect a wedged loop the same way it does for the
    other background loops.
    """
    interval = loop_interval_seconds()
    log.info(
        "ai_feedback: dissatisfaction alert loop starting "
        "(interval=%ds, threshold=%.0f%%, window=%ds, min_samples=%d)",
        interval,
        dissatisfaction_threshold_ratio() * 100,
        dissatisfaction_window_seconds(),
        min_samples(),
    )
    # Local import so the module stays importable in unit tests
    # without spinning up a real DB pool. The loop driver only
    # runs from ``main`` where the DB is already initialised.
    from database import db  # noqa: WPS433
    from metrics import record_loop_tick  # noqa: WPS433

    try:
        while True:
            try:
                await run_dissatisfaction_check(bot, db=db)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "ai_feedback: dissatisfaction alert tick crashed; "
                    "next tick in %ds",
                    interval,
                )
            else:
                record_loop_tick("ai_feedback_alert")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log.info("ai_feedback: dissatisfaction alert loop cancelled; "
                 "exiting cleanly")
        raise


def start_dissatisfaction_alert_task(bot: Bot) -> asyncio.Task:
    """Spawn the loop; caller is responsible for cancel + await on
    shutdown (mirrors :func:`pending_alert.start_pending_alert_task`).
    """
    return asyncio.create_task(
        dissatisfaction_alert_loop(bot),
        name="ai-feedback-dissatisfaction-loop",
    )


# ── Test hooks ─────────────────────────────────────────────────────


def reset_for_tests() -> None:
    """Reset module-level state. Called from per-test fixtures."""
    reset_alert_state_for_tests()
