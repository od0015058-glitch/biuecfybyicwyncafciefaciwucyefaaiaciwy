"""Per-user spending analytics (Stage-15-Step-E #2, first slice).

Renders the snapshot produced by
:meth:`Database.get_user_spending_summary` as a Telegram message
suitable for the new ``hub_stats`` wallet sub-screen.

Status: **STARTED, not finished.** This module is the minimal
viable first slice. The full Step-E #2 spec (per HANDOFF §5) calls
for:

* ``/stats`` text-message screen  ✅ (this module + the
  ``hub_stats`` handler)
* ``/stats`` slash-command alias  ❌ (the wallet-menu button is
  the only entry point right now; an explicit ``/stats`` command
  is a one-liner ``router.message`` handler the next AI can add)
* Per-day / per-week breakdowns  ❌ (we surface lifetime + a
  rolling 30-day window only; no day-over-day series yet)
* Graphs / sparkline charts  ❌ (would need a rendering library
  like ``matplotlib`` or ``Pillow`` — explicitly out of scope for
  the first slice; product-team to confirm before adding the
  dependency)
* Rate limiting on the screen  ❌ (the wallet-menu button is the
  only entry, and Telegram's own callback-debouncing provides a
  soft cap; if a ``/stats`` text command is added, gate it behind
  the same chat-token bucket as ``cmd_chat``)
* CSV export of the user's full ``usage_logs`` rows  ❌ (the
  admin panel already has a JSON view; a user-facing CSV would
  pair nicely with the conversation export from Step-E #1)

The next AI working on Step-E #2 should pick up where this module
leaves off — the schema and the user-facing entry point are
already in place.

Pure-function module on purpose: takes a DB snapshot dict in,
returns a rendered string out. The handler does the I/O. Lets the
formatter be unit-tested without the full asyncpg / aiogram stack.

Bug-fix bundled with the same PR: the conversation-history export
caption used to claim ``len(rows)`` messages even when the
formatter trimmed older messages to fit under the 1 MB cap — see
the docstring on
:func:`conversation_export.format_history_as_text` for the
``(text, kept_count)`` shape change that fixes it.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Iterable

from strings import t


# Hard cap on the model-id portion of a "top models" line. Keeps
# the rendered Telegram message under the 4 KB limit even when an
# OpenRouter slug is unusually long (e.g. some
# "provider/model-name-instruct-vN" combinations push past 60
# chars). Pure presentation cap — the underlying DB row is
# untouched, so a future caller wanting the full slug can read
# from ``snapshot["top_models"]`` directly.
_TOP_MODEL_NAME_MAX_CHARS = 50


def _truncate_model_name(name: str) -> str:
    """Truncate ``name`` to :data:`_TOP_MODEL_NAME_MAX_CHARS` with an
    ellipsis suffix. Returns ``name`` unchanged when already short
    enough. Defensive against ``None`` / non-string input — falls
    back to ``"?"`` so the rendered line stays well-formed."""
    if not isinstance(name, str) or not name:
        return "?"
    if len(name) <= _TOP_MODEL_NAME_MAX_CHARS:
        return name
    # Reserve 1 char for the ellipsis so the visible width is
    # exactly ``_TOP_MODEL_NAME_MAX_CHARS``. ``len("…")`` is 1 in
    # Python (it's a single Unicode codepoint), so the slice
    # arithmetic is straightforward.
    return name[: _TOP_MODEL_NAME_MAX_CHARS - 1] + "…"


def _safe_float(value: object) -> float:
    """Coerce ``value`` to a finite float, falling back to ``0.0``.

    Defense-in-depth against a corrupted DB row (NaN / Inf /
    non-numeric type) bricking the whole rendered screen — same
    NaN-defence policy as ``wallet_display.format_balance_block``.
    """
    if isinstance(value, bool):
        # ``bool`` is a subclass of ``int`` in Python; refuse it
        # explicitly so a buggy caller passing ``True`` / ``False``
        # doesn't render as ``$1.00`` / ``$0.00``.
        return 0.0
    if not isinstance(value, (int, float)):
        return 0.0
    f = float(value)
    if not math.isfinite(f):
        return 0.0
    return f


def _safe_int(value: object) -> int:
    """Coerce ``value`` to a non-negative int, falling back to ``0``.

    Defense in depth — a NaN ``count`` from a corrupted aggregate
    would otherwise raise ``ValueError`` inside ``int()`` and
    crash the formatter.
    """
    if isinstance(value, bool):
        return 0
    if not isinstance(value, (int, float)):
        return 0
    if isinstance(value, float) and not math.isfinite(value):
        return 0
    n = int(value)
    return max(n, 0)


def format_stats_summary(
    snapshot: dict,
    lang: str | None,
    *,
    balance_usd: float | None = None,
) -> str:
    """Render a user's spending dashboard as a Markdown message body.

    ``snapshot`` is the dict produced by
    :meth:`database.Database.get_user_spending_summary`. The
    function is pure: no I/O, no DB. Intended caller is
    :func:`handlers.hub_stats_handler` after it has fetched the
    snapshot + the user's wallet balance.

    ``balance_usd`` is the user's current wallet balance, surfaced
    at the top of the screen so a user looking at "how much have
    I spent" doesn't have to bounce back to the wallet to see
    "how much do I have left". ``None`` (or non-finite) skips the
    balance line — defensive policy mirrors
    ``wallet_display.format_toman_annotation`` (no row is better
    than a misleading ``$nan`` row).

    The output uses Telegram's legacy Markdown (``*bold*``,
    ``` `code` ``` for model ids). Slugs that contain Markdown
    reserved characters (``_``, ``*``) are wrapped in inline-code
    backticks so they render as literals, matching the admin-side
    ``format_metrics`` rendering of the global "top models" tile.
    """
    lifetime = snapshot.get("lifetime") or {}
    window = snapshot.get("window") or {}
    top_models = snapshot.get("top_models") or []
    window_days = _safe_int(snapshot.get("window_days")) or 30

    lines: list[str] = [t(lang, "stats_title")]

    # Optional balance line — only render when the caller hands us
    # a finite, non-negative number. NaN / Inf / negative values are
    # corruption signals, not real balances; rendering them as ``$0.00``
    # would silently hide the corruption from a user who's looking at
    # this screen *because* they suspect their wallet is misbehaving.
    # Skipping the line entirely matches the "no row is better than a
    # misleading row" policy from ``wallet_display.format_toman_annotation``.
    if (
        balance_usd is not None
        and isinstance(balance_usd, (int, float))
        and not isinstance(balance_usd, bool)
        and math.isfinite(float(balance_usd))
        and float(balance_usd) >= 0.0
    ):
        lines.append("")
        lines.append(
            t(lang, "stats_balance_line", balance=float(balance_usd))
        )

    lifetime_calls = _safe_int(lifetime.get("total_calls"))
    lifetime_tokens = _safe_int(lifetime.get("total_tokens"))
    lifetime_cost = _safe_float(lifetime.get("total_cost_usd"))

    # Empty-data short-circuit. A user who has never sent a prompt
    # gets a friendly placeholder instead of a wall of zeroes —
    # mirrors the admin-side dashboard's ``"_(no usage logged
    # yet)_"`` line.
    if lifetime_calls == 0:
        lines.append("")
        lines.append(t(lang, "stats_empty"))
        return "\n".join(lines)

    lines.append("")
    lines.append(t(lang, "stats_lifetime_header"))
    lines.append(
        t(
            lang,
            "stats_lifetime_line",
            calls=lifetime_calls,
            tokens=lifetime_tokens,
            cost=lifetime_cost,
        )
    )

    window_calls = _safe_int(window.get("total_calls"))
    window_tokens = _safe_int(window.get("total_tokens"))
    window_cost = _safe_float(window.get("total_cost_usd"))

    lines.append("")
    lines.append(t(lang, "stats_window_header", days=window_days))
    lines.append(
        t(
            lang,
            "stats_window_line",
            calls=window_calls,
            tokens=window_tokens,
            cost=window_cost,
        )
    )

    if top_models:
        lines.append("")
        lines.append(t(lang, "stats_top_models_header", days=window_days))
        for i, row in enumerate(_iter_top_models(top_models), start=1):
            lines.append(
                t(
                    lang,
                    "stats_top_models_line",
                    rank=i,
                    model=_truncate_model_name(row["model"]),
                    calls=row["calls"],
                    cost=row["cost_usd"],
                )
            )

    # Stage-15-Step-E #2 follow-up #3: per-day spending breakdown.
    # Rendered as ASCII bars in a fenced code block so the date /
    # bar / cost columns line up under Telegram's variable-width
    # font. Skipped when there's no daily data — same "no row is
    # better than a misleading empty bar chart" policy as the
    # balance line at the top of the screen.
    daily = snapshot.get("daily") or []
    daily_block = _format_daily_bars(daily, lang, window_days=window_days)
    if daily_block:
        lines.append("")
        lines.append(daily_block)
    return "\n".join(lines)


# Width of the bar in characters. Picked so the longest line
# ("YYYY-MM-DD ████…████ $XX.XXXX") stays under ~40 cols on a
# narrow phone — Telegram's monospace fits ~32 chars before the
# code block starts wrapping on iPhone SE-class screens. 16 is
# the widest bar that keeps the whole line readable.
_DAILY_BAR_WIDTH = 16
_DAILY_BAR_FILLED_CHAR = "█"
_DAILY_BAR_EMPTY_CHAR = "░"


def _format_daily_bars(
    daily: Iterable[dict],
    lang: str | None,
    *,
    window_days: int,
) -> str:
    """Render the per-day spending series as an ASCII bar chart.

    Returns ``""`` when *daily* is empty so the caller can decide
    whether to emit a header. Otherwise returns a Markdown
    code-block string with one row per day (newest last).

    The bar width is proportional to ``cost_usd / max(cost_usd)``
    over the visible window. Days with zero cost render as an
    all-empty bar (``░░░░░░░░░░░░░░░░``) so the user can see the
    continuity of dates rather than a sparse "every-other-day"
    chart. Days that aren't present in *daily* but fall inside
    ``[max_date - window_days + 1, max_date]`` are filled with
    zero rows for the same reason.

    Defensive against legacy / corrupted rows: any row missing
    ``date`` / ``cost_usd``, with a non-string date, with a
    non-finite cost, or with a date that doesn't ISO-parse, is
    skipped. ``window_days`` is clamped to the same range as
    ``Database.get_user_daily_spending``'s server-side clamp so a
    buggy caller can't blow the bar count up.
    """
    rows = list(_iter_daily_rows(daily))
    if not rows:
        return ""

    # Pad missing days so the bars form a continuous date axis.
    rows = _pad_missing_days(rows, window_days=window_days)
    max_cost = max((r["cost_usd"] for r in rows), default=0.0)

    body_lines: list[str] = []
    for row in rows:
        cost = row["cost_usd"]
        if max_cost > 0:
            ratio = cost / max_cost
        else:
            ratio = 0.0
        # Round half-up via int(ratio*W + 0.5). At ratio=1 this is
        # exactly W; at ratio=0 it's 0; at ratio=0.5 it's W/2 +
        # rounding. Bounded to [0, W] defensively against a NaN
        # ratio (which can't actually happen here — _iter_daily_rows
        # already drops NaN cost — but bounded math is clearer).
        filled = max(0, min(_DAILY_BAR_WIDTH, int(ratio * _DAILY_BAR_WIDTH + 0.5)))
        bar = (
            _DAILY_BAR_FILLED_CHAR * filled
            + _DAILY_BAR_EMPTY_CHAR * (_DAILY_BAR_WIDTH - filled)
        )
        body_lines.append(
            f"{row['date']}  {bar}  ${cost:.4f}"
        )

    header = t(lang, "stats_daily_header", days=window_days)
    body = "\n".join(body_lines)
    # Triple-backtick fence keeps the columns monospaced. Trailing
    # newline before the closing fence avoids Telegram swallowing
    # the last data row on some clients.
    return f"{header}\n```\n{body}\n```"


def _iter_daily_rows(rows: Iterable[dict]) -> Iterable[dict]:
    """Yield only well-formed rows from *rows*.

    Same defensive shape as :func:`_iter_top_models`. A row whose
    ``date`` doesn't parse, or whose ``cost_usd`` is non-finite /
    non-numeric, is dropped rather than rendered as a broken bar.
    """
    for r in rows:
        if not isinstance(r, dict):
            continue
        raw_date = r.get("date")
        if not isinstance(raw_date, str) or not raw_date:
            continue
        try:
            parsed_date = date.fromisoformat(raw_date)
        except ValueError:
            continue
        cost = _safe_float(r.get("cost_usd"))
        calls = _safe_int(r.get("calls"))
        yield {
            "date": parsed_date.isoformat(),
            "_date_obj": parsed_date,
            "cost_usd": cost,
            "calls": calls,
        }


def _pad_missing_days(
    rows: list[dict],
    *,
    window_days: int,
) -> list[dict]:
    """Fill any gap days inside the rendered window with zero rows.

    The DB query in :meth:`Database.get_user_daily_spending`
    intentionally omits days with no usage to keep the row count
    bounded; the renderer then has to put them back so the date
    axis on the bar chart looks continuous. Spans more than
    *window_days* are truncated to the most-recent *window_days*
    days, defensive against a future caller passing rows older
    than the requested window.
    """
    if not rows:
        return rows
    # Clamp window_days defensively. The DB layer already clamps,
    # but the formatter must not blow up if a hand-built snapshot
    # passes an unreasonable value.
    window_days = max(1, min(int(window_days), 365))

    # Walk the window from oldest -> newest based on the LATEST
    # row's date, NOT today's date. This keeps the chart anchored
    # on the user's most recent activity even if the latest row
    # is several days behind the wall clock — otherwise a user
    # who hasn't chatted in a week would see a chart of all
    # zero-bars with their actual usage scrolled off the bottom.
    latest = rows[-1]["_date_obj"]
    earliest = latest - timedelta(days=window_days - 1)
    by_date = {r["_date_obj"]: r for r in rows if r["_date_obj"] >= earliest}

    padded: list[dict] = []
    cursor = earliest
    while cursor <= latest:
        if cursor in by_date:
            row = by_date[cursor]
            padded.append({
                "date": row["date"],
                "cost_usd": row["cost_usd"],
                "calls": row["calls"],
            })
        else:
            padded.append({
                "date": cursor.isoformat(),
                "cost_usd": 0.0,
                "calls": 0,
            })
        cursor = cursor + timedelta(days=1)
    return padded


def _iter_top_models(rows: Iterable[dict]) -> Iterable[dict]:
    """Yield only well-formed rows from ``rows``.

    A row missing ``model`` / with a non-string model id, or with
    a non-finite ``cost_usd`` / non-finite ``calls``, is skipped
    rather than rendered as a broken line. The DB method
    :meth:`Database.get_user_spending_summary` already coerces both
    fields to plain Python ``int`` / ``float`` so this never fires
    against real DB output, but a future caller passing a
    hand-built snapshot (e.g. a unit test) shouldn't be able to
    crash the formatter — and a corrupted aggregate that *does*
    leak ``Inf`` / ``NaN`` through (operator-injected bogus
    ``cost_deducted_usd`` rows on the DB → ``SUM`` returns
    ``Decimal('Infinity')`` → asyncpg → ``float`` cast → ``inf``)
    must NOT show up as ``$0.0000`` next to a real model name.
    Pre-fix the row was silently coerced to zero by
    :func:`_safe_float`, lying to the user about which model their
    spend went to. Post-fix the row is dropped entirely so the
    "top models" list shrinks rather than misattributes spend.
    """
    for r in rows:
        if not isinstance(r, dict):
            continue
        model = r.get("model")
        if not isinstance(model, str) or not model:
            continue
        # Honour the docstring: a non-finite cost / calls is
        # corruption, not "$0". Drop the row.
        cost_raw = r.get("cost_usd")
        if not _is_finite_number(cost_raw):
            continue
        calls_raw = r.get("calls")
        if not _is_finite_number(calls_raw):
            continue
        yield {
            "model": model,
            "calls": _safe_int(calls_raw),
            "cost_usd": _safe_float(cost_raw),
        }


def _is_finite_number(value: object) -> bool:
    """True iff ``value`` is a finite int/float (and NOT a bool).

    Shared predicate for :func:`_iter_top_models`'s row filter so
    the cost / calls checks stay in lock-step. ``bool`` is
    subclassed off ``int`` in Python, but ``True`` / ``False``
    silently rendering as ``$1.00`` / ``$0.00`` cost was never the
    intent — same explicit rejection :func:`_safe_float` /
    :func:`_safe_int` already do.
    """
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    if isinstance(value, float) and not math.isfinite(value):
        return False
    return True


__all__ = ["format_stats_summary"]
