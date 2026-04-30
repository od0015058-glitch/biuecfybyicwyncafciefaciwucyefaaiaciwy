"""User-facing usage / spending stats (Stage-15-Step-E #2, first slice).

Renders the dict returned by
:meth:`Database.get_user_spending_summary` as a Telegram-ready
Markdown digest for the ``/stats`` command and the inline
``hub_stats`` button on the wallet screen.

Status: **STARTED, not finished.** This module is the first
slice. The full Step-E #2 spec includes:

* Total spent, per-model breakdown ✅ (this module)
* All-time / 7d / 30d windows ✅
* ``/stats`` command + inline button ✅
* Daily / weekly **graphs** ❌ (would need `matplotlib` + an
  image-export step — defer until the operator confirms the
  dependency footprint is acceptable; alternative is ASCII
  bar charts which work in plain Telegram messages)
* Cost-per-token average per model ❌ (data is available — add
  a derived ``avg_cost_per_1k_tokens`` field per row when the
  next AI extends the digest)
* Spending trend (week-over-week delta) ❌ (the 7d / 30d
  windows are absolute; computing the prior 7-day window in
  the same CTE is straightforward — left for the next AI)
* CSV / PDF export of the user's full usage_logs ❌ (parallel
  to the conversation-history export from Step-E #1; reuse
  the same `BufferedInputFile` document-send pattern)

The next AI working on Step-E #2 should pick up where this
module leaves off — DB layer, handler, and i18n strings are
already in place.
"""

from __future__ import annotations

import math

from strings import t

# Hard cap on rendered top-model rows per the
# ``top_n_models`` knob in the DB layer; the digest itself
# never displays more than ``DIGEST_TOP_MODELS`` even if the
# DB returns more (defensive).
DIGEST_TOP_MODELS = 5


def _safe_float(value: object) -> float:
    """Coerce to float, treating NaN / inf as 0.

    Mirrors the same NaN-defence pattern used in
    :mod:`wallet_display` / :mod:`metrics`. A corrupted DB row
    with NaN cost should render as ``$0.00`` rather than ``$nan``
    or crashing the formatter.
    """
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(f):
        return 0.0
    return f


def _safe_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def format_user_stats(summary: dict, lang: str | None) -> str:
    """Render a user's spending summary as a Markdown digest.

    Empty-buffer (zero usage rows) renders the empty-state
    message — the caller (``/stats`` handler) does not need to
    branch on the dict shape.
    """
    total_calls = _safe_int(summary.get("total_calls"))
    if total_calls == 0:
        return t(lang, "user_stats_empty")

    total_spent = _safe_float(summary.get("total_spent_usd"))
    prompt_tokens = _safe_int(summary.get("total_prompt_tokens"))
    completion_tokens = _safe_int(summary.get("total_completion_tokens"))
    spent_7d = _safe_float(summary.get("spent_last_7d_usd"))
    calls_7d = _safe_int(summary.get("calls_last_7d"))
    spent_30d = _safe_float(summary.get("spent_last_30d_usd"))
    calls_30d = _safe_int(summary.get("calls_last_30d"))

    lines = [
        t(lang, "user_stats_title"),
        "",
        t(
            lang,
            "user_stats_total_line",
            calls=f"{total_calls:,}",
            spent=f"${total_spent:.4f}",
        ),
        t(
            lang,
            "user_stats_tokens_line",
            prompt=f"{prompt_tokens:,}",
            completion=f"{completion_tokens:,}",
        ),
        t(
            lang,
            "user_stats_window_line",
            window="7d",
            calls=f"{calls_7d:,}",
            spent=f"${spent_7d:.4f}",
        ),
        t(
            lang,
            "user_stats_window_line",
            window="30d",
            calls=f"{calls_30d:,}",
            spent=f"${spent_30d:.4f}",
        ),
    ]

    top_models = summary.get("top_models") or []
    # Defensive cap — the DB layer already bounds via ``top_n_models``
    # but renders below this constant either way.
    top_models = list(top_models)[:DIGEST_TOP_MODELS]
    if top_models:
        lines.append("")
        lines.append(t(lang, "user_stats_top_models_header"))
        for i, row in enumerate(top_models, start=1):
            model = str(row.get("model") or "(unknown)")
            count = _safe_int(row.get("count"))
            cost = _safe_float(row.get("cost_usd"))
            lines.append(
                f"  {i}. `{model}` — {count:,} calls, ${cost:.4f}"
            )

    return "\n".join(lines)
