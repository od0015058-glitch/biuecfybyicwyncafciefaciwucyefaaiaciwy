"""User-facing usage-log CSV export (Stage-15-Step-E #2 follow-up).

Renders a user's full ``usage_logs`` history as an RFC-4180 CSV
file suitable for shipping back as a Telegram document.

Pairs with the ``hub_stats`` wallet sub-screen and mirrors the
shape of :mod:`conversation_export` (Stage-15-Step-E #1 follow-up):
pure-function module with no I/O, no DB; the caller fetches the
rows from the DB and hands them in.

Status: **first slice.** Future work tracked in HANDOFF.md
§"Stage-15-Step-E #2 follow-ups":

* ``.xlsx`` export using ``openpyxl`` for a non-technical user
  who'd rather double-click in Finder than fight Excel's CSV
  import wizard. Out of scope for now — adds a 2 MB install
  footprint for a feature CSV already covers.
* Per-period filtering (last 7 days / last month) — currently
  the export covers the full lifetime up to the DB cap. The
  stats screen's window selector could surface a matching
  selector here, but the more common ask is "everything I've
  ever spent on this bot, please" so the lifetime export is
  the right default.
* Stripping the ``model`` column to only the
  ``provider/model_id`` slug if the user has a "private mode"
  preference — moot today (the model is already a slug, not a
  display name).
"""

from __future__ import annotations

import csv
import io
import math
from datetime import datetime, timezone
from typing import Iterable

# Hard upper bound on the rendered CSV. Same 5 MB ceiling Telegram's
# document upload comfortably handles without paid-tier limits, well
# above any realistic per-user usage_logs export (50 000 rows × ~150
# bytes / row = ~7.5 MB raw; the DB caps at 50 000 rows so this only
# clamps when the rows are unusually wide — a model id like
# ``anthropic/claude-3-5-sonnet-20241022`` is 36 bytes vs the
# ``"openai/gpt-4o"`` baseline of 13 bytes — but the byte budget is
# the durable contract).
EXPORT_MAX_BYTES = 5_000_000

# Header row. Order matches the admin-side ``GET /admin/users/{id}/usage``
# table column order so a user comparing the CSV to a screenshot of
# the admin view doesn't have to mentally re-sort. ``id`` is the
# ``usage_logs.log_id`` SERIAL — exposed so a user filing a "this
# charge looks wrong" support ticket can reference an exact row,
# matching the convention from the conversation-history export's
# stable per-message timestamps.
_CSV_COLUMNS: tuple[str, ...] = (
    "id",
    "created_at",
    "model",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "cost_usd",
)


def _format_timestamp_for_csv(value: object) -> str:
    """Render ``created_at`` in a stable ``YYYY-MM-DD HH:MM:SS UTC``
    shape for the CSV cell.

    Same policy as :func:`conversation_export._format_timestamp`:
    a naive datetime is treated as UTC (defensive against a future
    DB schema change that drops the ``timestamptz`` annotation;
    silently rendering as local time would be hostile to a user
    whose host is in a different zone). Returns the empty string
    for ``None`` / unparseable values so the column reads cleanly
    in Excel — a literal ``"unknown"`` placeholder shows up as a
    bogus comparison target if the user sorts by date.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        # The DB layer ``_coerce_usage_log_row`` already calls
        # ``isoformat()`` on the asyncpg datetime; accept the
        # string form too so a unit test can pass a hand-built
        # row without a real datetime object.
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return value
        value = parsed
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.strftime("%Y-%m-%d %H:%M:%S UTC")
    return ""


def _format_cost_for_csv(value: object) -> str:
    """Render a per-row cost cell.

    Six fractional digits matches the
    ``usage_logs.cost_deducted_usd DECIMAL(10,6)`` precision so
    the CSV round-trips losslessly into a spreadsheet (a 4-digit
    rendering would lose sub-cent precision for OpenRouter's
    cheapest models). Non-finite / non-numeric inputs render as
    ``"0.000000"`` — the DB layer's ``_coerce_usage_log_row``
    already scrubs these, but defending here means a hand-built
    test row can't sneak ``"nan"`` into the CSV.
    """
    if isinstance(value, bool):
        # ``bool`` is a subclass of ``int`` in Python — refuse it
        # explicitly so ``True`` / ``False`` doesn't render as
        # ``"1.000000"`` / ``"0.000000"``.
        return "0.000000"
    if not isinstance(value, (int, float)):
        return "0.000000"
    f = float(value)
    if not math.isfinite(f):
        return "0.000000"
    return f"{f:.6f}"


def _format_int_for_csv(value: object) -> str:
    """Render a per-row token-count cell.

    Token columns are ``INT NOT NULL`` in the schema so this is
    almost always a clean ``int``, but the DB scrub policy
    (mirrored at the boundary in ``_coerce_usage_log_row``) means
    we can also see ``0`` from a defensive coercion of a future
    NaN. Negative values are clamped to zero — there's no
    meaningful "negative tokens" semantic.
    """
    if isinstance(value, bool):
        return "0"
    if not isinstance(value, (int, float)):
        return "0"
    if isinstance(value, float) and not math.isfinite(value):
        return "0"
    try:
        return str(max(int(value), 0))
    except (TypeError, ValueError, OverflowError):
        return "0"


def _format_model_for_csv(value: object) -> str:
    """Render the model id cell.

    ``model_used`` is ``VARCHAR NOT NULL`` in the schema; the
    expected value is an OpenRouter slug like
    ``"openai/gpt-4o"``. Coerce to ``str`` so a hand-built row
    passing ``None`` doesn't crash the writer; the result is the
    empty string, which Excel renders as a blank cell.
    """
    if value is None:
        return ""
    return str(value)


def _row_to_csv_tuple(row: dict) -> tuple[str, ...]:
    """Render a single ``usage_logs`` row as a tuple of CSV cells."""
    return (
        _format_int_for_csv(row.get("id")),
        _format_timestamp_for_csv(row.get("created_at")),
        _format_model_for_csv(row.get("model")),
        _format_int_for_csv(row.get("prompt_tokens")),
        _format_int_for_csv(row.get("completion_tokens")),
        _format_int_for_csv(row.get("total_tokens")),
        _format_cost_for_csv(row.get("cost_usd")),
    )


def _write_csv_bytes(rendered_rows: list[tuple[str, ...]]) -> bytes:
    """Serialise ``rendered_rows`` (already-stringified tuples) into
    UTF-8-encoded CSV bytes.

    Uses ``csv.writer`` with the default RFC-4180 dialect so a cell
    containing ``,`` / ``"`` / ``\\n`` is correctly quoted — a
    model name like ``some/model,with-comma`` (hypothetical, but
    OpenRouter's slug schema permits it) won't shift the column
    layout. The BOM (``\\ufeff``) is prepended so Excel auto-
    detects UTF-8 instead of mojibaking a Persian model display
    name on the rare row where one shows up.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(_CSV_COLUMNS)
    for row in rendered_rows:
        writer.writerow(row)
    return ("\ufeff" + buf.getvalue()).encode("utf-8")


def format_usage_logs_as_csv(
    rows: Iterable[dict],
    *,
    user_handle: str | None = None,
) -> tuple[bytes, int]:
    """Render a user's ``usage_logs`` rows as a CSV byte string.

    ``rows`` is an iterable of dicts shaped like the per-row
    output of :meth:`database.Database.export_user_usage_logs`
    (and :meth:`database.Database.list_user_usage_logs`'s
    ``"rows"``)::

        {
            "id": int,
            "created_at": str | None,
            "model": str,
            "prompt_tokens": int,
            "completion_tokens": int,
            "total_tokens": int,
            "cost_usd": float,
        }

    ``user_handle`` is currently unused but accepted for parity
    with :func:`conversation_export.format_history_as_text` —
    a future enhancement could prepend a header comment row
    (``# Exported for @<handle> on <date>``) but that risks
    confusing Excel's auto-detection (some versions treat
    ``#`` as a literal first cell rather than a comment). For
    now the CSV is pure data with no preamble.

    The result is **never** truncated mid-row — if the rendered
    bytes would exceed :data:`EXPORT_MAX_BYTES`, whole *oldest*
    rows are dropped from the front (preserving the most recent
    activity, matching the conversation-export convention) until
    it fits. The caller is expected to surface the returned
    ``kept_count`` instead of ``len(rows)`` so the caption matches
    what's actually in the file — pre-fix a heavy user whose
    export got trimmed under them would see "Usage CSV (50 000
    rows)" while the actual file only contained the most recent
    ~30 000.

    Returns ``(csv_bytes, kept_count)``. ``kept_count`` is the
    number of data rows in the file (excluding the header). When
    the input is empty the result is the header row by itself
    plus ``kept_count == 0``.
    """
    del user_handle  # reserved for future use; see docstring
    rendered_rows = [_row_to_csv_tuple(r) for r in rows]
    original_count = len(rendered_rows)

    csv_bytes = _write_csv_bytes(rendered_rows)
    if len(csv_bytes) <= EXPORT_MAX_BYTES:
        return csv_bytes, original_count

    # Front-trim oldest rows until the body fits. Pre-compute each
    # row's encoded byte size *once* and run a single forward pass
    # — same O(n) pattern the conversation-export trim loop landed
    # in Stage-15-Step-E #1 follow-up #2 to avoid the O(n²) re-
    # encode. For the export-row case the cost is much less per
    # iteration (one ``csv.writer.writerow`` call, ~150 bytes
    # encoded UTF-8) but the algorithmic shape is the same and a
    # 50 000-row export trimming half its body would still burn
    # ~1.8 GB of repeated encoding work in the naive shape.
    #
    # The header row + BOM never get trimmed — those are constant
    # overhead independent of ``rendered_rows``. Computing it once
    # and subtracting from the budget gives the per-row body
    # budget directly.
    header_bytes = _write_csv_bytes([])
    body_budget = EXPORT_MAX_BYTES - len(header_bytes)
    if body_budget < 0:
        body_budget = 0

    # Each row's encoded size — write it standalone through the
    # csv module so quoting / escaping match the final write
    # exactly. ``+ 1`` for the row separator the actual write
    # would produce inside the file.
    encoded_sizes: list[int] = []
    for row in rendered_rows:
        single = io.StringIO()
        csv.writer(single, lineterminator="\n").writerow(row)
        encoded_sizes.append(len(single.getvalue().encode("utf-8")))

    body_total = sum(encoded_sizes)
    dropped = 0
    while encoded_sizes and body_total > body_budget:
        body_total -= encoded_sizes.pop(0)
        rendered_rows.pop(0)
        dropped += 1

    return _write_csv_bytes(rendered_rows), original_count - dropped


def usage_filename_for(telegram_id: int) -> str:
    """Stable filename: ``meowassist-usage-<telegram_id>-<UTC date>.csv``.

    Mirror of :func:`conversation_export.export_filename_for` so
    a user with both files saved sees a consistent naming pattern
    and can sort them together in their downloads folder.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"meowassist-usage-{telegram_id}-{today}.csv"
