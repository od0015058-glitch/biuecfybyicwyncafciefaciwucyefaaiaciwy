"""Tests for ``usage_csv_export`` (Stage-15-Step-E #2 follow-up).

Covers the pure formatter — DB-query tests live in
``test_database_queries.py`` (alongside the existing
``list_user_usage_logs`` tests) and handler-wiring tests live in
``test_handlers_usage_csv.py``.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone

import pytest

from usage_csv_export import (
    EXPORT_MAX_BYTES,
    format_usage_logs_as_csv,
    usage_filename_for,
)


def _row(
    *,
    log_id: int = 1,
    created_at: object = "2026-01-02T03:04:05+00:00",
    model: str = "openai/gpt-4o",
    prompt_tokens: object = 10,
    completion_tokens: object = 20,
    total_tokens: object = None,
    cost_usd: object = 0.0042,
) -> dict:
    """Build a usage_logs row dict matching ``Database.export_user_usage_logs``.

    ``total_tokens`` defaults to ``prompt_tokens + completion_tokens``
    when both are int/float; otherwise it falls back to ``0`` so a
    test passing a non-numeric ``prompt_tokens`` (NaN, None, str)
    doesn't blow up the helper itself.
    """
    if total_tokens is None:
        if (
            isinstance(prompt_tokens, (int, float))
            and isinstance(completion_tokens, (int, float))
            and not isinstance(prompt_tokens, bool)
            and not isinstance(completion_tokens, bool)
        ):
            total_tokens = prompt_tokens + completion_tokens
        else:
            total_tokens = 0
    return {
        "id": log_id,
        "created_at": created_at,
        "model": model,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "cost_usd": cost_usd,
    }


def _parse_csv(csv_bytes: bytes) -> tuple[list[str], list[list[str]]]:
    """Strip BOM, decode, and parse via ``csv.reader`` so tests can
    assert on the actual round-tripped values rather than chasing
    quoting / escaping by hand."""
    text = csv_bytes.decode("utf-8")
    if text.startswith("\ufeff"):
        text = text[1:]
    rows = list(csv.reader(io.StringIO(text)))
    assert rows, "CSV must always contain at least the header row"
    return rows[0], rows[1:]


# ---------------------------------------------------------------------
# format_usage_logs_as_csv — happy paths
# ---------------------------------------------------------------------


def test_format_renders_header_in_documented_column_order():
    """The header row order is the user-facing contract — it matches
    the admin-side ``GET /admin/users/{id}/usage`` table column order
    so a user comparing CSV → screenshot doesn't have to mentally
    re-sort. Pin it."""
    csv_bytes, kept = format_usage_logs_as_csv([])
    header, data = _parse_csv(csv_bytes)
    assert header == [
        "id",
        "created_at",
        "model",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "cost_usd",
    ]
    assert data == []
    assert kept == 0


def test_format_renders_one_row_per_input_row():
    rows = [
        _row(log_id=1, prompt_tokens=10, completion_tokens=20),
        _row(log_id=2, prompt_tokens=5, completion_tokens=7),
    ]
    csv_bytes, kept = format_usage_logs_as_csv(rows)
    _, data = _parse_csv(csv_bytes)
    assert kept == 2
    assert len(data) == 2
    assert [r[0] for r in data] == ["1", "2"]


def test_format_renders_cost_with_six_fractional_digits():
    """``cost_deducted_usd`` is ``DECIMAL(10,6)`` in the schema —
    a 4-digit rendering would lose sub-cent precision for
    OpenRouter's cheapest models. Pin the precision."""
    csv_bytes, _ = format_usage_logs_as_csv([_row(cost_usd=0.000123)])
    _, data = _parse_csv(csv_bytes)
    assert data[0][6] == "0.000123"


def test_format_renders_total_tokens_as_provided():
    """The total_tokens column round-trips the input — the
    formatter trusts the DB layer's pre-summed value rather
    than re-computing prompt + completion (the DB mapper is
    the single source of truth so a future schema change that
    decouples the columns doesn't have a divergent renderer to
    chase)."""
    csv_bytes, _ = format_usage_logs_as_csv([
        _row(prompt_tokens=10, completion_tokens=20),
    ])
    _, data = _parse_csv(csv_bytes)
    # column index 5 == total_tokens
    assert data[0][5] == "30"


def test_format_renders_timestamp_in_utc_iso_form_for_string_input():
    csv_bytes, _ = format_usage_logs_as_csv([
        _row(created_at="2026-01-02T03:04:05+00:00"),
    ])
    _, data = _parse_csv(csv_bytes)
    assert data[0][1] == "2026-01-02 03:04:05 UTC"


def test_format_renders_timestamp_for_datetime_input():
    """A hand-built test passing a real ``datetime`` object should
    render exactly the same as the asyncpg path (which calls
    ``isoformat()`` first)."""
    ts = datetime(2026, 5, 10, 12, 30, 0, tzinfo=timezone.utc)
    csv_bytes, _ = format_usage_logs_as_csv([_row(created_at=ts)])
    _, data = _parse_csv(csv_bytes)
    assert data[0][1] == "2026-05-10 12:30:00 UTC"


def test_format_renders_naive_datetime_as_utc():
    """Defensive — a naive datetime is treated as UTC. Matches the
    ``conversation_export._format_timestamp`` policy: silently
    rendering as local time would be hostile to a user whose host
    is in a different zone."""
    naive = datetime(2026, 5, 10, 12, 30, 0)
    csv_bytes, _ = format_usage_logs_as_csv([_row(created_at=naive)])
    _, data = _parse_csv(csv_bytes)
    assert data[0][1] == "2026-05-10 12:30:00 UTC"


def test_format_renders_non_utc_datetime_converted_to_utc():
    """A timezone-aware datetime in some non-UTC zone is converted
    to UTC before formatting so the CSV's date column reads as a
    single canonical clock."""
    from datetime import timedelta
    tz = timezone(timedelta(hours=3, minutes=30))  # Iran tz
    aware = datetime(2026, 5, 10, 12, 30, 0, tzinfo=tz)
    csv_bytes, _ = format_usage_logs_as_csv([_row(created_at=aware)])
    _, data = _parse_csv(csv_bytes)
    assert data[0][1] == "2026-05-10 09:00:00 UTC"


def test_format_renders_empty_string_for_none_timestamp():
    """``None`` ``created_at`` becomes the empty cell so Excel sort-by-
    date doesn't mix in a literal "(unknown)" placeholder."""
    csv_bytes, _ = format_usage_logs_as_csv([_row(created_at=None)])
    _, data = _parse_csv(csv_bytes)
    assert data[0][1] == ""


def test_format_passes_unparseable_timestamp_string_through():
    """An unparseable string drops back to the literal value (not
    "" — the caller might be a future caller storing custom labels
    we don't recognize). Defensive but not aggressive."""
    csv_bytes, _ = format_usage_logs_as_csv([
        _row(created_at="not-a-date"),
    ])
    _, data = _parse_csv(csv_bytes)
    assert data[0][1] == "not-a-date"


# ---------------------------------------------------------------------
# format_usage_logs_as_csv — defensive coercions
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_cost",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
        None,
        "not-a-number",
        True,  # bool subclass of int — refuse explicitly
    ],
)
def test_format_renders_zero_for_non_finite_or_non_numeric_cost(bad_cost):
    """Defense-in-depth — the DB layer scrubs already, but a hand-built
    test row can't sneak ``"nan"`` into the user-visible CSV."""
    csv_bytes, _ = format_usage_logs_as_csv([_row(cost_usd=bad_cost)])
    _, data = _parse_csv(csv_bytes)
    assert data[0][6] == "0.000000"


@pytest.mark.parametrize(
    "bad_int",
    [
        float("nan"),
        float("inf"),
        None,
        "not-a-number",
        True,
        -5,  # negative tokens have no semantic meaning
    ],
)
def test_format_renders_zero_for_non_finite_or_non_numeric_tokens(bad_int):
    csv_bytes, _ = format_usage_logs_as_csv([
        _row(prompt_tokens=bad_int),
    ])
    _, data = _parse_csv(csv_bytes)
    # column index 3 == prompt_tokens
    assert data[0][3] == "0"


def test_format_renders_empty_string_for_none_model():
    csv_bytes, _ = format_usage_logs_as_csv([_row(model=None)])
    _, data = _parse_csv(csv_bytes)
    assert data[0][2] == ""


def test_format_quotes_model_with_comma():
    """Defensive — RFC-4180 quoting must survive a comma in the
    model field (OpenRouter's slug schema doesn't currently emit
    them, but the CSV writer must not shift columns if it ever
    does)."""
    csv_bytes, _ = format_usage_logs_as_csv([
        _row(model="some/model,with-comma"),
    ])
    _, data = _parse_csv(csv_bytes)
    assert data[0][2] == "some/model,with-comma"


def test_format_quotes_model_with_quotes():
    """``"`` in a model id must be properly escaped (RFC-4180
    quote-doubling) so the CSV doesn't desync."""
    csv_bytes, _ = format_usage_logs_as_csv([
        _row(model='some/"weird"/model'),
    ])
    _, data = _parse_csv(csv_bytes)
    assert data[0][2] == 'some/"weird"/model'


def test_format_quotes_model_with_newline():
    """Embedded newlines are CSV-quoted. A row with ``\\n`` in the
    model wouldn't shift the row count of the rendered file."""
    csv_bytes, _ = format_usage_logs_as_csv([
        _row(model="some\nmodel"),
    ])
    rows = list(csv.reader(io.StringIO(
        csv_bytes.decode("utf-8").lstrip("\ufeff")
    )))
    # 1 header + 1 data row, even with the embedded newline.
    assert len(rows) == 2
    assert rows[1][2] == "some\nmodel"


# ---------------------------------------------------------------------
# format_usage_logs_as_csv — encoding / BOM
# ---------------------------------------------------------------------


def test_format_starts_with_utf8_bom_for_excel_auto_detection():
    """Excel only auto-detects UTF-8 if the BOM is present —
    otherwise a Persian model display name (rare but possible)
    mojibakes."""
    csv_bytes, _ = format_usage_logs_as_csv([])
    assert csv_bytes.startswith(b"\xef\xbb\xbf")


def test_format_uses_lf_line_terminator_consistently():
    """Pin the ``\\n`` line terminator so a future caller diffing
    two exports doesn't have to chase ``\\r\\n`` vs ``\\n``
    drift between platforms."""
    csv_bytes, _ = format_usage_logs_as_csv([_row()])
    text = csv_bytes.decode("utf-8").lstrip("\ufeff")
    assert "\r\n" not in text
    assert text.count("\n") == 2  # header + 1 data row


# ---------------------------------------------------------------------
# format_usage_logs_as_csv — front-trim semantics
# ---------------------------------------------------------------------


def test_format_returns_kept_count_equal_to_input_when_under_budget():
    rows = [_row(log_id=i) for i in range(100)]
    _, kept = format_usage_logs_as_csv(rows)
    assert kept == 100


def test_format_front_trims_oldest_when_exceeds_budget(monkeypatch):
    """When the rendered CSV exceeds ``EXPORT_MAX_BYTES`` the
    formatter must drop the OLDEST rows (front of the iterable —
    matches the conversation-export convention so a heavy user
    keeps their most-recent activity)."""
    # Force a tiny budget so we can trigger the trim path with a
    # small number of rows.
    monkeypatch.setattr(
        "usage_csv_export.EXPORT_MAX_BYTES", 500
    )
    # Each row is ~80 bytes encoded, so ~6 rows fit. 100 rows
    # well exceeds the budget.
    rows = [_row(log_id=i, model="openai/gpt-4o") for i in range(100)]
    csv_bytes, kept = format_usage_logs_as_csv(rows)
    _, data = _parse_csv(csv_bytes)
    assert kept < 100
    assert kept == len(data)
    # Most-recent rows are kept (front-trim drops oldest).
    last_id = int(data[-1][0])
    assert last_id == 99
    # First kept row is some id > 0 (the dropped front).
    first_id = int(data[0][0])
    assert first_id > 0
    # Result is under the budget.
    assert len(csv_bytes) <= 500


def test_format_export_max_bytes_is_a_meaningful_default():
    """Pin the documented 5 MB ceiling so a future drift in the
    constant is a deliberate change, not an accidental edit."""
    assert EXPORT_MAX_BYTES == 5_000_000


def test_format_returns_header_only_when_input_is_empty():
    csv_bytes, kept = format_usage_logs_as_csv([])
    text = csv_bytes.decode("utf-8").lstrip("\ufeff")
    assert kept == 0
    assert text.strip().count("\n") == 0  # only the header line
    assert text.startswith("id,created_at,model,")


def test_format_user_handle_argument_is_accepted_but_ignored():
    """``user_handle`` is reserved for future use (header comment).
    Accept it without raising so a caller passing it doesn't break,
    but don't render it into the CSV today."""
    csv_bytes_a, _ = format_usage_logs_as_csv([_row()])
    csv_bytes_b, _ = format_usage_logs_as_csv(
        [_row()], user_handle="alice"
    )
    assert csv_bytes_a == csv_bytes_b


# ---------------------------------------------------------------------
# usage_filename_for
# ---------------------------------------------------------------------


def test_usage_filename_for_uses_stable_pattern():
    name = usage_filename_for(123)
    assert name.startswith("meowassist-usage-123-")
    assert name.endswith(".csv")
    # Date is in YYYY-MM-DD form
    date_part = name[len("meowassist-usage-123-"):-len(".csv")]
    # Parses cleanly as a date
    datetime.strptime(date_part, "%Y-%m-%d")


# ---------------------------------------------------------------------
# String registration — ensure the new slugs landed in both locales
# ---------------------------------------------------------------------


def test_usage_csv_export_strings_present_in_both_locales():
    """A regression-pin against silent string-registration drift.
    The PR adds 4 new slugs; if a future refactor accidentally
    drops any, the test catches it."""
    from strings import _STRINGS as STRINGS

    for slug in (
        "btn_export_usage_csv",
        "usage_csv_export_empty",
        "usage_csv_export_caption",
        "usage_csv_export_done",
    ):
        assert slug in STRINGS["fa"], (
            f"missing FA copy for {slug!r}"
        )
        assert slug in STRINGS["en"], (
            f"missing EN copy for {slug!r}"
        )
