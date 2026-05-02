"""Conversation history export (Stage-15-Step-E #1).

Renders a user's persisted conversation buffer as one or more
plain-text ``.txt`` files suitable for shipping back as Telegram
documents.

Two public renderers are exposed:

* :func:`format_history_as_text` — single-file mode, returns a
  ``(text, kept_count)`` tuple. Buffers larger than
  :data:`EXPORT_MAX_BYTES` (1 MB) get the *oldest* messages
  trimmed front-first so the most recent context survives. This
  is the legacy entrypoint and is preserved unchanged for
  backward compatibility with callers / tests that already
  consume the single-file shape.
* :func:`format_history_as_text_multipart` — pagination mode
  (Stage-15-Step-E #1 follow-up #2). Returns a list of
  ``(text, kept_count_in_part)`` pairs. A buffer that fits in a
  single part returns a one-element list (callers that always
  iterate behave identically). Larger buffers are split into
  up to :data:`EXPORT_MAX_PARTS` parts, each capped at
  :data:`EXPORT_PART_MAX_BYTES`. Buffers that would exceed the
  total budget (:data:`EXPORT_TOTAL_MAX_BYTES`) get oldest
  messages trimmed first, then split.

The full Step-E #1 spec (still tracked in HANDOFF):

* ``.txt`` export ✅ (single-file + multi-part)
* ``.pdf`` export ❌ (needs ``reportlab`` or ``weasyprint``;
  blocked on operator picking the dep for Persian / RTL
  rendering)
* ``/history`` command alias ✅ (``cmd_history`` in ``handlers``)
* Pagination for very long histories ✅ (this module —
  :func:`format_history_as_text_multipart`)
* Rate limiting on the export action ✅ (the slash-command alias
  shares ``consume_chat_token`` with ``cmd_chat``; the wallet-
  menu callback path relies on Telegram's own callback debounce)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable

log = logging.getLogger(__name__)

# Per-part hard cap. Telegram's document upload comfortably handles
# files much larger than this, but a 1 MB ceiling keeps each part
# fast to download on a mobile connection and predictable for the
# user — and it preserves the legacy single-file behaviour bit-for-
# bit when the buffer fits in one part. 1 MB ≈ 250 000
# 4-character tokens, more than a year of daily heavy use.
EXPORT_PART_MAX_BYTES = 1_000_000

# Maximum number of parts the multi-part renderer will produce.
# 10 × 1 MB = 10 MB, comfortably under Telegram's 50 MB document
# cap and well above any plausible per-user buffer (the running
# context window is bounded by ``MEMORY_CONTENT_MAX_CHARS`` and
# ``MEMORY_CONTEXT_LIMIT`` so even a heavy user with memory ON
# for years will not realistically reach this). When the rendered
# buffer would exceed ``EXPORT_PART_MAX_BYTES * EXPORT_MAX_PARTS``
# the oldest-first trim kicks in to keep the export bounded.
EXPORT_MAX_PARTS = 10

# Total byte budget across every part. Derived from
# :data:`EXPORT_PART_MAX_BYTES` × :data:`EXPORT_MAX_PARTS` so a
# bump to one constant flows through to the other.
EXPORT_TOTAL_MAX_BYTES = EXPORT_PART_MAX_BYTES * EXPORT_MAX_PARTS

# Backward-compat alias: callers / tests that imported
# ``EXPORT_MAX_BYTES`` from the single-file mode continue to see
# the same value (the per-part / single-file cap). The constant
# is also referenced by ``format_history_as_text`` directly so
# the legacy entrypoint's behaviour is unchanged.
EXPORT_MAX_BYTES = EXPORT_PART_MAX_BYTES

# When the database row's ``created_at`` is missing or unparseable
# we fall back to this placeholder so the rendered file still has
# a stable shape. Should be unreachable in production (the column
# has a ``DEFAULT CURRENT_TIMESTAMP``) but defensive against a
# manual DB edit or a future schema change that adds nullability.
_TIMESTAMP_FALLBACK = "(unknown time)"

# Role labels in the rendered output. Kept in English even on the
# Persian side because the model itself emits English ``role``
# values — translating only the label here would be more
# confusing than helpful.
_ROLE_LABELS = {
    "user": "You",
    "assistant": "Assistant",
}


def _coerce_text_field(value: object) -> str:
    """Normalize a row column to a plain ``str`` for the export.

    Stage-15-Step-E #1 follow-up #2 bundled bug fix. Pre-fix,
    :func:`_format_one_message` did ``str(row.get("content", ""))``
    which returns the literal four-character string ``"None"`` when
    the column value is ``None`` (Python's ``str(None)`` is
    ``"None"``, not ``""``). The ``conversation_messages.content``
    column is declared ``TEXT NOT NULL`` so this can't happen on a
    healthy production row, but it DOES surface in (a) test fixtures
    that pass ``content=None`` deliberately to exercise the empty-
    body case, (b) a future schema change that adds nullability,
    (c) a manual SQL fix that nullifies a row mid-incident, and
    (d) any custom ``Row`` shim a future caller passes through
    that returns ``None`` for unknown columns. Same regression
    shape applies to the ``role`` column. The pre-fix behaviour
    polluted the user's exported archive with bogus ``None:`` /
    ``None`` lines instead of falling through to the well-defined
    empty-string + unknown-role placeholders the rest of the
    formatter already supports.

    The helper accepts:

    * ``str`` — passed through unchanged so the existing
      ``rstrip()`` / ``strip()`` treatment in the caller still
      works on the natural shape.
    * ``int`` / ``float`` — coerced via ``str(...)`` for the
      legitimate "numeric content" case (a future ``role=42``
      row would render as ``"42"`` rather than the placeholder,
      matching the caller's existing capitalised-fallback
      convention).
    * everything else (``None``, ``bytes``, ``list``, ``dict``,
      objects) — returns the empty string so the caller's own
      placeholder logic picks up.
    """
    if isinstance(value, str):
        return value
    # ``bool`` is a subclass of ``int`` in Python — refuse it
    # explicitly so a stray ``True`` / ``False`` doesn't render
    # as the ambiguous "1" / "0".
    if isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def _format_timestamp(value: object) -> str:
    """Render a ``created_at`` value in a stable, locale-free
    ``YYYY-MM-DD HH:MM:SS UTC`` shape."""
    if isinstance(value, datetime):
        # Force UTC for predictability — asyncpg returns
        # timezone-aware datetimes, but a naive one would silently
        # render as local time which is hostile when the bot host
        # is in one zone and the user is in another.
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.strftime("%Y-%m-%d %H:%M:%S UTC")
    return _TIMESTAMP_FALLBACK


def _format_one_message(row: dict) -> str:
    role = _coerce_text_field(row.get("role")).strip().lower() or "unknown"
    label = _ROLE_LABELS.get(role, role.capitalize())
    timestamp = _format_timestamp(row.get("created_at"))
    content = _coerce_text_field(row.get("content")).rstrip()
    # A blank-line separator between role-header and body keeps
    # the file readable when the assistant's reply spans many
    # paragraphs of code or markdown.
    return f"[{timestamp}] {label}:\n{content}\n"


def _build_header_lines(
    *,
    user_handle: str | None,
    kept: int,
    dropped: int,
    part_index: int | None = None,
    total_parts: int | None = None,
) -> list[str]:
    """Render the file header.

    ``part_index`` / ``total_parts`` are populated for multi-part
    exports (Stage-15-Step-E #1 follow-up #2) so the user can tell
    which file they're looking at when their downloads folder
    contains several parts side-by-side. Single-file exports omit
    the ``Part:`` line entirely so the legacy header shape is
    preserved bit-for-bit.
    """
    handle_line = f" for @{user_handle}" if user_handle else ""
    lines = [f"Conversation history{handle_line}"]
    if part_index is not None and total_parts is not None and total_parts > 1:
        lines.append(f"Part: {part_index}/{total_parts}")
    lines.append(
        f"Exported: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    suffix = f" (trimmed {dropped} oldest)" if dropped else ""
    lines.append(f"Messages: {kept}{suffix}")
    lines.append("")
    lines.append("—" * 40)
    lines.append("")
    return lines


def _build_part_text(
    *,
    user_handle: str | None,
    rendered_messages: list[str],
    kept: int,
    dropped: int,
    part_index: int | None = None,
    total_parts: int | None = None,
) -> str:
    header = _build_header_lines(
        user_handle=user_handle,
        kept=kept,
        dropped=dropped,
        part_index=part_index,
        total_parts=total_parts,
    )
    return "\n".join(header) + "\n" + "\n".join(rendered_messages)


def format_history_as_text(
    rows: Iterable[dict],
    *,
    user_handle: str | None = None,
) -> tuple[str, int]:
    """Render a user's conversation buffer as a single plain-text
    export.

    Legacy single-file mode. ``rows`` is an iterable of dicts
    shaped like ``{"role": "user"|"assistant", "content": str,
    "created_at": datetime}``. The output begins with a small
    header so the user can identify the file when they re-open
    it months later.

    The result is **never** truncated mid-message — if the
    rendered text would exceed :data:`EXPORT_MAX_BYTES` we drop
    whole *oldest* messages until it fits, prepending a header
    note so the user knows trimming happened.

    Returns ``(text, kept_count)``. ``kept_count`` is the number
    of messages that actually survived the trim (== ``len(rows)``
    when the buffer fits; smaller when the body had to be
    trimmed). The caller is expected to surface ``kept_count`` to
    the user instead of ``len(rows)`` so the caption / toast
    match what's actually in the file.

    For very long buffers prefer
    :func:`format_history_as_text_multipart`, which splits the
    output across multiple files instead of trimming.
    """
    rendered = [_format_one_message(r) for r in rows]
    original_count = len(rendered)

    text = _build_part_text(
        user_handle=user_handle,
        rendered_messages=rendered,
        kept=original_count,
        dropped=0,
    )
    if len(text.encode("utf-8")) <= EXPORT_MAX_BYTES:
        return text, original_count

    # Pre-compute each message's encoded byte size *once* and run a
    # single forward pass dropping from the front while the running
    # sum + the (worst-case) header is still over budget. O(n)
    # bytes processed instead of the O(n²) that the naive shape
    # (re-render + re-encode the whole buffer per dropped message)
    # would burn.
    encoded_sizes = [
        len(piece.encode("utf-8")) + 1  # +1 for the joining "\n"
        for piece in rendered
    ]
    max_header_bytes = (
        len(
            _build_part_text(
                user_handle=user_handle,
                rendered_messages=[],
                kept=0,
                dropped=original_count,
            ).encode("utf-8")
        )
        + 1
    )
    body_budget = max(EXPORT_MAX_BYTES - max_header_bytes, 0)

    dropped = 0
    body_total = sum(encoded_sizes)
    while encoded_sizes and body_total > body_budget:
        body_total -= encoded_sizes[0]
        encoded_sizes.pop(0)
        rendered.pop(0)
        dropped += 1

    text = _build_part_text(
        user_handle=user_handle,
        rendered_messages=rendered,
        kept=original_count - dropped,
        dropped=dropped,
    )
    return text, original_count - dropped


def format_history_as_text_multipart(
    rows: Iterable[dict],
    *,
    user_handle: str | None = None,
) -> list[tuple[str, int]]:
    """Render a user's conversation buffer as one or more
    plain-text exports.

    Stage-15-Step-E #1 follow-up #2: pagination for very long
    buffers. ``rows`` has the same shape as the input to
    :func:`format_history_as_text`.

    The renderer:

    1. Greedy-packs whole messages into parts of up to
       :data:`EXPORT_PART_MAX_BYTES` bytes each (never splits a
       single message across two parts — re-opening "Part 1"
       must always show a self-contained tail).
    2. Caps the total at :data:`EXPORT_MAX_PARTS` parts /
       :data:`EXPORT_TOTAL_MAX_BYTES` bytes. A buffer that would
       exceed the total budget gets oldest messages trimmed
       first; the surviving body is then re-packed into parts.
    3. A single message that is itself larger than
       :data:`EXPORT_PART_MAX_BYTES` (extreme outlier — the
       runtime ``MEMORY_CONTENT_MAX_CHARS`` cap is well below
       this) gets its own part anyway. The part will exceed the
       per-part cap, but Telegram's document upload tolerates
       the overshoot and shipping a giant part is strictly
       better than dropping the message entirely.

    Returns a list of ``(text, kept_count_in_part)`` pairs in
    presentation order (oldest part first). The list is
    guaranteed to be non-empty: an empty input still yields a
    one-part export with a placeholder header (callers can
    short-circuit the empty case before calling this if they
    prefer not to send an empty file). ``sum(kept for _, kept in
    parts)`` equals the total number of messages that survived
    the trim and made it into the export.

    A single-part export reproduces the legacy
    :func:`format_history_as_text` body exactly (same header
    shape, no ``Part:`` line) so callers that flip to the
    multipart entrypoint do not change the user-visible output
    for the common small-buffer case.
    """
    rendered = [_format_one_message(r) for r in rows]
    original_count = len(rendered)

    # Worst-case header size for a multi-part export ever produced
    # — re-rendering with the largest plausible part index /
    # dropped count gives us a safe upper bound.
    worst_header_bytes = (
        len(
            _build_part_text(
                user_handle=user_handle,
                rendered_messages=[],
                kept=0,
                dropped=original_count,
                part_index=EXPORT_MAX_PARTS,
                total_parts=EXPORT_MAX_PARTS,
            ).encode("utf-8")
        )
        + 1
    )
    body_budget_per_part = max(
        EXPORT_PART_MAX_BYTES - worst_header_bytes, 0
    )
    total_body_budget = body_budget_per_part * EXPORT_MAX_PARTS

    # Pre-compute every message's encoded body size (+1 for the
    # joining newline). Same pre-encode-once pattern the legacy
    # trim loop uses to avoid O(n²) re-encoding.
    encoded_sizes = [
        len(piece.encode("utf-8")) + 1 for piece in rendered
    ]

    # Step 1: trim oldest messages while the total exceeds the
    # cross-part budget. We trim BEFORE packing so a buffer
    # marginally over budget doesn't waste a part on a single
    # message that ends up alone in part 11.
    dropped = 0
    body_total = sum(encoded_sizes)
    while encoded_sizes and body_total > total_body_budget:
        body_total -= encoded_sizes[0]
        encoded_sizes.pop(0)
        rendered.pop(0)
        dropped += 1

    if dropped > 0:
        log.info(
            "format_history_as_text_multipart: trimmed %d oldest "
            "messages (kept %d) — original buffer of %d messages "
            "would have exceeded EXPORT_TOTAL_MAX_BYTES",
            dropped, len(rendered), original_count,
        )

    # Step 2: greedy-pack into parts, oldest first. ``part_groups``
    # accumulates ``(message_str, encoded_size)`` pairs.
    part_groups: list[list[tuple[str, int]]] = [[]]
    running = 0
    for piece, size in zip(rendered, encoded_sizes):
        # If the current part has at least one message and adding
        # this one would push it over the per-part budget, start a
        # new part. A single oversize message lands alone in its
        # own part (part_groups[-1] == [] check stays True the
        # first time around).
        if part_groups[-1] and running + size > body_budget_per_part:
            part_groups.append([])
            running = 0
        part_groups[-1].append((piece, size))
        running += size

    # Step 2b: greedy-packing can leave slack at the tail of each
    # part (a message that barely overflows the running total
    # forces a new part with most of the cap unused), so the trim
    # in Step 1 — which used the *full* per-part budget — can
    # leave us with up to ``EXPORT_MAX_PARTS + 1`` part_groups.
    # Drop the oldest groups until we're inside the cap and add
    # those messages to the trim count for part 1's header. The
    # surviving ``part_groups`` is guaranteed to have at most
    # ``EXPORT_MAX_PARTS`` entries so the rest of the renderer
    # never sees an overflow.
    while len(part_groups) > EXPORT_MAX_PARTS:
        dropped_group = part_groups.pop(0)
        dropped += len(dropped_group)
    if not part_groups:
        # Pathological corner case: every message was its own
        # oversize part *and* the cap was 0. Fall back to a
        # single empty part so the caller still gets a non-empty
        # list (matching the empty-rows behaviour at the bottom
        # of this function).
        part_groups = [[]]

    # Step 3: render each part with its own header. The header
    # uses the actual final ``part_index`` / ``total_parts`` so
    # the file the user opens reads correctly; the budget math
    # above used the worst-case header to stay conservative, so
    # the final encoded size is never larger than what we budgeted.
    total_parts = max(len(part_groups), 1)
    parts: list[tuple[str, int]] = []
    for part_index, group in enumerate(part_groups, start=1):
        messages = [piece for piece, _ in group]
        kept_in_part = len(messages)
        # The header line "Messages: N (trimmed M oldest)" only
        # appears in part 1 — printing the trim count on every
        # part would confuse users into thinking each part is its
        # own truncation. Parts 2..N just show their per-part
        # message count.
        part_dropped = dropped if part_index == 1 else 0
        text = _build_part_text(
            user_handle=user_handle,
            rendered_messages=messages,
            kept=kept_in_part,
            dropped=part_dropped,
            part_index=part_index,
            total_parts=total_parts,
        )
        parts.append((text, kept_in_part))

    return parts


def export_filename_for(telegram_id: int) -> str:
    """Stable filename pattern: ``meowassist-history-<telegram_id>-<UTC date>.txt``."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"meowassist-history-{telegram_id}-{today}.txt"


def export_filename_for_part(
    telegram_id: int, part_index: int, total_parts: int
) -> str:
    """Stable filename pattern for a multi-part export.

    Stage-15-Step-E #1 follow-up #2. When ``total_parts == 1`` the
    filename is identical to :func:`export_filename_for` so the
    legacy single-file naming convention is preserved bit-for-bit.
    For multi-part exports the filename includes a
    ``-part-<N>-of-<M>`` suffix so the user can sort the files in
    their downloads folder and immediately see the order.

    Padding ``part_index`` to the same width as ``total_parts``
    (``part-01-of-12.txt`` rather than ``part-1-of-12.txt``) is
    deliberate so a lexicographic sort matches the natural order;
    most file managers default to lexicographic and would otherwise
    place ``part-10`` before ``part-2``. Width is computed from
    ``total_parts`` so a 2-part export still uses ``part-1`` (no
    leading zero) — the minimal width that still sorts correctly.
    """
    if part_index < 1:
        raise ValueError(
            f"part_index must be >= 1, got {part_index!r}"
        )
    if total_parts < 1:
        raise ValueError(
            f"total_parts must be >= 1, got {total_parts!r}"
        )
    if part_index > total_parts:
        raise ValueError(
            f"part_index ({part_index}) > total_parts ({total_parts})"
        )
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if total_parts == 1:
        return f"meowassist-history-{telegram_id}-{today}.txt"
    width = len(str(total_parts))
    return (
        f"meowassist-history-{telegram_id}-{today}"
        f"-part-{part_index:0{width}d}-of-{total_parts}.txt"
    )
