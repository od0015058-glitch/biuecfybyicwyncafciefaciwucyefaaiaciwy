"""Conversation history export (Stage-15-Step-E #1, first slice).

Renders a user's persisted conversation buffer as a plain-text
``.txt`` file suitable for shipping back as a Telegram document.

Status: **STARTED, not finished.** This module is the minimal
viable first slice. The full Step-E #1 spec includes:

* ``.txt`` export ✅ (this module)
* ``.pdf`` export ❌ (not yet — needs ``reportlab`` or ``weasyprint``;
  add as a separate dep + branch in ``format_history`` once the
  product team confirms which library is acceptable for the
  Persian / RTL rendering case)
* ``/history`` command alias ❌ (the menu entry on the memory
  screen is wired up; an explicit ``/history`` command is a
  one-liner ``router.message`` handler the next AI can add)
* Pagination for very long histories ❌ (the current
  ``MEMORY_CONTENT_MAX_CHARS`` × default 30-message context
  window keeps the file well under Telegram's 50 MB document
  cap, but a heavy user with memory ON for weeks could
  accumulate thousands of rows; chunk into multiple files when
  the rendered text would exceed ~10 MB)
* Rate limiting on the export action ❌ (right now the menu
  button is the only entry point and Telegram's own
  callback-debouncing provides a soft cap; if a ``/history``
  text command is added, gate it behind the same chat-token
  bucket as ``cmd_chat``)

The next AI working on Step-E #1 should pick up where this
module leaves off — the schema and the user-facing entry point
are already in place.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

# Hard upper bound on the rendered output. Well below Telegram's
# 50 MB document cap, well above any realistic per-user buffer
# (1 MB ≈ 250 000 4-character tokens, more than a year of daily
# heavy use). If a user somehow exceeds this we truncate at the
# *front* (oldest first) so the most recent context survives.
EXPORT_MAX_BYTES = 1_000_000

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
    role = str(row.get("role", "")).strip().lower() or "unknown"
    label = _ROLE_LABELS.get(role, role.capitalize())
    timestamp = _format_timestamp(row.get("created_at"))
    content = str(row.get("content", "")).rstrip()
    # A blank-line separator between role-header and body keeps
    # the file readable when the assistant's reply spans many
    # paragraphs of code or markdown.
    return f"[{timestamp}] {label}:\n{content}\n"


def format_history_as_text(
    rows: Iterable[dict],
    *,
    user_handle: str | None = None,
) -> tuple[str, int]:
    """Render a user's conversation buffer as a plain-text export.

    ``rows`` is an iterable of dicts shaped like
    ``{"role": "user"|"assistant", "content": str, "created_at":
    datetime}``. The output begins with a small header so the user
    can identify the file when they re-open it months later.

    The result is **never** truncated mid-message — if the rendered
    text would exceed :data:`EXPORT_MAX_BYTES` we drop whole
    *oldest* messages until it fits, prepending a one-line note so
    the user knows trimming happened.

    Returns ``(text, kept_count)``. ``kept_count`` is the number of
    messages that actually survived the trim (== ``len(rows)`` when
    the buffer fits; smaller when the body had to be trimmed). The
    caller is expected to surface ``kept_count`` to the user
    instead of ``len(rows)`` so the caption / toast match what's
    actually in the file — pre-fix the handler reported the
    untrimmed input count to a heavy user whose buffer just got
    rewritten under them ("Conversation history (1500 messages)"
    when the .txt only contained the most recent ~500).
    """
    rendered = [_format_one_message(r) for r in rows]
    original_count = len(rendered)
    handle_line = f" for @{user_handle}" if user_handle else ""

    def _build(dropped: int) -> str:
        kept = original_count - dropped
        header = [
            f"Conversation history{handle_line}",
            f"Exported: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"Messages: {kept}"
            + (f" (trimmed {dropped} oldest)" if dropped else ""),
            "",
            "—" * 40,
            "",
        ]
        return "\n".join(header) + "\n" + "\n".join(rendered)

    text = _build(0)
    if len(text.encode("utf-8")) <= EXPORT_MAX_BYTES:
        return text, original_count

    # Truncate from the front (oldest messages first) until the
    # body fits. The header always reflects the *kept* count plus
    # the explicit "(trimmed N oldest)" suffix so the user can
    # see exactly how much was dropped.
    dropped = 0
    while rendered and len(text.encode("utf-8")) > EXPORT_MAX_BYTES:
        rendered.pop(0)
        dropped += 1
        text = _build(dropped)
    return text, original_count - dropped


def export_filename_for(telegram_id: int) -> str:
    """Stable filename pattern: ``meowassist-history-<telegram_id>-<UTC date>.txt``."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"meowassist-history-{telegram_id}-{today}.txt"
