"""Stage-15-Step-E #10 follow-up #2 — JSONB-backed image refs on conversation_messages.

Pre-migration: ``conversation_messages.content`` is a plain ``TEXT
NOT NULL`` column carrying the user prompt or assistant reply text.
The Stage-15-Step-E #10 vision integration (PR #129 + the
follow-up that wired it end-to-end) lets the user send photos to a
vision-capable model. The user-facing flow works, but the *memory*
side has a known shortcoming called out explicitly in the
HANDOFF.md "what remains" list for Step-E #10:

  > **Memory persistence for image turns** — the current
  > conversation_messages table stores plaintext content. The
  > integration slice persists the prompt text only (the image is
  > NOT in the schema). A memory-enabled user's vision turn replays
  > as text-only on the next turn — model loses the visual context
  > but keeps the conversational thread.

This migration adds the schema underpinning to fix that. The
follow-up application code (database.append_conversation_message /
get_recent_messages) lands in the same PR so the schema and the
read/write paths ship together. Concretely:

* ``image_data_uris JSONB NULL`` — the per-turn list of
  ``data:image/...;base64,...`` URIs the user sent. Nullable so
  text-only turns (the overwhelming majority) keep their existing
  shape with no rewrite (PostgreSQL ALTER TABLE ADD COLUMN with a
  nullable default is a metadata-only change on PG 11+, no table
  rewrite, instant on a hot table).

  Stored as JSONB rather than ``TEXT[]`` because:
    1. The wider codebase already standardises on JSONB for
       structured row metadata (``admin_audit_log.meta``,
       ``payment_status_transitions.meta``) — we keep the pattern
       consistent so a future refactor that consolidates JSON
       handling can collapse them all.
    2. JSONB tolerates a future schema evolution (per-image
       ``mime_type`` / ``content_hash`` fields, for example)
       without another ALTER TABLE — TEXT[] would need a column
       migration to a struct array, and a JSONB-to-anything
       migration is straightforward whereas array-to-anything is
       not.
    3. Reads come back as a single JSON-decoded list, mirroring
       the helpers that already exist for the ``meta`` columns.

  We deliberately do NOT add an index on this column. The hot
  read path (``get_recent_messages``) filters on
  ``telegram_id, created_at DESC`` (already covered by
  ``conversation_messages_user_created_idx``), then JOINs the
  JSONB payload back into the row. There's no use case for a
  ``WHERE image_data_uris IS NOT NULL`` scan.

The downgrade path drops the column. We do NOT downgrade the
``conversation_messages_user_created_idx`` — it predates this
migration. The column is nullable so dropping it does not
invalidate existing rows; any future re-upgrade re-creates an
empty column, and any rows that were vision turns between the
two upgrades lose their image refs (they are not recoverable
from the dropped column anyway). This is the intended and
documented downgrade contract for an additive nullable JSONB
column.

Revision ID: 0018_image_data_uris
Revises: 0017_openrouter_api_keys

NOTE on the (slightly cryptic) revision id: alembic's ``alembic_version``
table stores the revision in a ``character varying(32)`` column, so any
revision id longer than 32 characters crashes the upgrade with
``value too long for type character varying(32)``. The natural-language
``0018_conversation_image_data_uris`` (33 chars, one over the limit) was
the first attempt and CI caught it on the alembic-roundtrip job.
``0018_image_data_uris`` (20 chars) is the trimmed equivalent.
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0018_image_data_uris"
down_revision = "0017_openrouter_api_keys"
branch_labels = None
depends_on = None


UPGRADE_SQL = """\
ALTER TABLE conversation_messages
    ADD COLUMN IF NOT EXISTS image_data_uris JSONB NULL;
"""

DOWNGRADE_SQL = """\
ALTER TABLE conversation_messages
    DROP COLUMN IF EXISTS image_data_uris;
"""


def upgrade() -> None:
    op.execute(UPGRADE_SQL)


def downgrade() -> None:
    op.execute(DOWNGRADE_SQL)
