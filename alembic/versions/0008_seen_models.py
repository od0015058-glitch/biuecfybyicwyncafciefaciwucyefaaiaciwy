"""seen_models: persistent registry of OpenRouter model ids we've seen

Stage-10-Step-C. The bot's model catalog is refreshed from OpenRouter
every 24h, but there's no record of which models we've already
*observed* across refreshes. Consequently we can't tell the operator
"hey, OpenAI just shipped ``gpt-5-mini``" — the next refresh just
silently slots the new row into ``Catalog.by_provider`` and moves on.

This migration introduces ``seen_models`` as the persistent "watermark"
the discovery loop diffs against. On every poll the loop computes
``live_catalog_ids - seen_model_ids``; any result is a genuinely new
model that's never been in the catalog before. New ids are written
back to the table and broadcast to every admin in ``ADMIN_USER_IDS``
via a Telegram DM.

First-run semantics: when the table is empty (fresh deploy, just ran
the migration), the loop treats *every* current catalog model as
"already seen" and records them without sending a DM. Otherwise the
first refresh after deploy would spam admins with 200+ models.

Schema:

* ``seen_models``
    - ``model_id TEXT PRIMARY KEY`` — OpenRouter slug, e.g.
      ``openai/gpt-4o-mini``. Kept verbatim so a future feature that
      wants to correlate this table with ``Catalog.models`` or
      ``pricing.MODEL_PRICES`` can do so without a transform.
    - ``first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`` — when we
      first saw this id. Useful for the admin UI's "new this week"
      filter and for the operator to eyeball when a provider pushed
      a new model.

No index on ``first_seen_at`` — the loop only ever fetches the full
set of ``model_id`` values (for the diff), and any admin UI pagination
would sort client-side once we surface this as a list. If that
changes we can add one in a future migration.

``CREATE TABLE IF NOT EXISTS`` so the migration is idempotent (matches
the project convention from ``0005_admin_audit_log`` /
``0006_payment_status_transitions`` / ``0007_broadcast_jobs``).

Revision ID: 0008_seen_models
Revises: 0007_broadcast_jobs
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0008_seen_models"
down_revision = "0007_broadcast_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_models (
            model_id      TEXT        PRIMARY KEY,
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS seen_models")
