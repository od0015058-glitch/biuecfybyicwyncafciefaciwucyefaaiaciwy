"""usage_logs indexes for per-user usage browser

Stage-9-Step-8. The ``usage_logs`` table has been written to since
P0 but never indexed beyond its primary key — every per-user query
("show me this admin's last 50 calls") was a sequential scan. Today
that's a few-hundred-row scan; at 1M+ rows it becomes a tail
latency problem on the new ``/admin/users/{id}/usage`` browser.

Schema additions:

* ``idx_usage_logs_telegram_created`` on
  ``(telegram_id, created_at DESC)`` — the canonical access pattern
  for the per-user browser. ``created_at DESC`` matches the page's
  ORDER BY so a forward index scan returns rows in display order.

* ``idx_usage_logs_created`` on ``(created_at DESC)`` — for global
  reports / dashboards that ask "last N calls across the whole
  fleet" without filtering by user.

Both are plain B-tree indexes; ``CREATE INDEX IF NOT EXISTS`` so
the migration is idempotent (matches the project convention from
``0005_admin_audit_log``). NOT created CONCURRENTLY because Alembic
runs migrations inside a transaction by default — concurrent index
builds require ``autocommit_block`` and aren't worth the complexity
for tables this small at deploy time.

Revision ID: 0006_usage_logs_indexes
Revises: 0006_payment_status_transitions

The parent revision is the *other* 0006-prefixed migration
(``0006_payment_status_transitions``). Both were added concurrently —
Stage-9-Step-4 (IPN replay-dedupe table) and Stage-9-Step-8 (this
one, indexes on ``usage_logs``) — and both originally chained off
``0005_admin_audit_log``. That left the alembic graph with two heads,
which blocks ``alembic upgrade head`` with::

    Multiple head revisions are present for given argument 'head'

Linearizing here (this migration depends on the IPN replay-dedupe
one) is the cheap fix: nothing in this migration *needs* the
``payment_status_transitions`` table, and nothing in that migration
touches ``usage_logs``, so the order is essentially arbitrary. We
picked this direction because in production a deploy that's
been running since before either 0006 was merged will already be at
``0005_admin_audit_log``; ``alembic upgrade head`` from there will
first apply ``0006_payment_status_transitions`` (a CREATE TABLE,
fast) and then this migration (two CREATE INDEX statements, also
fast). No data backfill is involved either way.
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0006_usage_logs_indexes"
down_revision = "0006_payment_status_transitions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_usage_logs_telegram_created
            ON usage_logs (telegram_id, created_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_usage_logs_created
            ON usage_logs (created_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_usage_logs_created")
    op.execute("DROP INDEX IF EXISTS idx_usage_logs_telegram_created")
