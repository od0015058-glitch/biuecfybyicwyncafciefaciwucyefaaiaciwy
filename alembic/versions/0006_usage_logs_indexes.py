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
Revises: 0005_admin_audit_log
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0006_usage_logs_indexes"
down_revision = "0005_admin_audit_log"
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
