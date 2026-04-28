"""admin_audit_log + transactions.admin_telegram_id

Stage-9-Step-2 introduces a dedicated audit table for admin actions
plus a small schema fix on the existing wallet-adjustment ledger row.

Schema additions:

* ``admin_audit_log`` — append-only record of every admin action:
    - ``id BIGSERIAL PRIMARY KEY``
    - ``ts TIMESTAMPTZ NOT NULL DEFAULT NOW()``
    - ``actor TEXT NOT NULL``  e.g. ``"web"`` or ``"<telegram_id>"``
    - ``action TEXT NOT NULL`` short slug, e.g. ``login_ok``,
                                 ``promo_create``, ``user_adjust``
    - ``target TEXT NULL``     opaque identifier of the resource
                                 acted upon (``"user:1234"``,
                                 ``"promo:WELCOME20"``, …)
    - ``ip TEXT NULL``          client IP (only set for ``web``
                                 actions where ``request.remote`` /
                                 ``X-Forwarded-For`` is meaningful)
    - ``outcome TEXT NOT NULL`` ``"ok"`` | ``"deny"`` | ``"error"``
    - ``meta JSONB NULL``       free-form structured detail

  Indexed on ``ts DESC`` for the recent-activity feed and on
  ``(actor, ts DESC)`` for "what did this admin do" filters. We
  intentionally do NOT index ``action`` — Postgres's bitmap index
  scan over a few hundred rows is fast enough and cardinality is
  low (< 30 distinct slugs).

* ``transactions.admin_telegram_id`` — explicit BIGINT column so
  the acting admin id is a first-class field instead of being
  buried inside ``gateway_invoice_id`` as
  ``admin-<id>-<ts>-<rand>``. Existing rows retain the legacy
  encoding inside ``gateway_invoice_id`` (downgrade preserves it
  too) — only NEW admin-issued adjustments populate the new column.
  Forensics queries can now do
  ``WHERE admin_telegram_id IS NOT NULL`` instead of substring
  parsing.

Revision ID: 0005_admin_audit_log
Revises: 0004_bot_strings
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0005_admin_audit_log"
down_revision = "0004_bot_strings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_audit_log (
            id          BIGSERIAL PRIMARY KEY,
            ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            actor       TEXT NOT NULL,
            action      TEXT NOT NULL,
            target      TEXT NULL,
            ip          TEXT NULL,
            outcome     TEXT NOT NULL DEFAULT 'ok',
            meta        JSONB NULL
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_admin_audit_ts
            ON admin_audit_log (ts DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_admin_audit_actor_ts
            ON admin_audit_log (actor, ts DESC)
        """
    )

    op.execute(
        """
        ALTER TABLE transactions
            ADD COLUMN IF NOT EXISTS admin_telegram_id BIGINT NULL
        """
    )
    # Useful for "show me all admin-issued adjustments" without
    # paying the full-table scan cost. Partial index because the
    # vast majority of rows are NowPayments invoices where the
    # column is NULL.
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_transactions_admin
            ON transactions (admin_telegram_id)
            WHERE admin_telegram_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_transactions_admin")
    op.execute(
        "ALTER TABLE transactions DROP COLUMN IF EXISTS admin_telegram_id"
    )
    op.execute("DROP INDEX IF EXISTS idx_admin_audit_actor_ts")
    op.execute("DROP INDEX IF EXISTS idx_admin_audit_ts")
    op.execute("DROP TABLE IF EXISTS admin_audit_log")
