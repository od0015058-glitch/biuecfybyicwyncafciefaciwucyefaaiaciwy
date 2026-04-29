"""refund_columns: refund timestamp + reason on transactions.

Stage-12-Step-A. The admin web panel grew an in-product refund flow
(``POST /admin/transactions/{id}/refund``) that flips a SUCCESS row
to REFUNDED and debits the user's wallet by the credited USD amount.
The new flow needs two extra columns on ``transactions`` for the
forensic record:

* ``refunded_at TIMESTAMP WITH TIME ZONE NULL`` — when the refund
  was applied. Distinct from ``completed_at`` (which is set at the
  original SUCCESS / PARTIAL credit time and stays put through the
  refund — auditors care when both events happened).

* ``refund_reason TEXT NULL`` — the operator-supplied reason. Kept
  free-text rather than enum because operator context (chargeback
  ref id, bank case number, support ticket link, etc.) doesn't fit
  into a fixed taxonomy.

NULL on every existing row — no backfill needed, the columns mean
"this row was refunded via the admin flow" and historical rows
weren't.

``ADD COLUMN IF NOT EXISTS`` so the migration is idempotent
(matches 0002 / 0005 / 0010 / 0011).

Revision ID: 0012_refund_columns
Revises: 0011_tetrapay_locked_rate
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0012_refund_columns"
down_revision = "0011_tetrapay_locked_rate"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE transactions
        ADD COLUMN IF NOT EXISTS refunded_at
            TIMESTAMP WITH TIME ZONE NULL
        """
    )
    op.execute(
        """
        ALTER TABLE transactions
        ADD COLUMN IF NOT EXISTS refund_reason TEXT NULL
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE transactions
        DROP COLUMN IF EXISTS refund_reason
        """
    )
    op.execute(
        """
        ALTER TABLE transactions
        DROP COLUMN IF EXISTS refunded_at
        """
    )
