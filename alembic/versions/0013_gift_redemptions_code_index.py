"""gift_redemptions per-code index for the redemption drilldown

Stage-12-Step-D. The ``gift_redemptions`` table was created in
``0003_gift_codes`` with two access paths indexed:

* The PRIMARY KEY ``(code, telegram_id)`` — used by
  ``redeem_gift_code`` to detect "this user already redeemed this
  code" inside the open transaction.
* ``idx_gift_redemptions_user`` on ``(telegram_id, redeemed_at DESC)``
  — for per-user gift history.

What was *not* indexed: the ``WHERE code = ? ORDER BY redeemed_at DESC``
access path — i.e. "show me everyone who redeemed THIS code, newest
first". The PK can satisfy the WHERE but the sort it provides is by
``telegram_id`` (the second PK column), not by time, so the query
falls back to a per-code partition scan + in-memory sort. Today
that's a tiny scan; for a popular code redeemed by thousands of
users, the new ``/admin/gifts/{code}/redemptions`` paginated drill-down
would degrade as the redemption count grew.

Schema additions:

* ``idx_gift_redemptions_code_redeemed_at`` on
  ``(code, redeemed_at DESC)`` — matches the canonical access pattern
  for the new drilldown. ``redeemed_at DESC`` aligns with the page's
  ORDER BY so a forward index scan returns rows in display order
  with no extra sort step.

``CREATE INDEX IF NOT EXISTS`` so the migration is idempotent
(matches the repo convention from ``0006_usage_logs_indexes`` and
the original ``0003_gift_codes``). Not created CONCURRENTLY for the
same reason as 0006: alembic runs migrations in a transaction by
default and concurrent index builds require ``autocommit_block``;
the table is small at deploy time and a brief lock on a write-rare
table is the cheaper trade-off.

Revision ID: 0013_gift_redemptions_code_index
Revises: 0012_refund_columns
Create Date: 2026-04-29
"""

from __future__ import annotations

from alembic import op


revision = "0013_gift_redemptions_code_index"
down_revision = "0012_refund_columns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gift_redemptions_code_redeemed_at
            ON gift_redemptions (code, redeemed_at DESC)
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP INDEX IF EXISTS idx_gift_redemptions_code_redeemed_at"
    )
