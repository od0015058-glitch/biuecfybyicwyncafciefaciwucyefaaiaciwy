"""transactions.notes — free-text audit column for admin adjustments.

Stage-7-Part-2 wires up Telegram-side admin balance ops
(``/admin_credit`` / ``/admin_debit``). Each adjustment writes a
``transactions`` row with ``gateway='admin'`` and ``status='SUCCESS'``
so the wallet ledger stays consistent with NowPayments rows. The
human-readable *reason* the admin typed (e.g. ``"refund for stuck
TRX invoice 4440945140"``) doesn't fit anywhere in the existing
schema (``gateway`` is ``VARCHAR(50)`` and ``gateway_invoice_id`` is
already used for uniqueness). This migration adds an optional
free-text ``notes`` column so the reason stays in the DB next to the
amount.

Forward-compat: nullable, no default — existing rows get NULL,
existing INSERTs that don't reference the column keep working.

Revision ID: 0002_transactions_notes
Revises: 0001_baseline
Create Date: 2026-04-28
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002_transactions_notes"
down_revision: str | Sequence[str] | None = "0001_baseline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE transactions ADD COLUMN notes TEXT NULL")


def downgrade() -> None:
    op.execute("ALTER TABLE transactions DROP COLUMN notes")
