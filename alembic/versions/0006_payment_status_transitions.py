"""payment_status_transitions: append-only IPN audit + replay-dedupe

Stage-9-Step-4 introduces a dedicated audit table for every NowPayments
IPN we observe, keyed by ``(gateway_invoice_id, payment_status)`` so
the database itself rejects exact replays of the same status for the
same invoice. The existing row-status guards in
``finalize_payment`` / ``finalize_partial_payment`` /
``mark_transaction_terminal`` already drop most replays, but they leave
no audit trail of which webhook deliveries actually fired vs. which
were dropped. This table closes that gap.

Schema:

* ``payment_status_transitions``
    - ``id BIGSERIAL PRIMARY KEY``
    - ``gateway_invoice_id TEXT NOT NULL`` — the NowPayments
      ``payment_id`` we already use as the join key on ``transactions``.
    - ``payment_status TEXT NOT NULL`` — the IPN ``payment_status``
      value (``finished`` / ``partially_paid`` / ``expired`` / ``failed``
      / ``refunded`` / ``confirming`` / …). Stored verbatim; we don't
      enumerate at the schema level so future NowPayments status
      additions don't require a migration.
    - ``recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`` — when the
      webhook handler observed the IPN. Useful for spotting backdated
      / out-of-order deliveries.
    - ``outcome TEXT NOT NULL`` — what the handler did with the IPN:
      ``"applied"`` (state mutation actually fired),
      ``"replay"`` (this exact ``(invoice, status)`` had been seen
      before so the handler bailed early),
      ``"noop"`` (in-flight / unhandled / informational status —
      e.g. ``confirming`` — no state change).
    - ``meta JSONB NULL`` — free-form structured detail
      (``{"actually_paid_usd": 4.20}``, ``{"price_amount": "10.00"}``,
      …). Aids forensics without requiring a schema change every
      time we want to record a new field.

* ``UNIQUE(gateway_invoice_id, payment_status)`` is the dedupe contract.
  ``record_payment_status_transition`` does ``INSERT … ON CONFLICT DO
  NOTHING RETURNING id`` so two IPN deliveries for the same
  ``(invoice, status)`` pair collapse to one row in the audit and one
  state mutation. The second delivery's caller sees ``RETURNING id``
  return ``NULL`` and bails out before touching ``transactions`` or
  the wallet.

Indexed on ``recorded_at DESC`` for the recent-IPN forensic feed
(`/admin/payment_health` is in the queue but not yet shipped) and
``(gateway_invoice_id, recorded_at DESC)`` for "show me everything
we observed for this invoice". The ``UNIQUE`` constraint already
provides the lookup index for dedupe.

Revision ID: 0006_payment_status_transitions
Revises: 0005_admin_audit_log
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0006_payment_status_transitions"
down_revision = "0005_admin_audit_log"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS payment_status_transitions (
            id                  BIGSERIAL PRIMARY KEY,
            gateway_invoice_id  TEXT NOT NULL,
            payment_status      TEXT NOT NULL,
            recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            outcome             TEXT NOT NULL,
            meta                JSONB NULL,
            CONSTRAINT uq_payment_status_transitions
                UNIQUE (gateway_invoice_id, payment_status)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_payment_status_transitions_recorded_at
            ON payment_status_transitions (recorded_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_payment_status_transitions_invoice
            ON payment_status_transitions (gateway_invoice_id, recorded_at DESC)
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP INDEX IF EXISTS idx_payment_status_transitions_invoice"
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_payment_status_transitions_recorded_at"
    )
    op.execute("DROP TABLE IF EXISTS payment_status_transitions")
