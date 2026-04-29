"""tetrapay_locked_rate: per-invoice FX rate lock for the TetraPay Rial gateway.

Stage-11-Step-C. The TetraPay (Rial card / Shaparak) gateway settles in
IRR; the wallet stays denominated in USD. Without a per-invoice rate
lock the user could enter ``$5`` worth of toman at one rate, complete
3DS verification a few minutes later (Iranian banks regularly take
multiple minutes for Shaparak round-trips), and we'd recompute the
USD figure at *settlement-time* rate — robbing the user if the rial
weakened in the gap.

Fix: capture the live USD→Toman rate at order-creation time and store
it on the ``transactions`` row. At settlement we credit
``amount_usd_credited`` (the locked USD figure) verbatim; the locked
rate is recorded for forensic purposes only ("was the rate fair?
what rate did we promise the user?").

Crypto rows leave the column NULL — NowPayments quotes the
crypto-to-USD conversion on its own side, so there's nothing for us
to lock.

Schema:

* ``transactions``
    - ``gateway_locked_rate_toman_per_usd DECIMAL(20, 4) NULL`` — the
      USD→Toman rate (tomans per 1 USD, NOT rials) captured at
      order-creation time for the row's gateway. NULL for any
      gateway that doesn't need a rate lock (today: NowPayments;
      future: any USD-quoted PSP).

      Stored as ``DECIMAL(20, 4)`` rather than ``DOUBLE PRECISION``
      to match the existing money-shaped columns on this table
      (``amount_usd_credited DECIMAL(10, 4)``,
      ``amount_crypto_or_rial DECIMAL(20, 8)``) — keeps the audit
      story consistent. The four-decimal precision is overkill
      (USD/Toman quotes in practice are integer tomans) but free
      and lets us round-trip the float cache value without
      precision loss.

      The plausibility band enforced upstream by
      :func:`fx_rates._is_plausible` is ``[10 000, 1 000 000]``
      tomans per USD, well within ``DECIMAL(20, 4)``'s range.

``ADD COLUMN IF NOT EXISTS`` so the migration is idempotent (matches
0002 / 0005 / 0010).

Revision ID: 0011_tetrapay_locked_rate
Revises: 0010_fx_rates_snapshot
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0011_tetrapay_locked_rate"
down_revision = "0010_fx_rates_snapshot"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE transactions
        ADD COLUMN IF NOT EXISTS gateway_locked_rate_toman_per_usd
            DECIMAL(20, 4) NULL
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE transactions
        DROP COLUMN IF EXISTS gateway_locked_rate_toman_per_usd
        """
    )
