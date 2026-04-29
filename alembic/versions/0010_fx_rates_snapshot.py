"""fx_rates_snapshot: persist the last-known USD→Toman rate

Stage-11-Step-A. The :mod:`fx_rates` background refresher keeps an
in-memory cache of the current USD→Toman rate, but a fresh process
starts with an empty cache. For the ~10 minutes between boot and
the first refresh, wallet displays (Stage-11-Step-D) and the Toman
top-up path (Stage-11-Step-B) would have NO rate to convert with —
they'd either crash or render "0 TMN", both of which are worse than
serving the last-observed rate with an "(approx)" marker.

This migration introduces ``fx_rates_snapshot`` as a single-row
persistent table (we only ever care about "the latest value"). The
table is upserted on every successful refresh and read once on
process boot to warm the cache.

Schema:

* ``fx_rates_snapshot``
    - ``id INT PRIMARY KEY`` — always ``1`` (single-row convention).
      We use an explicit integer rather than a dedicated
      ``rate_kind`` discriminator because there is exactly one FX
      pair this bot cares about (USD→Toman); a future additional
      pair would warrant a multi-row redesign.
    - ``toman_per_usd DOUBLE PRECISION NOT NULL`` — the rate
      itself, quoted as tomans per USD (NOT rials — the module
      normalises nobitex's rial-denominated output by dividing
      by 10 before caching).
    - ``source TEXT NOT NULL`` — which parser produced this value
      (``nobitex`` / ``bonbast`` / ``custom_static`` / ``db``).
      Useful for the operator to eyeball in logs / admin UI:
      "did yesterday's spike come from Nobitex or from a broken
      bonbast mirror?".
    - ``fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`` — when this
      snapshot was written. The in-memory cache's ``is_stale``
      check relies on this so the wallet UI can decorate the
      Toman figure with "(approx)" when the rate is old.

``CREATE TABLE IF NOT EXISTS`` so the migration is idempotent
(matches 0005–0009).

Revision ID: 0010_fx_rates_snapshot
Revises: 0009_model_prices
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0010_fx_rates_snapshot"
down_revision = "0009_model_prices"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS fx_rates_snapshot (
            id             INT              PRIMARY KEY,
            toman_per_usd  DOUBLE PRECISION NOT NULL,
            source         TEXT             NOT NULL,
            fetched_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS fx_rates_snapshot")
