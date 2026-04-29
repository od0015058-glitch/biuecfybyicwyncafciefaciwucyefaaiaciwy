"""model_prices: persistent snapshot of last-observed OpenRouter prices

Stage-10-Step-D. The live OpenRouter catalog (``models_catalog``)
already refreshes prices every 24h in-memory, but nothing persists
them — so the bot can't tell the operator "OpenAI just raised
``gpt-4o-mini`` input prices by 40%". Between restarts, price history
is lost entirely; between refreshes within one process, we simply
overwrite the in-memory snapshot.

This migration introduces ``model_prices`` as the persistent "last
known" snapshot. Every discovery pass (see
``model_discovery.run_discovery_pass``) now:

1. Force-refreshes the live catalog (``models_catalog.force_refresh``)
   so prices are current within the cadence of the discovery loop —
   NOT the 24h in-memory TTL.
2. Reads the prior snapshot from this table.
3. Computes significant-delta events (input or output per-1M moved by
   more than ``PRICE_ALERT_THRESHOLD_PERCENT``, default 20%).
4. DMs every admin about the deltas (separate DM from the new-model
   discovery DM so the two alerts can be read independently).
5. Upserts the new prices back to this table.

Schema:

* ``model_prices``
    - ``model_id TEXT PRIMARY KEY`` — OpenRouter slug, e.g.
      ``openai/gpt-4o-mini``. Verbatim so a future feature that
      cross-references ``seen_models`` (Stage-10-Step-C) or
      ``pricing.MODEL_PRICES`` can do so directly.
    - ``input_per_1m_usd DOUBLE PRECISION NOT NULL`` — last observed
      price for 1M input tokens. Raw OpenRouter number (NOT
      marked-up) — the markup is applied at display time by
      ``pricing.apply_markup_to_price``.
    - ``output_per_1m_usd DOUBLE PRECISION NOT NULL`` — same, output
      side.
    - ``last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`` — when we
      last wrote this snapshot. Useful for the operator to eyeball
      when a stale-looking price was last refreshed; a future admin
      UI could surface this.

No index beyond the primary key — the discovery loop only ever
fetches the full set (for the diff) or upserts per-model. If a future
admin UI needs to sort by ``last_seen_at`` we can add an index
later.

``CREATE TABLE IF NOT EXISTS`` so the migration is idempotent
(matches the project convention from 0005-0008).

Revision ID: 0009_model_prices
Revises: 0008_seen_models
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0009_model_prices"
down_revision = "0008_seen_models"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS model_prices (
            model_id           TEXT             PRIMARY KEY,
            input_per_1m_usd   DOUBLE PRECISION NOT NULL,
            output_per_1m_usd  DOUBLE PRECISION NOT NULL,
            last_seen_at       TIMESTAMPTZ      NOT NULL DEFAULT NOW()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS model_prices")
