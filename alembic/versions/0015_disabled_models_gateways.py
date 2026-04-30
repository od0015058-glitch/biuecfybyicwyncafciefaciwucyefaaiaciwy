"""Admin toggles for AI models and payment gateways.

Stage-14-Step-A + Stage-14-Step-B.

Two new tables:

* ``disabled_models`` — stores model ids that the admin has
  disabled via the web panel.  The ``model_id`` column is the
  OpenRouter slug (e.g. ``openai/gpt-4o``).  A row existing means
  the model is hidden from the picker and refused at chat time.
  Deleting the row re-enables it.

* ``disabled_gateways`` — stores gateway/currency keys that the
  admin has disabled.  The ``gateway_key`` column is either
  ``tetrapay`` (the Rial card gateway) or a NowPayments ticker
  (e.g. ``btc``, ``usdttrc20``).  A row existing means the
  currency/gateway is hidden from the payment picker.  Deleting
  the row re-enables it.

Both tables are append-only (INSERT to disable, DELETE to enable),
so re-enabling a model or gateway is a clean row removal rather
than a boolean flip — simpler audit trail and no stale rows.

Revision ID: 0015
Revises: 0014
"""

from alembic import op

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None

UPGRADE_SQL = """\
CREATE TABLE IF NOT EXISTS disabled_models (
    model_id     VARCHAR(255) PRIMARY KEY,
    disabled_by  VARCHAR(64)  NOT NULL DEFAULT 'web',
    disabled_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS disabled_gateways (
    gateway_key  VARCHAR(64) PRIMARY KEY,
    disabled_by  VARCHAR(64) NOT NULL DEFAULT 'web',
    disabled_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

DOWNGRADE_SQL = """\
DROP TABLE IF EXISTS disabled_gateways;
DROP TABLE IF EXISTS disabled_models;
"""


def upgrade() -> None:
    op.execute(UPGRADE_SQL)


def downgrade() -> None:
    op.execute(DOWNGRADE_SQL)
