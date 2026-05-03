"""Per-gateway model disable cross-table.

Stage-15-Step-E #10b row 30. The admin already has a global
disable for AI models (``disabled_models``) and a global disable
for payment gateways / currencies (``disabled_gateways``); see
``alembic/versions/0015_disabled_models_gateways.py``. This
migration adds the **cross-table** that lets the admin disable
a model *only* when the chosen payment gateway matches a
specific value — e.g. block ``openai/gpt-4o`` only when the user
funds via ``zarinpal``, while leaving every other gateway free
to top up a wallet that uses GPT-4o.

Schema rationale
================

* ``model_id VARCHAR(255)`` and ``gateway_key VARCHAR(64)``
  match the column shapes in ``disabled_models.model_id`` and
  ``disabled_gateways.gateway_key`` respectively (alembic 0015),
  so the cross-table can be JOIN-ed against either parent
  without an implicit cast. We deliberately do NOT add a
  formal ``REFERENCES`` foreign key here — the parent tables
  store *disabled* rows, and the cross-table refers to the
  *catalog* of valid model ids / gateway keys (which lives in
  ``models_catalog`` for models and a Python frozenset for
  gateways), not the disabled rows. A FK against the disabled
  parents would be semantically wrong (it would only allow
  cross-blocking models / gateways that are themselves already
  disabled).

* The composite primary key on ``(model_id, gateway_key)`` lets
  the admin toggle endpoint use a single ``ON CONFLICT DO
  NOTHING`` insert / single-row delete (mirrors the parent
  tables' append-only style — INSERT to disable, DELETE to
  enable, no boolean flips).

* ``disabled_by`` and ``disabled_at`` mirror the parent tables
  so the cross-grid view on ``/admin/models`` can show the
  same provenance breakdown ("disabled 3 days ago by web") as
  the existing per-model and per-gateway pages.

Idempotent
==========

Both upgrade and downgrade are guarded with ``IF (NOT) EXISTS``
so a partially-applied migration in a dev sandbox doesn't wedge
on a re-run.

Revision ID: 0019_disabled_model_per_gateway
Revises: 0018_image_data_uris
"""

from alembic import op

revision = "0019_disabled_model_per_gateway"
down_revision = "0018_image_data_uris"
branch_labels = None
depends_on = None

UPGRADE_SQL = """\
CREATE TABLE IF NOT EXISTS disabled_model_per_gateway (
    model_id     VARCHAR(255) NOT NULL,
    gateway_key  VARCHAR(64)  NOT NULL,
    disabled_by  VARCHAR(64)  NOT NULL DEFAULT 'web',
    disabled_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (model_id, gateway_key)
);

CREATE INDEX IF NOT EXISTS disabled_model_per_gateway_gateway_idx
    ON disabled_model_per_gateway (gateway_key);
"""

DOWNGRADE_SQL = """\
DROP INDEX IF EXISTS disabled_model_per_gateway_gateway_idx;
DROP TABLE IF EXISTS disabled_model_per_gateway;
"""


def upgrade() -> None:
    op.execute(UPGRADE_SQL)


def downgrade() -> None:
    op.execute(DOWNGRADE_SQL)
