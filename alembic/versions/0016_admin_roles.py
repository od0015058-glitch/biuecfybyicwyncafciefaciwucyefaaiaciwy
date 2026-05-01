"""Stage-15-Step-E #5 — first slice of the admin role system.

The bot's admin surface today is a flat env-list (``ADMIN_USER_IDS``):
every admin can do everything (broadcast, credit/debit wallets, mint
gift codes). The roadmap (§5 Stage-15-Step-E row 5) calls for three
graduated roles stored in the DB instead:

* ``viewer``   — read-only dashboard / metrics
* ``operator`` — broadcasts, promo codes, gift codes
* ``super``    — full access including wallet credit/debit + refunds

This migration adds the table; the Python side wires up the role
hierarchy + grant/revoke commands. We deliberately do NOT migrate the
existing env-list admins into ``admin_roles`` — that would be a
destructive action that surprises the operator. Instead, the lookup
helper (``admin_roles.effective_role``) treats env-listed admins as
``super`` for backward-compat, and the operator can later opt-in to
the DB-tracked model by inserting rows + (in a follow-up PR) trimming
the env list.

Schema:

* ``admin_roles``
    - ``telegram_id BIGINT PRIMARY KEY`` — Telegram user id.
    - ``role TEXT NOT NULL CHECK (role IN ('viewer','operator','super'))``
      — current role. The CHECK is defensive: a typo (``"opperator"``)
      from a manual SQL fix or a future buggy caller would otherwise
      land silently and degrade every gate to "unknown role → no access".
    - ``granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`` — last-update
      timestamp; refreshed on UPSERT so the audit trail shows when the
      role last changed.
    - ``granted_by BIGINT NULL`` — Telegram id of the granting admin,
      NULL for env-list bootstrap / SQL-applied seed rows. We don't FK
      this to ``users`` because admins typically have not yet talked
      to the bot (they DM ``/admin`` directly), so a stricter
      relationship would refuse legitimate grants.
    - ``notes TEXT NULL`` — optional free-form reason for the audit
      trail (parallels ``transactions.notes``).

Index on ``role`` for the "list all operators" filter; the table is
small enough (handful of rows) that a sequential scan would be fine,
but the partial index is cheap insurance against a future regression
adding hundreds of viewers.

Revision ID: 0016_admin_roles
Revises: 0015_disabled_models_gateways
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0016_admin_roles"
down_revision = "0015_disabled_models_gateways"
branch_labels = None
depends_on = None


UPGRADE_SQL = """\
CREATE TABLE IF NOT EXISTS admin_roles (
    telegram_id BIGINT PRIMARY KEY,
    role        TEXT NOT NULL
                CHECK (role IN ('viewer', 'operator', 'super')),
    granted_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    granted_by  BIGINT NULL,
    notes       TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_admin_roles_role
    ON admin_roles (role);
"""

DOWNGRADE_SQL = """\
DROP INDEX IF EXISTS idx_admin_roles_role;
DROP TABLE IF EXISTS admin_roles;
"""


def upgrade() -> None:
    op.execute(UPGRADE_SQL)


def downgrade() -> None:
    op.execute(DOWNGRADE_SQL)
