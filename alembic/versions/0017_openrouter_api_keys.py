"""Stage-15-Step-E #4 follow-up #2 — DB-backed OpenRouter API key registry.

Pre-migration: the OpenRouter multi-key load-balancer (`openrouter_keys.py`)
read its key pool exclusively from the env vars ``OPENROUTER_API_KEY`` /
``OPENROUTER_API_KEY_1`` … ``OPENROUTER_API_KEY_10``. Adding, rotating,
or disabling a key meant editing the operator's ``.env`` file and
restarting the bot — a friction point that scales badly once the
operator runs >2 keys (every rotation requires a deploy window).

This migration adds a small registry table the admin panel uses to
add / disable / delete keys at runtime, alongside the env-loaded
keys. The loader (``openrouter_keys.refresh_from_db``) merges the
two sources at runtime so existing env-driven deploys keep working
without any schema-touching migration step.

We keep the schema deliberately minimal:

* ``id BIGSERIAL PRIMARY KEY`` — surrogate key, used in audit rows
  and the panel's URL slugs ("disable key 12") rather than leaking
  the api_key string into URLs.
* ``label TEXT NOT NULL`` — operator-supplied human-readable name
  ("main", "backup-1", "ops-2025-q2"). Required and trimmed by the
  caller; the table accepts NOT NULL so a buggy caller passing an
  empty string lands a constraint error instead of silently storing
  a label-less row that's hard to identify in the panel later.
* ``api_key TEXT NOT NULL UNIQUE`` — the raw sk-or-… value.
  ``UNIQUE`` defends against an operator pasting the same key
  twice; on conflict the panel surfaces a flash error rather than
  silently double-counting the key in the load balancer.
* ``enabled BOOLEAN NOT NULL DEFAULT TRUE`` — soft-disable knob.
  Disabled keys stay in the table (so the audit trail / per-key
  counters survive) but the loader skips them.
* ``created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`` — when the
  operator added the key. Bookkeeping for "this key has been in
  rotation 60 days, time to rotate".
* ``last_used_at TIMESTAMPTZ NULL`` — bumped by the loader on the
  first ``key_for_user`` pick after a refresh; gives the panel a
  freshness signal without needing per-call writes.
* ``notes TEXT NULL`` — optional free-form metadata (e.g. the
  associated OpenRouter account email, "do not use after $date").

Index on ``enabled`` is a partial covering the hot read path
(loader pulls only enabled rows). The table is bounded by the
operator's OpenRouter account ceiling (typically <20 keys), so a
sequential scan would be fine — the index is defence in depth.

Revision ID: 0017_openrouter_api_keys
Revises: 0016_admin_roles
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0017_openrouter_api_keys"
down_revision = "0016_admin_roles"
branch_labels = None
depends_on = None


UPGRADE_SQL = """\
CREATE TABLE IF NOT EXISTS openrouter_api_keys (
    id            BIGSERIAL PRIMARY KEY,
    label         TEXT NOT NULL,
    api_key       TEXT NOT NULL UNIQUE,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_used_at  TIMESTAMP WITH TIME ZONE NULL,
    notes         TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_openrouter_api_keys_enabled
    ON openrouter_api_keys (enabled)
    WHERE enabled = TRUE;
"""

DOWNGRADE_SQL = """\
DROP INDEX IF EXISTS idx_openrouter_api_keys_enabled;
DROP TABLE IF EXISTS openrouter_api_keys;
"""


def upgrade() -> None:
    op.execute(UPGRADE_SQL)


def downgrade() -> None:
    op.execute(DOWNGRADE_SQL)
