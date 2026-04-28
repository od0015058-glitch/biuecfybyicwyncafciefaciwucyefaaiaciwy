"""bot_strings — admin-editable overrides for the compiled string table.

Source-of-truth for every user-facing label is still ``strings.py``
(the compiled ``_STRINGS`` dict). This table layers per-(lang, key)
overrides on top: when a row exists for a (lang, key) pair, the
runtime ``t()`` helper serves the override; otherwise it falls back
to the compiled default. Reverting an override = deleting the row.

Schema:

* ``bot_strings`` — one row per overridden (lang, key):
    - ``lang TEXT NOT NULL``        ('fa' | 'en' — anchored to
                                     ``strings.SUPPORTED_LANGUAGES``)
    - ``key TEXT NOT NULL``         (slug from ``strings._STRINGS``)
    - ``value TEXT NOT NULL``       (the user-visible text;
                                     ``str.format`` placeholders preserved)
    - ``updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()``
    - ``updated_by TEXT NULL``      (admin actor — telegram id as
                                     str, or ``"web"`` for web admin
                                     edits; nullable for the bot's
                                     own writes during seeding)
    - ``PRIMARY KEY (lang, key)``

The lang/key are NOT FK'd to anything: the compiled string table
lives in code and changes shape across deploys, and we'd rather
keep stale overrides queryable (so the admin can revert them
manually) than CASCADE-delete them on schema mismatches.

Revision ID: 0004_bot_strings
Revises: 0003_gift_codes
Create Date: 2026-04-28
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0004_bot_strings"
down_revision: str | Sequence[str] | None = "0003_gift_codes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_strings (
            lang        TEXT NOT NULL,
            key         TEXT NOT NULL,
            value       TEXT NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_by  TEXT NULL,
            PRIMARY KEY (lang, key)
        )
        """
    )
    # The runtime path looks up overrides by (lang, key) on every
    # ``t()`` call so the PRIMARY KEY index already covers the read
    # path. The admin "list overrides newest first" page sorts by
    # updated_at DESC, so a separate index there pays for itself.
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bot_strings_updated
            ON bot_strings (updated_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS bot_strings")
