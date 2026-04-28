"""gift codes — admin-issued codes that directly credit user balance.

Distinct from ``promo_codes`` (which require a paid invoice to apply
their bonus). A gift code is "free money" the admin hands out:

    "10 people get $X each, no purchase required"

Schema:

* ``gift_codes`` — one row per code:
    - ``code TEXT PRIMARY KEY``  (uppercased on insert)
    - ``amount_usd NUMERIC(10,4) NOT NULL CHECK (amount_usd > 0)``
    - ``max_uses INTEGER NULL``  (NULL = unlimited)
    - ``used_count INTEGER NOT NULL DEFAULT 0``
    - ``expires_at TIMESTAMPTZ NULL``  (NULL = never expires)
    - ``is_active BOOLEAN NOT NULL DEFAULT TRUE``
    - ``created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()``

* ``gift_redemptions`` — one row per (code, telegram_id) pair, so
  each user can redeem each code at most once:
    - ``code TEXT NOT NULL REFERENCES gift_codes(code) ON DELETE CASCADE``
    - ``telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE``
    - ``redeemed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()``
    - ``transaction_id INTEGER NULL REFERENCES transactions(transaction_id) ON DELETE SET NULL``
    - ``PRIMARY KEY (code, telegram_id)``
  The ``transaction_id`` link is nullable + ON DELETE SET NULL so a
  manual ``transactions`` cleanup never breaks the redemption record.

Revision ID: 0003_gift_codes
Revises: 0002_transactions_notes
Create Date: 2026-04-28
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0003_gift_codes"
down_revision: str | Sequence[str] | None = "0002_transactions_notes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS gift_codes (
            code         TEXT PRIMARY KEY,
            amount_usd   DECIMAL(10, 4) NOT NULL CHECK (amount_usd > 0),
            max_uses     INTEGER NULL CHECK (max_uses IS NULL OR max_uses > 0),
            used_count   INTEGER NOT NULL DEFAULT 0,
            expires_at   TIMESTAMPTZ NULL,
            is_active    BOOLEAN NOT NULL DEFAULT TRUE,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gift_codes_active_expires
            ON gift_codes (is_active, expires_at)
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS gift_redemptions (
            code            TEXT NOT NULL
                REFERENCES gift_codes(code) ON DELETE CASCADE,
            telegram_id     BIGINT NOT NULL
                REFERENCES users(telegram_id) ON DELETE CASCADE,
            redeemed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            transaction_id  INTEGER NULL
                REFERENCES transactions(transaction_id) ON DELETE SET NULL,
            PRIMARY KEY (code, telegram_id)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gift_redemptions_user
            ON gift_redemptions (telegram_id, redeemed_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS gift_redemptions")
    op.execute("DROP TABLE IF EXISTS gift_codes")
