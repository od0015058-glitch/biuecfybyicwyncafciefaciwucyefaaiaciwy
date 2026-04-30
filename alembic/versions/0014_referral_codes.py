"""referral codes \u2014 user-to-user invites that credit both wallets.

Stage-13-Step-C. The user explicitly asked for *"referral codes \u2014
user-to-user invite codes that credit both wallets on the invitee's
first paid top-up"* (HANDOFF \u00a75).

Two tables:

* ``referral_codes`` \u2014 one row per user, looked up at invite time
  to render the share link. The ``code`` is short (8 chars), ASCII
  alphanumeric, generated when the owner first opens the invite
  screen (NOT eagerly at signup, so users who never use the feature
  don't pollute the table). One code per user; PK on
  ``owner_telegram_id`` enforces that. The ``code`` column itself
  is also UNIQUE so a typo'd code at ``/start ref_<code>`` lookup
  is unambiguous.

* ``referral_grants`` \u2014 one row per (referrer, invitee) pair.
  Created at PENDING state when the invitee taps a referral
  ``/start ref_<code>`` payload; flipped to PAID inside the
  ``finalize_payment`` / ``finalize_partial_payment`` SUCCESS /
  PARTIAL transition that crosses the invitee's *first* USD credit.
  After the flip, both ``referrer.balance_usd`` and
  ``invitee.balance_usd`` get the configured bonus. ``invitee_telegram_id``
  is UNIQUE so an invitee can't be claimed by two different referrers
  (first one wins). Self-referral is rejected at write time:
  the CHECK constraint blocks rows where invitee = referrer.

  ``status`` is one of ``PENDING`` / ``PAID`` / ``REJECTED`` (the
  last is reserved for an admin-side undo path \u2014 not in the v1
  surface but reserved so future migrations don't have to widen the
  CHECK).

  ``triggering_transaction_id`` links the grant to the exact
  ``transactions`` row that crossed the threshold, so the audit trail
  shows *which* top-up unlocked the bonus. Nullable + ``ON DELETE
  SET NULL`` so a manual transactions cleanup doesn't cascade the
  grant row away.

Indexes:

* ``referral_grants`` PRIMARY KEY is ``id`` (SERIAL) for cheap audit
  pagination by recency.
* ``idx_referral_grants_referrer_status`` on ``(referrer_telegram_id,
  status)`` \u2014 the "show me my pending invites" lookup the future
  ``/wallet \u2192 invite stats`` screen would use.
* ``idx_referral_grants_pending_by_invitee`` on
  ``(invitee_telegram_id) WHERE status = 'PENDING'`` \u2014 the
  hot-path lookup at first-credit time. Partial index keeps it tiny;
  PAID grants don't need to be in the index.

Revision ID: 0014_referral_codes
Revises: 0013_gift_redemptions_code_index
Create Date: 2026-04-30
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0014_referral_codes"
down_revision: str | Sequence[str] | None = "0013_gift_redemptions_code_index"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS referral_codes (
            owner_telegram_id BIGINT PRIMARY KEY
                REFERENCES users(telegram_id) ON DELETE CASCADE,
            code              TEXT NOT NULL UNIQUE
                CHECK (length(code) BETWEEN 4 AND 32),
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS referral_grants (
            id                        SERIAL PRIMARY KEY,
            referrer_telegram_id      BIGINT NOT NULL
                REFERENCES users(telegram_id) ON DELETE CASCADE,
            invitee_telegram_id       BIGINT NOT NULL UNIQUE
                REFERENCES users(telegram_id) ON DELETE CASCADE,
            code                      TEXT NOT NULL,
            status                    TEXT NOT NULL
                CHECK (status IN ('PENDING', 'PAID', 'REJECTED')),
            pending_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            paid_at                   TIMESTAMPTZ NULL,
            bonus_usd_referrer        DECIMAL(10, 4) NULL
                CHECK (bonus_usd_referrer IS NULL OR bonus_usd_referrer >= 0),
            bonus_usd_invitee         DECIMAL(10, 4) NULL
                CHECK (bonus_usd_invitee IS NULL OR bonus_usd_invitee >= 0),
            triggering_transaction_id INTEGER NULL
                REFERENCES transactions(transaction_id) ON DELETE SET NULL,
            triggering_amount_usd     DECIMAL(10, 4) NULL
                CHECK (triggering_amount_usd IS NULL OR triggering_amount_usd >= 0),
            CHECK (referrer_telegram_id <> invitee_telegram_id)
        )
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_referral_grants_referrer_status
            ON referral_grants (referrer_telegram_id, status)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_referral_grants_pending_by_invitee
            ON referral_grants (invitee_telegram_id)
            WHERE status = 'PENDING'
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS referral_grants")
    op.execute("DROP TABLE IF EXISTS referral_codes")
