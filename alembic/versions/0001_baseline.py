"""baseline — current consolidated schema.

Carries the exact SQL of the legacy ``schema.sql`` (as of merge 2e0fbd9
on main, which already includes the effects of legacy
``migrations/001_promo_codes.sql``, ``002_conversation_memory.sql``, and
``003_bump_free_messages_to_10.sql``).

Existing production deployments — whose DB already has these tables —
should run ``alembic stamp head`` once to mark the DB as up-to-date
without re-applying the SQL. New deployments just ``alembic upgrade
head``.

Revision ID: 0001_baseline
Revises:
Create Date: 2026-04-28
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001_baseline"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Single source of truth for the baseline schema. Future migrations
# never touch this string — they add CREATE TABLE / ALTER TABLE / etc.
# in their own revision files.
_BASELINE_SQL = """
-- 1. USERS TABLE
CREATE TABLE users (
    telegram_id BIGINT PRIMARY KEY,
    username VARCHAR(255),
    language_code VARCHAR(10) DEFAULT 'fa',
    balance_usd DECIMAL(10, 4) DEFAULT 0.0000,
    free_messages_left INT DEFAULT 10,
    active_model VARCHAR(255) DEFAULT 'openai/gpt-3.5-turbo',
    memory_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. TRANSACTIONS TABLE
-- See schema.sql for the status state machine + amount_usd_credited
-- semantics.
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    telegram_id BIGINT REFERENCES users(telegram_id),
    gateway VARCHAR(50) NOT NULL,
    currency_used VARCHAR(10) NOT NULL,
    amount_crypto_or_rial DECIMAL(20, 8),
    amount_usd_credited DECIMAL(10, 4) NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    gateway_invoice_id VARCHAR(255) UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE
);

-- 3. USAGE LOGS TABLE
CREATE TABLE usage_logs (
    log_id SERIAL PRIMARY KEY,
    telegram_id BIGINT REFERENCES users(telegram_id),
    model_used VARCHAR(255) NOT NULL,
    prompt_tokens INT NOT NULL,
    completion_tokens INT NOT NULL,
    cost_deducted_usd DECIMAL(10, 6) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. SYSTEM SETTINGS TABLE
CREATE TABLE system_settings (
    setting_key VARCHAR(50) PRIMARY KEY,
    setting_value VARCHAR(255) NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO system_settings (setting_key, setting_value)
    VALUES ('usd_to_toman_rate', '60000');

-- 5. PROMO CODES TABLE
CREATE TABLE promo_codes (
    code VARCHAR(64) PRIMARY KEY,
    discount_percent INT,
    discount_amount DECIMAL(10, 4),
    max_uses INT,
    used_count INT NOT NULL DEFAULT 0,
    expires_at TIMESTAMP WITH TIME ZONE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT promo_codes_discount_xor CHECK (
        (discount_percent IS NOT NULL AND discount_amount IS NULL)
        OR (discount_percent IS NULL AND discount_amount IS NOT NULL)
    )
);

-- 6. PROMO USAGE TABLE
CREATE TABLE promo_usage (
    promo_code VARCHAR(64) REFERENCES promo_codes(code),
    telegram_id BIGINT REFERENCES users(telegram_id),
    transaction_id INT REFERENCES transactions(transaction_id),
    bonus_usd DECIMAL(10, 4) NOT NULL,
    used_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (promo_code, telegram_id)
);

-- Promo fields on the transactions row.
ALTER TABLE transactions
    ADD COLUMN promo_code_used VARCHAR(64) REFERENCES promo_codes(code),
    ADD COLUMN promo_bonus_usd DECIMAL(10, 4) NOT NULL DEFAULT 0;

-- 7. CONVERSATION MESSAGES TABLE
CREATE TABLE conversation_messages (
    id BIGSERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    role VARCHAR(16) NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX conversation_messages_user_created_idx
    ON conversation_messages (telegram_id, created_at DESC);
"""


_BASELINE_DOWNGRADE_SQL = """
DROP INDEX IF EXISTS conversation_messages_user_created_idx;
DROP TABLE IF EXISTS conversation_messages;
DROP TABLE IF EXISTS promo_usage;
DROP TABLE IF EXISTS usage_logs;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS promo_codes;
DROP TABLE IF EXISTS system_settings;
DROP TABLE IF EXISTS users;
"""


def upgrade() -> None:
    op.execute(_BASELINE_SQL)


def downgrade() -> None:
    op.execute(_BASELINE_DOWNGRADE_SQL)
