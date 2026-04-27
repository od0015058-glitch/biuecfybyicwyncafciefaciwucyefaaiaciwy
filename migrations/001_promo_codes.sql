-- Migration: promo codes (P2-5)
-- Apply to an existing aibot_db that already has the schema.sql tables.
-- Idempotent — safe to re-run.
--
--   psql -U botuser -d aibot_db -f migrations/001_promo_codes.sql

CREATE TABLE IF NOT EXISTS promo_codes (
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

CREATE TABLE IF NOT EXISTS promo_usage (
    promo_code VARCHAR(64) REFERENCES promo_codes(code),
    telegram_id BIGINT REFERENCES users(telegram_id),
    transaction_id INT REFERENCES transactions(transaction_id),
    bonus_usd DECIMAL(10, 4) NOT NULL,
    used_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (promo_code, telegram_id)
);

ALTER TABLE transactions
    ADD COLUMN IF NOT EXISTS promo_code_used VARCHAR(64) REFERENCES promo_codes(code),
    ADD COLUMN IF NOT EXISTS promo_bonus_usd DECIMAL(10, 4) NOT NULL DEFAULT 0;
