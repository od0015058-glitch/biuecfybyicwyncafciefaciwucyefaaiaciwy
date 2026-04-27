-- 1. USERS TABLE
-- This tracks the user's identity, their language preference (for Farsi/English), 
-- their exact financial balance, and their free message funnel.
CREATE TABLE users (
    telegram_id BIGINT PRIMARY KEY,
    username VARCHAR(255),
    language_code VARCHAR(10) DEFAULT 'fa', -- Defaults to Farsi
    balance_usd DECIMAL(10, 4) DEFAULT 0.0000, -- Precise to 4 decimal places for micro-cent API costs
    free_messages_left INT DEFAULT 5, -- The Freemium Funnel
    active_model VARCHAR(255) DEFAULT 'openai/gpt-3.5-turbo',
    -- P3-5 conversation memory opt-in. OFF by default; enabling
    -- causes ai_engine to prepend the user's recent conversation_messages
    -- as context to every prompt — costs scale with conversation length.
    memory_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. TRANSACTIONS TABLE
-- This is your ledger. Every time someone clicks "Add Credit", a row is created
-- here as PENDING. The IPN webhook from NowPayments transitions it through
-- the lifecycle below.
--
-- Status state machine (see database.finalize_payment / finalize_partial_payment
-- / mark_transaction_terminal for the corresponding code paths):
--
--   PENDING  ──finished──→ SUCCESS
--   PENDING  ──partially_paid──→ PARTIAL
--   PENDING  ──expired/failed/refunded──→ EXPIRED / FAILED / REFUNDED
--   PARTIAL  ──finished──→ SUCCESS                  (credits remainder delta)
--   PARTIAL  ──partially_paid──→ PARTIAL            (credits new delta only)
--   PARTIAL  ──expired/failed/refunded──→ EXPIRED / FAILED / REFUNDED
--                                                   (no debit; user keeps the partial credit)
--
-- amount_usd_credited semantics:
--   * For PENDING rows:     the USD amount we *intend* to credit on success.
--   * For non-PENDING rows: the cumulative USD already credited to the wallet
--                            from this invoice. Updated atomically alongside
--                            the wallet credit.
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    telegram_id BIGINT REFERENCES users(telegram_id),
    gateway VARCHAR(50) NOT NULL, -- e.g., 'NowPayments'
    currency_used VARCHAR(10) NOT NULL, -- e.g., 'TON', 'btc', 'usdttrc20'
    amount_crypto_or_rial DECIMAL(20, 8), -- The exact amount they sent in the chosen currency
    amount_usd_credited DECIMAL(10, 4) NOT NULL, -- See semantics note above
    status VARCHAR(20) DEFAULT 'PENDING', -- PENDING, SUCCESS, PARTIAL, EXPIRED, FAILED, REFUNDED
    gateway_invoice_id VARCHAR(255) UNIQUE, -- The ID provided by the payment gateway to prevent duplicate processing
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE
);

-- 3. USAGE LOGS TABLE
-- Every single prompt sent to OpenRouter is logged here. If a user asks where their money went, 
-- you query this table and show them their exact token usage.
CREATE TABLE usage_logs (
    log_id SERIAL PRIMARY KEY,
    telegram_id BIGINT REFERENCES users(telegram_id),
    model_used VARCHAR(255) NOT NULL,
    prompt_tokens INT NOT NULL,
    completion_tokens INT NOT NULL,
    cost_deducted_usd DECIMAL(10, 6) NOT NULL, -- The micro-cent cost of that specific message
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. SYSTEM SETTINGS TABLE
-- This allows you to change global variables without restarting the Python bot.
CREATE TABLE system_settings (
    setting_key VARCHAR(50) PRIMARY KEY,
    setting_value VARCHAR(255) NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert default exchange rate for Rial to USD conversions
INSERT INTO system_settings (setting_key, setting_value) VALUES ('usd_to_toman_rate', '60000');

-- 5. PROMO CODES TABLE
-- Discount codes the user can apply during the wallet top-up flow. Each
-- successful redemption credits a bonus on top of the paid invoice
-- amount (does NOT reduce the amount the user pays NowPayments).
--
-- Discount semantics: exactly one of discount_percent / discount_amount
-- is non-NULL. Bonus on a $20 top-up:
--   * 10 % code -> $2.00 bonus  (discount_percent = 10)
--   * $5 code   -> $5.00 bonus  (discount_amount  = 5)
-- Redemption is gated on (is_active, NOT expired, used_count < max_uses,
-- and the user hasn't already used this code) — see
-- database.validate_promo_code / redeem_promo_code.
CREATE TABLE promo_codes (
    code VARCHAR(64) PRIMARY KEY,
    discount_percent INT, -- 1-100, or NULL if discount_amount is set
    discount_amount DECIMAL(10, 4), -- USD, or NULL if discount_percent is set
    max_uses INT, -- NULL means unlimited
    used_count INT NOT NULL DEFAULT 0,
    expires_at TIMESTAMP WITH TIME ZONE, -- NULL means no expiry
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- Enforce mutual exclusivity at the DB layer.
    CONSTRAINT promo_codes_discount_xor CHECK (
        (discount_percent IS NOT NULL AND discount_amount IS NULL)
        OR (discount_percent IS NULL AND discount_amount IS NOT NULL)
    )
);

-- 6. PROMO USAGE TABLE
-- One row per successful redemption. Composite primary key prevents the
-- same user from redeeming the same code twice. Inserted atomically
-- alongside the bonus credit in finalize_payment.
CREATE TABLE promo_usage (
    promo_code VARCHAR(64) REFERENCES promo_codes(code),
    telegram_id BIGINT REFERENCES users(telegram_id),
    transaction_id INT REFERENCES transactions(transaction_id),
    bonus_usd DECIMAL(10, 4) NOT NULL,
    used_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (promo_code, telegram_id)
);

-- Promo fields on the transactions row. Set when the user enters a
-- promo before invoice creation; the bonus is applied to the wallet
-- credit on the SUCCESS transition (only — partials don't unlock the
-- bonus, the user has to pay the full invoice).
ALTER TABLE transactions
    ADD COLUMN promo_code_used VARCHAR(64) REFERENCES promo_codes(code),
    ADD COLUMN promo_bonus_usd DECIMAL(10, 4) NOT NULL DEFAULT 0;

-- 7. CONVERSATION MESSAGES TABLE
-- Per-user multi-turn conversation memory (P3-5). Only written to when
-- ``users.memory_enabled = TRUE``. ai_engine reads the most recent N
-- rows (chronological) and feeds them as the OpenAI Chat-Completions
-- ``messages`` array on each request.
--
-- "🆕 New chat" deletes every row for the user so they can reset
-- context without flipping the toggle. ON DELETE CASCADE handles a
-- future delete-account flow safely.
CREATE TABLE conversation_messages (
    id BIGSERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    role VARCHAR(16) NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX conversation_messages_user_created_idx
    ON conversation_messages (telegram_id, created_at DESC);