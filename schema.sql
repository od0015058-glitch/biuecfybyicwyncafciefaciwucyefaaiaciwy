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
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. TRANSACTIONS TABLE
-- This is your ledger. Every time someone clicks "Add Credit", a row is created here as PENDING. 
-- When NowPayments or ZarinPal sends the success webhook, it updates to SUCCESS.
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    telegram_id BIGINT REFERENCES users(telegram_id),
    gateway VARCHAR(50) NOT NULL, -- e.g., 'NowPayments', 'ZarinPal'
    currency_used VARCHAR(10) NOT NULL, -- e.g., 'TON', 'IRR'
    amount_crypto_or_rial DECIMAL(20, 8), -- The exact amount they sent in TON or Tomans
    amount_usd_credited DECIMAL(10, 4) NOT NULL, -- The USD equivalent added to their wallet
    status VARCHAR(20) DEFAULT 'PENDING', -- PENDING, SUCCESS, FAILED
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