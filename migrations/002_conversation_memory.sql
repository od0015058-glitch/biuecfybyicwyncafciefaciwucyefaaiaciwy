-- Migration: per-user conversation memory toggle (P3-5)
-- Apply to an existing aibot_db that already has the schema.sql tables.
-- Idempotent — safe to re-run.
--
--   psql -U botuser -d aibot_db -f migrations/002_conversation_memory.sql

-- 1. Per-user opt-in flag for conversation memory. OFF by default so
--    the user explicitly chooses to pay for the larger token bills
--    that come with multi-turn context.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS memory_enabled BOOLEAN NOT NULL DEFAULT FALSE;

-- 2. Conversation messages. One row per user prompt or assistant reply.
--    The bot appends here only when ``users.memory_enabled = TRUE``;
--    when disabled, requests are stateless and nothing lands here.
--
--    "🆕 New chat" deletes every row for the user — see
--    database.clear_conversation. The ON DELETE CASCADE makes the user
--    deletion safe too if we ever add a delete-account flow.
CREATE TABLE IF NOT EXISTS conversation_messages (
    id BIGSERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    role VARCHAR(16) NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for the only read pattern we care about: "give me the last N
-- messages for this user, newest first" (we then reverse client-side
-- to feed them to the model in chronological order).
CREATE INDEX IF NOT EXISTS conversation_messages_user_created_idx
    ON conversation_messages (telegram_id, created_at DESC);
