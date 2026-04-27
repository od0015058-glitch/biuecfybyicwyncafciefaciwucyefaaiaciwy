-- P3-7: bump the free trial allotment from 5 to 10 messages.
-- New users get 10 via the schema default; existing users with their
-- pristine 5-message budget (i.e. they haven't used a single message
-- yet) get topped up to 10 to match. Users who've already started
-- using their trial keep whatever they have left — we don't reset
-- progress, and we don't second-guess users who burned through their
-- free messages and may now be on paid balance.
ALTER TABLE users
    ALTER COLUMN free_messages_left SET DEFAULT 10;

-- One-shot top-up for users whose count is still equal to the old
-- default. This is idempotent: re-running it after the bump finds
-- nobody at exactly 5 (because they either got bumped or used some).
UPDATE users
SET free_messages_left = 10
WHERE free_messages_left = 5;
