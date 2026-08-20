-- Add migration script here
ALTER TABLE messages RENAME COLUMN user_id TO user_id_old;
ALTER TABLE messages ADD COLUMN user_id BIGINT REFERENCES users(id);

-- Backfill
UPDATE messages SET user_id = user_id_old;