-- Add migration script here
ALTER TABLE channels DROP COLUMN name_old;
ALTER TABLE messages DROP COLUMN user_id_old;