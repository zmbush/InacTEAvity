-- Add migration script here
ALTER TABLE channels RENAME COLUMN name TO name_old;
ALTER TABLE channels ADD COLUMN name TEXT;