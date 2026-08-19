-- Add migration script here
ALTER TABLE guilds ADD COLUMN search_window_buffer_days INT NOT NULL DEFAULT 5;
ALTER TABLE guilds ADD COLUMN report_channel BIGINT REFERENCES channels(id);