-- Add migration script here
ALTER TABLE guilds ADD COLUMN generate_report_at_hour INT NOT NULL DEFAULT 12;
ALTER TABLE guilds ADD COLUMN warning_threshold_days INT NOT NULL DEFAULT 7;