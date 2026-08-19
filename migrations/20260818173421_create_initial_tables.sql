-- Add migration script here
CREATE TABLE guilds (
    id BIGINT NOT NULL PRIMARY KEY,
    inactivity_threshold_days INT NOT NULL
);

CREATE TABLE users (
    id BIGINT NOT NULL PRIMARY KEY,
    is_bot BOOLEAN NOT NULL
);

CREATE TABLE channels (
    id BIGINT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    guild_id BIGINT NOT NULL REFERENCES guilds(id)
);

CREATE TABLE messages (
    id BIGINT NOT NULL PRIMARY KEY,
    channel_id BIGINT NOT NULL REFERENCES channels(id),
    user_id BIGINT NOT NULL REFERENCES users(id),
    created_at TIMESTAMP NOT NULL,
    edited_at TIMESTAMP
);

CREATE TABLE reactions (
    message_id BIGINT NOT NULL REFERENCES messages(id),
    user_id BIGINT NOT NULL REFERENCES users(id),
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (message_id, user_id)
);