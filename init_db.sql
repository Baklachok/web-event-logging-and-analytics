CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS default.events (
    user_id UInt64,
    event_type String,
    page String,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY timestamp;
