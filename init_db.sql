CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS default.events (
    user_id UInt64,
    event_type String,
    page String,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY timestamp;

-- Kafka engine table: ClickHouse сам подписывается на топик
CREATE TABLE IF NOT EXISTS default.events_kafka (
    user_id    UInt64,
    event_type String,
    page       String,
    timestamp  String          -- сырой ISO-8601, парсим в MV
) ENGINE = Kafka
SETTINGS
    kafka_broker_list       = 'redpanda:9092',
    kafka_topic_list        = 'events',
    kafka_group_name        = 'clickhouse-events-consumer',
    kafka_format            = 'JSONEachRow',
    kafka_num_consumers     = 1,
    kafka_skip_broken_messages = 100;

-- Materialized view: переносит данные из Kafka-таблицы в MergeTree, парсит timestamp
CREATE MATERIALIZED VIEW IF NOT EXISTS default.events_mv TO default.events AS
SELECT
    user_id,
    event_type,
    page,
    parseDateTimeBestEffort(timestamp) AS timestamp
FROM default.events_kafka;
