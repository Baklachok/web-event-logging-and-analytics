import os
from typing import Final

HOST: Final[str] = os.getenv("HOST", "0.0.0.0")
PORT: Final[int] = int(os.getenv("PORT", "8080"))

# RabbitMQ
RABBIT_URL: Final[str] = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")

# ClickHouse
CLICKHOUSE_HOST: Final[str] = os.getenv("CLICKHOUSE_HOST", "clickhouse")
# HTTP port for clickhouse_connect (API)
CLICKHOUSE_HTTP_PORT: Final[int] = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))

KAFKA_BOOTSTRAP_SERVERS: Final[str] = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"
)
KAFKA_EVENTS_TOPIC: Final[str] = os.getenv("KAFKA_EVENTS_TOPIC", "events")
KAFKA_ACKS: Final[str] = os.getenv("KAFKA_ACKS", "all")
KAFKA_IDEMPOTENCE: Final[bool] = (
    os.getenv("KAFKA_IDEMPOTENCE", "true").lower() == "true"
)
