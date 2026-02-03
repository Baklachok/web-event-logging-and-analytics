import asyncio
import os
from datetime import datetime
import json
import logging

import aio_pika
from clickhouse_driver import Client  # type: ignore

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_NATIVE_PORT = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000"))
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")

# Настройка логгера
logger = logging.getLogger("worker")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(name)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_NATIVE_PORT)


async def process_message(message: aio_pika.IncomingMessage):
    try:
        body = json.loads(message.body.decode())

        user_id = body["user_id"]
        event_type = body["event_type"]
        page = body["page"]
        timestamp = datetime.fromisoformat(body["timestamp"])

        client.execute(
            """
            INSERT INTO events (user_id, event_type, page, timestamp)
            VALUES
            """,
            [(user_id, event_type, page, timestamp)],
        )
        logger.info("Inserted event into ClickHouse: %s", body)
    except Exception as e:
        logger.error("Failed to process message: %s; error: %s", message.body, e)
        raise


async def connect_with_retry(url, retries=10, base_delay=2):
    attempt = 0
    while True:
        try:
            logger.info("Connecting to RabbitMQ at %s...", url)
            connection = await aio_pika.connect_robust(url)
            logger.info("Connected to RabbitMQ")
            return connection
        except Exception as e:
            attempt += 1
            delay = base_delay * (2 ** (attempt - 1))
            delay = min(delay, 30)  # ограничение максимального backoff
            logger.warning(
                "RabbitMQ not ready (attempt %d/%d: %s), retrying in %ds...",
                attempt,
                retries,
                e,
                delay,
            )
            await asyncio.sleep(delay)
            if retries and attempt >= retries:
                logger.error("Exceeded maximum retry attempts (%d).", retries)
                raise


async def main():
    connection = await connect_with_retry(RABBIT_URL)
    channel = await connection.channel()

    queue = await channel.declare_queue(
        "events_queue",
        durable=True,
    )
    logger.info("Subscribed to queue: %s", queue)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                async with message.process(requeue=True):
                    await process_message(message)
            except Exception as e:
                logger.error("Error processing message: %s", e)


if __name__ == "__main__":
    asyncio.run(main())
