import asyncio
import os
import json
import logging

import aio_pika
from clickhouse_driver import Client  # type: ignore

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_NATIVE_PORT = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000"))
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")

COMMANDS_EXCHANGE = "commands_exchange"
COMMANDS_QUEUE = "commands"
PURGE_ROUTING_KEY = "commands.purge"
DLQ_EXCHANGE = "commands_dlx"
DLQ_QUEUE = "commands.dlq"

logger = logging.getLogger("worker")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("[%(asctime)s] [%(levelname)s] %(name)s: %(message)s")
)
logger.addHandler(handler)

client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_NATIVE_PORT)


async def process_command(message: aio_pika.IncomingMessage) -> None:
    """Обрабатывает purge-команду. Исключение → nack → DLQ."""
    body = json.loads(message.body.decode())
    user_id = body["user_id"]  # KeyError → nack → DLQ, что корректно для мусора

    logger.info("Purge-команда для user_id=%s", user_id)
    client.execute(
        "ALTER TABLE events DELETE WHERE user_id = %(uid)s",
        {"uid": user_id},
        settings={
            "mutations_sync": 2
        },  # ждём реального удаления, не постановки в очередь
    )
    logger.info("Данные user_id=%s удалены из ClickHouse", user_id)


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
            delay = min(base_delay * (2 ** (attempt - 1)), 30)
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
    await channel.set_qos(prefetch_count=10)

    # --- DLQ-инфраструктура: dead-letter exchange + очередь для «мёртвых» команд ---
    dlx = await channel.declare_exchange(
        DLQ_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True
    )
    dlq = await channel.declare_queue(DLQ_QUEUE, durable=True)
    await dlq.bind(dlx)

    # --- основной topic-exchange для команд ---
    commands_exchange = await channel.declare_exchange(
        COMMANDS_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )

    # --- рабочая очередь с привязкой к DLX: nack(requeue=False) → сообщение уходит в DLQ ---
    queue = await channel.declare_queue(
        COMMANDS_QUEUE,
        durable=True,
        arguments={"x-dead-letter-exchange": DLQ_EXCHANGE},
    )
    await queue.bind(commands_exchange, routing_key=PURGE_ROUTING_KEY)
    logger.info(
        "Подписан на очередь '%s' (rk=%s), DLQ='%s'",
        COMMANDS_QUEUE,
        PURGE_ROUTING_KEY,
        DLQ_QUEUE,
    )

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                # requeue=False: при исключении сообщение НЕ возвращается в очередь,
                # а по x-dead-letter-exchange уезжает в commands.dlq
                async with message.process(requeue=False):
                    await process_command(message)
            except Exception as e:
                logger.error("Команда ушла в DLQ: %s; error: %s", message.body, e)


if __name__ == "__main__":
    asyncio.run(main())
