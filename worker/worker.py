import asyncio
from datetime import datetime
import json

import aio_pika
from clickhouse_driver import Client  # type: ignore

from worker_config import CLICKHOUSE_HOST, RABBIT_URL  # type: ignore

client = Client(host=CLICKHOUSE_HOST)


async def process_message(message: aio_pika.IncomingMessage):
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
    print("Inserted:", body)


async def connect_with_retry(url, retries=10, base_delay=2):
    attempt = 0
    while True:
        try:
            return await aio_pika.connect_robust(url)
        except Exception as e:
            attempt += 1
            delay = base_delay * (2 ** (attempt - 1))
            delay = min(delay, 30)  # ограничение максимального backoff
            print(f"RabbitMQ not ready ({e}), retrying in {delay}s...")
            await asyncio.sleep(delay)
            if retries and attempt >= retries:
                raise


async def main():
    connection = await connect_with_retry(RABBIT_URL)
    channel = await connection.channel()

    queue = await channel.declare_queue(
        "events_queue",
        durable=True,
    )

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                async with message.process(requeue=True):
                    await process_message(message)
            except Exception as e:
                print("Error:", e)


if __name__ == "__main__":
    asyncio.run(main())
