import asyncio
import json
import time
from typing import Optional, Callable, Awaitable

import aio_pika
from aio_pika import Message, DeliveryMode, ExchangeType
from aio_pika.abc import (
    AbstractRobustConnection,
    AbstractChannel,
    AbstractExchange,
    AbstractQueue,
    AbstractIncomingMessage,
)

from src.utils.logging import get_logger

logger = get_logger("rabbitmq")


class RabbitMQ:
    """
    Менеджер асинхронного подключения к RabbitMQ.
    Создаёт exchange/queue, публикует и читает сообщения.
    """

    PREFETCH_COUNT = 50
    MAX_RETRIES = 5
    BASE_BACKOFF = 2

    def __init__(
        self,
        url: str,
        metrics,
        exchange_name: str = "events_exchange",
        queue_name: str = "events_queue",
        routing_key: str = "events.key",
    ):
        self.url = url
        self.metrics = metrics
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.routing_key = routing_key

        self.connection: Optional[AbstractRobustConnection] = None
        self.channel: Optional[AbstractChannel] = None
        self.exchange: Optional[AbstractExchange] = None
        self.queue: Optional[AbstractQueue] = None

    # -----------------------------------------------------
    # PUBLIC CONNECT
    # -----------------------------------------------------
    async def connect(self) -> None:
        """Основной метод подключения к RabbitMQ с retry и setup."""
        self.connection, self.channel = await self._connect_with_retry()
        await self._setup_exchange_queue()

    # -----------------------------------------------------
    # PRIVATE: CONNECT WITH RETRY
    # -----------------------------------------------------
    async def _connect_with_retry(
        self,
    ) -> tuple[AbstractRobustConnection, AbstractChannel]:
        """Подключается к RabbitMQ с экспоненциальным backoff."""
        attempt = 0
        while attempt < self.MAX_RETRIES:
            try:
                attempt += 1
                logger.info(
                    f"Connecting to RabbitMQ at {self.url} (attempt {attempt})..."
                )
                connection = await aio_pika.connect_robust(self.url)
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=self.PREFETCH_COUNT)
                return connection, channel
            except Exception as e:
                backoff = min(self.BASE_BACKOFF * (2 ** (attempt - 1)), 30)
                logger.warning(f"Failed to connect: {e}. Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
        raise ConnectionError(
            f"Cannot connect to RabbitMQ after {self.MAX_RETRIES} attempts"
        )

    # -----------------------------------------------------
    # PRIVATE: SETUP EXCHANGE AND QUEUE
    # -----------------------------------------------------
    async def _setup_exchange_queue(self) -> None:
        """Создаёт exchange, очередь и биндинг."""
        if self.channel is None:
            raise RuntimeError("Channel is not initialized")
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name, ExchangeType.TOPIC, durable=True
        )
        self.queue = await self.channel.declare_queue(self.queue_name, durable=True)
        await self.queue.bind(self.exchange, routing_key=self.routing_key)
        logger.info(
            f"Setup complete → exchange='{self.exchange_name}', queue='{self.queue_name}', routing_key='{self.routing_key}'"
        )

    # -----------------------------------------------------
    # PUBLISH
    # -----------------------------------------------------
    async def publish(self, message: dict) -> None:
        """Публикует JSON-сообщение в очередь и обновляет метрики."""
        if not self.exchange:
            raise RuntimeError("RabbitMQ is not connected. Call connect() first.")

        start = time.monotonic()
        msg = Message(
            body=json.dumps(message).encode("utf-8"),
            delivery_mode=DeliveryMode.PERSISTENT,
        )
        await self.exchange.publish(msg, routing_key=self.routing_key)

        latency = time.monotonic() - start
        self.metrics.inc_sent()
        self.metrics.add_latency(latency)

        logger.debug(
            f"Published message to '{self.routing_key}' (latency={latency:.5f}s)"
        )

    # -----------------------------------------------------
    # CONSUME
    # -----------------------------------------------------
    async def consume(
        self, callback: Callable[[AbstractIncomingMessage], Awaitable[None]]
    ) -> None:
        """Запускает консьюмера очереди."""
        if not self.queue:
            raise RuntimeError("RabbitMQ is not connected. Call connect() first.")

        logger.info(f"Starting consumer on queue '{self.queue_name}'")
        async with self.queue.iterator() as iterator:
            async for message in iterator:
                logger.debug("Received message from queue → processing")
                async with message.process():
                    await callback(message)

    # -----------------------------------------------------
    # CLOSE
    # -----------------------------------------------------
    async def close(self) -> None:
        """Закрывает соединение с RabbitMQ."""
        if self.connection:
            await self.connection.close()
            logger.info("RabbitMQ connection closed.")
