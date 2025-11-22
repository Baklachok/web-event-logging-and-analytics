from typing import Optional, Callable, Awaitable

import aio_pika
from aio_pika.abc import (
    AbstractRobustConnection,
    AbstractChannel,
    AbstractExchange,
    AbstractQueue,
    AbstractIncomingMessage,
)
from aio_pika import Message, DeliveryMode, ExchangeType


class RabbitMQ:
    """
    Асинхронное подключение к RabbitMQ с поддержкой robust соединений.
    Позволяет создавать канал, обменник, очередь, публиковать и потреблять сообщения.
    """

    def __init__(
        self,
        url: str,
        exchange_name: str = "events_exchange",
        queue_name: str = "events_queue",
        routing_key: str = "events.key",
    ):
        self.url = url
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.routing_key = routing_key

        self.connection: Optional[AbstractRobustConnection] = None
        self.channel: Optional[AbstractChannel] = None
        self.exchange: Optional[AbstractExchange] = None
        self.queue: Optional[AbstractQueue] = None

    # ------------------ CONNECT ------------------

    async def connect(self) -> None:
        """
        Подключение к RabbitMQ.
        Создаёт robust-соединение, канал, обменник и очередь.
        """
        print("[RabbitMQ] Connecting...")

        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()

        # Настройка prefetch для потребителей
        await self.channel.set_qos(prefetch_count=50)

        # Создание обменника типа TOPIC
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name,
            ExchangeType.TOPIC,
            durable=True,
        )

        # Создание очереди
        self.queue = await self.channel.declare_queue(
            self.queue_name,
            durable=True,
        )

        # Привязка очереди к обменнику
        await self.queue.bind(self.exchange, routing_key=self.routing_key)

        print(
            f"[RabbitMQ] Connected → exchange={self.exchange_name}, queue={self.queue_name}"
        )

    # ------------------ PUBLISH ------------------

    async def publish(self, message: dict) -> None:
        """
        Публикация сообщения в RabbitMQ.
        """
        if not self.exchange:
            raise RuntimeError("RabbitMQ is not connected. Call connect() first.")

        msg = Message(
            body=str(message).encode("utf-8"),
            delivery_mode=DeliveryMode.PERSISTENT,
        )

        await self.exchange.publish(msg, routing_key=self.routing_key)

    # ------------------ CONSUME ------------------

    async def consume(
        self, callback: Callable[[AbstractIncomingMessage], Awaitable[None]]
    ) -> None:
        """
        Запуск консьюмера.
        callback: асинхронная функция для обработки сообщений
        """
        if not self.queue:
            raise RuntimeError("RabbitMQ is not connected. Call connect() first.")

        async with self.queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    await callback(message)

    # ------------------ CLOSE ------------------

    async def close(self) -> None:
        """
        Закрывает соединение с RabbitMQ.
        """
        if self.connection:
            await self.connection.close()
            print("[RabbitMQ] Connection closed.")
