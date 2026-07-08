import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from src.rabbit.rabbit import RabbitMQ


class DummyMetrics:
    def __init__(self):
        self.sent = 0
        self.latencies = []

    def inc_sent(self):
        self.sent += 1

    def add_latency(self, latency):
        self.latencies.append(latency)


@pytest.mark.asyncio
async def test_connect_and_setup():
    metrics = DummyMetrics()
    rabbit = RabbitMQ(url="amqp://guest:guest@localhost/", metrics=metrics)

    with patch("aio_pika.connect_robust", new_callable=AsyncMock) as mock_connect:
        mock_channel = AsyncMock()
        mock_connect.return_value.channel = AsyncMock(return_value=mock_channel)
        mock_channel.set_qos = AsyncMock()
        mock_channel.declare_exchange = AsyncMock()
        mock_channel.declare_queue = (
            AsyncMock()
        )  # оставляем мок, чтобы поймать лишний вызов

        await rabbit.connect()

        assert rabbit.connection is not None
        assert rabbit.channel is not None
        assert rabbit.exchange is not None

        # НОВЫЙ контракт: продюсер объявляет только exchange, очередь принадлежит worker'у
        assert rabbit.queue is None  # ← перевёрнутый инвариант
        mock_channel.declare_exchange.assert_awaited_once()  # exchange — да
        mock_channel.declare_queue.assert_not_awaited()  # queue — нет (защита от гонки 406)


@pytest.mark.asyncio
async def test_publish():
    metrics = DummyMetrics()
    rabbit = RabbitMQ(url="amqp://guest:guest@localhost/", metrics=metrics)

    # Мокаем exchange.publish
    rabbit.exchange = AsyncMock()
    rabbit.routing_key = "test.key"

    message = {"user_id": 1, "event": "click"}
    await rabbit.publish(message)

    rabbit.exchange.publish.assert_awaited_once()
    assert metrics.sent == 1
    assert len(metrics.latencies) == 1


@pytest.mark.asyncio
async def test_consume_calls_callback():
    metrics = DummyMetrics()
    rabbit = RabbitMQ(url="amqp://guest:guest@localhost/", metrics=metrics)

    # Мокаем сообщение
    mock_message = MagicMock()

    # message.process как async context manager
    class AsyncCM:
        async def __aenter__(self):
            return mock_message

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

    mock_message.process = MagicMock(return_value=AsyncCM())

    # Мокаем итератор очереди как async context manager
    class AsyncIteratorCM:
        def __init__(self, items):
            self.items = items

        async def __aenter__(self):
            self._iter = iter(self.items)
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self._iter)
            except StopIteration:
                raise StopAsyncIteration

    mock_queue = MagicMock()
    mock_queue.iterator.return_value = AsyncIteratorCM([mock_message])
    rabbit.queue = mock_queue

    callback_called = False

    async def callback(msg):
        nonlocal callback_called
        callback_called = True

    await asyncio.wait_for(rabbit.consume(callback), timeout=0.1)
    assert callback_called
