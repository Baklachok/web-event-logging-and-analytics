import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from worker.worker import process_message, main


@pytest.mark.asyncio
async def test_process_message(monkeypatch):
    # Подменяем client.execute внутри worker.worker
    mock_execute = MagicMock()
    monkeypatch.setattr("worker.worker.client.execute", mock_execute)

    # Создаём фиктивное сообщение
    message = AsyncMock()
    message.body = json.dumps(
        {
            "user_id": 1,
            "event_type": "click",
            "page": "home",
            "timestamp": "2025-11-23T12:00:00+00:00",
        }
    ).encode()
    message.process = AsyncMock()  # контекстный менеджер

    # Подменяем контекстный менеджер message.process
    message.process.return_value.__aenter__.return_value = None
    message.process.return_value.__aexit__.return_value = None

    await process_message(message)

    # Проверяем, что execute вызван один раз с правильными аргументами
    assert mock_execute.call_count == 1
    args, kwargs = mock_execute.call_args
    assert len(args[1]) == 1  # вставлен один кортеж
    assert args[1][0][0] == 1  # user_id
    assert args[1][0][1] == "click"  # event_type


@pytest.mark.asyncio
async def test_worker_consumes_message():
    # Создаём мок-сообщение
    mock_message = AsyncMock()
    mock_message.body = b'{"user_id":1,"event_type":"click","page":"home","timestamp":"2025-11-23T12:00:00+00:00"}'

    # Асинхронный контекстный менеджер для message.process
    class DummyProcess:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

    mock_message.process = lambda *args, **kwargs: DummyProcess()

    # Асинхронный итератор для очереди
    class MockQueueIterator:
        def __init__(self, messages):
            self.messages = messages

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

        async def __aiter__(self):
            for message in self.messages:
                yield message

    # Мок очереди
    class MockQueue:
        def iterator(self):
            return MockQueueIterator([mock_message])

    # Мок канала
    class MockChannel:
        async def declare_queue(self, name, durable):
            return MockQueue()

    # Мок соединения
    class MockConnection:
        async def channel(self):
            return MockChannel()

    # Патчим aio_pika.connect_robust и process_message
    with (
        patch(
            "worker.worker.aio_pika.connect_robust",
            new=AsyncMock(return_value=MockConnection()),
        ),
        patch("worker.worker.process_message", new=AsyncMock()) as mock_process_message,
    ):
        await main()

        # Проверяем, что process_message вызван один раз с нашим mock_message
        mock_process_message.assert_called_once_with(mock_message)
