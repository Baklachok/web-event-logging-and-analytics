import json

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from worker.worker import main, process_command


@pytest.fixture
def execute(monkeypatch):
    """Замоканный client.execute — общий для изолированных тестов process_command."""
    mock = MagicMock()
    monkeypatch.setattr("worker.worker.client.execute", mock)
    return mock


def _message(body) -> MagicMock:
    """Сообщение с .body: dict сериализуется в JSON, bytes — как есть."""
    msg = MagicMock()
    msg.body = body if isinstance(body, (bytes, bytearray)) else json.dumps(body).encode()
    return msg


# =====================================================================
# process_command — изолированно (без RabbitMQ, без event loop-плумбинга)
# =====================================================================


@pytest.mark.asyncio
async def test_process_command_purges_user(execute):
    await process_command(_message({"user_id": 42}))

    assert execute.call_count == 1
    args, kwargs = execute.call_args
    # SQL — DELETE-мутация, а не INSERT
    assert "ALTER TABLE events DELETE" in args[0]
    # user_id уходит именованным параметром (защита от SQL-инъекции)
    assert args[1] == {"uid": 42}
    # синхронная мутация: ack == «реально удалено», не «поставлено в очередь»
    assert kwargs["settings"]["mutations_sync"] == 2


@pytest.mark.asyncio
async def test_process_command_rejects_missing_user_id(execute):
    """Нет user_id → KeyError до ClickHouse → (в main) nack → DLQ."""
    with pytest.raises(KeyError):
        await process_command(_message({"wrong_field": 1}))
    execute.assert_not_called()


@pytest.mark.asyncio
async def test_process_command_rejects_malformed_json(execute):
    """Битый JSON → JSONDecodeError до ClickHouse → DLQ."""
    with pytest.raises(json.JSONDecodeError):
        await process_command(_message(b"{not json"))
    execute.assert_not_called()


# =====================================================================
# Мок-плумбинг для main(): записывающий канал + очереди
# =====================================================================


class MockExchange:
    pass


class MockQueueIterator:
    def __init__(self, messages):
        self.messages = messages

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def __aiter__(self):
        for m in self.messages:
            yield m


class MockQueue:
    def __init__(self, name, messages):
        self.name = name
        self.messages = messages
        self.binds = []  # список (exchange, kwargs)

    def iterator(self):
        return MockQueueIterator(self.messages)

    async def bind(self, exchange, **kwargs):
        self.binds.append((exchange, kwargs))


class RecordingChannel:
    """Канал, который записывает аргументы declare/bind/qos — чтобы тест
    мог проверить DLX-проводку, prefetch и биндинги."""

    def __init__(self, messages):
        self._messages = messages
        self.qos = None
        self.declared_exchanges = {}  # name -> kwargs
        self.declared_queues = {}  # name -> kwargs
        self.queues = {}  # name -> MockQueue

    async def set_qos(self, **k):
        self.qos = k

    async def declare_exchange(self, name, *a, **k):
        self.declared_exchanges[name] = k
        return MockExchange()

    async def declare_queue(self, name, *a, **k):
        self.declared_queues[name] = k  # arguments={"x-dead-letter-exchange": ...}
        q = MockQueue(name, self._messages)
        self.queues[name] = q
        return q


class MockConnection:
    def __init__(self, channel):
        self._channel = channel

    async def channel(self):
        return self._channel


def _capturing_message(process_calls: dict) -> MagicMock:
    """Сообщение, чей .process(**kwargs) захватывает kwargs (проверяем requeue=False)."""

    class DummyProcess:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *a):
            return None  # исключение из тела пробрасывается → ловит try/except в main

    def _process(*a, **k):
        process_calls.update(k)
        return DummyProcess()

    msg = _message({"user_id": 42})
    msg.process = _process
    return msg


async def _run_main(process_command_mock: AsyncMock):
    """Прогоняет main() с одним сообщением через RecordingChannel.
    Возвращает (channel, process_calls, message)."""
    process_calls: dict = {}
    message = _capturing_message(process_calls)
    channel = RecordingChannel(messages=[message])
    with (
        patch(
            "worker.worker.aio_pika.connect_robust",
            new=AsyncMock(return_value=MockConnection(channel)),
        ),
        patch("worker.worker.process_command", new=process_command_mock),
    ):
        await main()
    return channel, process_calls, message


# =====================================================================
# main() — happy path + DLX-контракт
# =====================================================================


@pytest.mark.asyncio
async def test_worker_wires_dlx_and_consumes_command():
    mock_proc = AsyncMock()
    channel, process_calls, message = await _run_main(mock_proc)

    # сообщение обработано и не в requeue-петле
    mock_proc.assert_called_once_with(message)
    assert process_calls.get("requeue") is False

    # prefetch не потерян
    assert channel.qos == {"prefetch_count": 10}

    # КЛЮЧЕВОЕ: рабочая очередь настроена на dead-letter-exchange
    cmd = channel.declared_queues["commands"]
    assert cmd["durable"] is True
    assert cmd["arguments"]["x-dead-letter-exchange"] == "commands_dlx"

    # рабочая очередь привязана по правильному routing key
    cmd_binds = channel.queues["commands"].binds
    assert any(k.get("routing_key") == "commands.purge" for _, k in cmd_binds)

    # DLQ существует и привязана к DLX (иначе «мёртвые» команды испаряются)
    assert "commands.dlq" in channel.declared_queues
    assert len(channel.queues["commands.dlq"].binds) == 1


@pytest.mark.asyncio
async def test_worker_survives_failure_and_keeps_requeue_false():
    """process_command падает → цикл ловит исключение, main не поднимает его,
    и requeue=False сохраняется (в реале это nack(requeue=False) → DLX)."""
    _, process_calls, _ = await _run_main(AsyncMock(side_effect=RuntimeError("boom")))
    assert process_calls.get("requeue") is False
