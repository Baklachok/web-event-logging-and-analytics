"""Юнит-тесты KafkaProducer: AIOKafkaProducer замокан целиком.

Сериализаторы задаются в конструкторе AIOKafkaProducer и потому НЕ
выполняются на пути publish() при замоканном продюсере — send_and_wait
получает сырые dict/user_id. Их проверяем отдельно, доставая лямбды из
call_args конструктора.
"""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.errors import KafkaConnectionError, RequestTimedOutError

from src.kafka.kafka import KafkaProducer

TOPIC = "events"
EVENT = {
    "user_id": "u-123",
    "event_type": "page_view",
    "page": "/главная",
    "timestamp": 1_700_000_000,
}


@pytest.fixture
def kafka():
    """KafkaProducer с замоканным AIOKafkaProducer и метриками."""
    with patch("src.kafka.kafka.AIOKafkaProducer") as ctor:
        inner = MagicMock()
        inner.start = AsyncMock()
        inner.stop = AsyncMock()
        metadata = MagicMock(topic=TOPIC, partition=0, offset=42)
        inner.send_and_wait = AsyncMock(return_value=metadata)
        ctor.return_value = inner

        metrics = MagicMock()
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092", metrics=metrics, topic=TOPIC
        )
        yield SimpleNamespace(
            producer=producer,
            inner=inner,
            ctor=ctor,
            metrics=metrics,
            metadata=metadata,
        )


# ── publish: маршрутизация и ключ ─────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_uses_user_id_as_key(kafka):
    await kafka.producer.publish(EVENT)
    kafka.inner.send_and_wait.assert_awaited_once_with(TOPIC, value=EVENT, key="u-123")


@pytest.mark.asyncio
async def test_publish_returns_metadata(kafka):
    result = await kafka.producer.publish(EVENT)
    assert result is kafka.metadata


@pytest.mark.asyncio
async def test_publish_without_user_id_sends_null_key(kafka):
    # Нет user_id → key=None уходит в send_and_wait как есть.
    event = {"event_type": "ping"}
    await kafka.producer.publish(event)
    kafka.inner.send_and_wait.assert_awaited_once_with(TOPIC, value=event, key=None)


# ── publish: метрики ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_updates_metrics_on_success(kafka):
    await kafka.producer.publish(EVENT)
    kafka.metrics.inc_sent.assert_called_once()
    kafka.metrics.add_latency.assert_called_once()
    (latency,), _ = kafka.metrics.add_latency.call_args
    assert isinstance(latency, float)
    assert latency >= 0


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [KafkaConnectionError, RequestTimedOutError])
async def test_publish_reraises_broker_errors_without_touching_metrics(kafka, exc):
    kafka.inner.send_and_wait.side_effect = exc("boom")
    with pytest.raises(exc):
        await kafka.producer.publish(EVENT)
    kafka.metrics.inc_sent.assert_not_called()
    kafka.metrics.add_latency.assert_not_called()


# ── Сериализаторы (заданы в конструкторе, тестируются напрямую) ────────


def test_value_serializer_is_utf8_json(kafka):
    value_serializer = kafka.ctor.call_args.kwargs["value_serializer"]
    raw = value_serializer({"event_type": "клик", "n": 1})
    assert isinstance(raw, bytes)
    assert json.loads(raw.decode("utf-8")) == {"event_type": "клик", "n": 1}
    # ensure_ascii=False → кириллица как utf-8-байты, а не \uXXXX
    assert "клик".encode("utf-8") in raw


def test_key_serializer_stringifies_to_utf8(kafka):
    key_serializer = kafka.ctor.call_args.kwargs["key_serializer"]
    assert key_serializer("u-123") == b"u-123"
    assert key_serializer(123) == b"123"


# ── Гарантии доставки (acks=all + идемпотентность) ────────────────────


def test_producer_configured_for_idempotent_all_acks(kafka):
    kwargs = kafka.ctor.call_args.kwargs
    assert kwargs["acks"] == "all"
    assert kwargs["enable_idempotence"] is True


# ── Жизненный цикл ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_start_stop_delegate_to_producer(kafka):
    await kafka.producer.start()
    kafka.inner.start.assert_awaited_once()
    await kafka.producer.stop()
    kafka.inner.stop.assert_awaited_once()
