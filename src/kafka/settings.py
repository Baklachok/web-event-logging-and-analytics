import logging

from src.config import (
    KAFKA_ACKS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_EVENTS_TOPIC,
    KAFKA_IDEMPOTENCE,
)
from src.kafka.kafka import KafkaProducer

logger = logging.getLogger(__name__)


async def setup_kafka(app) -> None:
    """Инициализация продюсера при старте приложения"""
    logger.info("Инициализация Kafka Producer...")

    metrics_client = app.get("metrics")  # aiohttp: метрики кладутся тем же dict-стилем

    producer_wrapper = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        metrics=metrics_client,
        topic=KAFKA_EVENTS_TOPIC,  # "events" — одно имя в конфиге, общее с DDL Kafka Engine
        acks=KAFKA_ACKS,
        enable_idempotence=KAFKA_IDEMPOTENCE,
    )
    await producer_wrapper.start()

    app["kafka"] = producer_wrapper
    logger.info("Kafka Producer успешно запущен и готов к работе.")


async def cleanup_kafka(app) -> None:
    """Корректное закрытие соединений при остановке приложения"""
    logger.info("Остановка Kafka Producer...")

    producer_wrapper = app.get("kafka")  # тот же ключ, что и в setup
    if producer_wrapper is not None:
        await producer_wrapper.stop()  # внутри await self.producer.stop()
        logger.info("Kafka Producer успешно остановлен.")
    else:
        logger.warning("Kafka Producer не найден в app при очистке.")
