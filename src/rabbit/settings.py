from aiohttp import web

from src.config import RABBIT_URL
from src.rabbit.rabbit import RabbitMQ
from src.utils.logging import get_logger

logger = get_logger(__name__)


async def setup_rabbit(app: web.Application) -> None:
    """Подключение к RabbitMQ и сохранение в app."""
    rabbit = RabbitMQ(
        url=RABBIT_URL,
        metrics=app["metrics"],
        exchange_name="events_exchange",
        queue_name="events_queue",
        routing_key="events.key",
    )
    await rabbit.connect()
    app["rabbit"] = rabbit


async def cleanup_rabbit(app: web.Application) -> None:
    """Закрытие соединения с RabbitMQ."""
    rabbit = app.get("rabbit")
    if rabbit:
        m = app["metrics"]
        logger.info(f"Sent messages: {m.sent_messages}")
        logger.info(
            f"Avg publish latency: {sum(m.publish_latencies) / len(m.publish_latencies):.4f}s"
        )
        await rabbit.close()


async def graceful_shutdown(app: web.Application):
    print("[SHUTDOWN] Stopping background tasks and closing connections...")

    # Останавливаем очередь событий
    if "event_queue" in app:
        await app["event_queue"].stop()

    # Закрываем соединение с RabbitMQ
    if "rabbit" in app:
        await app["rabbit"].close()

    print("[SHUTDOWN] Done.")
