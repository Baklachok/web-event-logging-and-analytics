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
        exchange_name="commands_exchange",  # та же биржа, что биндит worker
        routing_key="commands.purge",  # дефолтный ключ — командный
        # queue_name больше не используется продюсером — можно не задавать
    )
    await rabbit.connect()
    app["rabbit"] = rabbit


async def cleanup_rabbit(app: web.Application) -> None:
    """Закрытие соединения с RabbitMQ."""
    rabbit = app.get("rabbit")
    if rabbit:
        m = app["metrics"]
        logger.info(f"Sent messages: {m.sent_messages}")
        if m.publish_latencies:
            avg_latency = sum(m.publish_latencies) / len(m.publish_latencies)
            logger.info(f"Avg publish latency: {avg_latency:.4f}s")
        else:
            logger.info("Avg publish latency: 0.0000s")
        await rabbit.close()


async def graceful_shutdown(app: web.Application):
    print("[SHUTDOWN] Stopping background tasks and closing connections...")

    # Закрываем соединение с RabbitMQ
    if "rabbit" in app:
        await app["rabbit"].close()

    print("[SHUTDOWN] Done.")
