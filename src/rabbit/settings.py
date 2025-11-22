from aiohttp import web

from src.config import RABBIT_URL
from src.rabbit.rabbit import RabbitMQ


async def setup_rabbit(app: web.Application) -> None:
    """Подключение к RabbitMQ и сохранение в app."""
    rabbit = RabbitMQ(
        url=RABBIT_URL,
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
        await rabbit.close()
