from aiohttp import web

from src.config import HOST, PORT, BATCH_SIZE, BATCH_INTERVAL
from src.db.clickhouse import ch_client
from src.metrics.metrics import Metrics
from src.queues.event_queue import EventQueue
from src.rabbit.settings import setup_rabbit, cleanup_rabbit, graceful_shutdown
from src.swagger.settings import setup_swagger
from src.utils.logging import setup_logging


# -----------------------------------------------------------
# Event Queue
# -----------------------------------------------------------
async def event_queue_ctx(app: web.Application):
    """
    Контекст менеджер для EventQueue.
    Автоматически запускает и останавливает очередь.
    """
    queue = EventQueue(batch_size=BATCH_SIZE, batch_interval=BATCH_INTERVAL)
    queue.bind_app(app)

    app["event_queue"] = queue
    await queue.start()
    yield
    await queue.stop()


# -----------------------------------------------------------
# RabbitMQ
# -----------------------------------------------------------
async def rabbit_ctx(app: web.Application):
    """
    Контекстный менеджер для RabbitMQ.
    Гарантирует корректный запуск и закрытие соединения.
    """
    await setup_rabbit(app)
    yield
    await cleanup_rabbit(app)


# -----------------------------------------------------------
# Основной конструктор приложения
# -----------------------------------------------------------
def create_app() -> web.Application:
    """Создаёт и полностью инициализирует aiohttp приложение."""
    setup_logging()
    app = web.Application()

    # ClickHouse client
    app["clickhouse"] = ch_client

    # Metrics
    app["metrics"] = Metrics()

    # Контекстные менеджеры сервисов
    app.cleanup_ctx.append(event_queue_ctx)
    app.cleanup_ctx.append(rabbit_ctx)

    # Swagger и маршруты
    setup_swagger(app)

    # Graceful shutdown
    app.on_shutdown.append(graceful_shutdown)

    return app


# -----------------------------------------------------------
# Entry point
# -----------------------------------------------------------
if __name__ == "__main__":
    web.run_app(create_app(), host=HOST, port=PORT)
