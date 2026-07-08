from aiohttp import web

from src.config import HOST, PORT
from src.db.clickhouse import ch_client
from src.kafka.settings import setup_kafka, cleanup_kafka
from src.metrics.metrics import Metrics
from src.rabbit.settings import setup_rabbit, cleanup_rabbit, graceful_shutdown
from src.swagger.settings import setup_swagger
from src.utils.logging import setup_logging


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


async def kafka_ctx(app: web.Application):
    """Жизненный цикл Kafka-продюсера: старт до yield, остановка после."""
    await setup_kafka(app)
    yield
    await cleanup_kafka(app)


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
    app.cleanup_ctx.append(rabbit_ctx)
    app.cleanup_ctx.append(kafka_ctx)

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
