from aiohttp import web
from aiohttp_swagger3 import SwaggerDocs, SwaggerUiSettings, SwaggerInfo

from src.api.handlers import add_event, get_events, get_stats
from src.config import HOST, PORT, BATCH_SIZE, BATCH_INTERVAL
from src.db.clickhouse import ch_client
from src.queues.event_queue import EventQueue


# -----------------------------
# Инициализация приложения
# -----------------------------
def create_app() -> web.Application:
    app = web.Application()

    # Подключаем ClickHouse клиент
    app["clickhouse"] = ch_client

    # Инициализация Swagger
    SwaggerDocs(
        app,
        swagger_ui_settings=SwaggerUiSettings(path="/docs"),
        info=SwaggerInfo(
            title="Events API",
            version="1.0.0",
            description="API для работы с событиями",
        ),
    )

    # Инициализация очереди событий
    event_queue = EventQueue(batch_size=BATCH_SIZE, batch_interval=BATCH_INTERVAL)
    event_queue.bind_app(app)
    app["event_queue"] = event_queue

    # Регистрация фоновых задач
    app.on_startup.append(lambda app: event_queue.start())
    app.on_cleanup.append(lambda app: event_queue.stop())

    # Роутинг
    swagger_routes = [
        web.post("/events", add_event),
        web.get("/events", get_events),
        web.get("/stats", get_stats),
    ]
    app.router.add_routes(swagger_routes)

    return app


# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    web.run_app(create_app(), host=HOST, port=PORT)
