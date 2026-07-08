from aiohttp import web
from aiohttp_swagger3 import SwaggerDocs, SwaggerUiSettings, SwaggerInfo
from src.api.handlers import add_event, get_events, get_stats, purge_user  # +purge_user


def setup_swagger(app: web.Application) -> None:
    """Инициализация Swagger документации."""
    docs = SwaggerDocs(
        app,
        swagger_ui_settings=SwaggerUiSettings(path="/docs"),
        info=SwaggerInfo(
            title="Events API",
            version="1.0.0",
            description="API для работы с событиями",
        ),
    )
    docs.add_routes(
        [
            web.post("/events", add_event),
            web.get("/events", get_events),
            web.get("/stats", get_stats),
            web.post("/commands/purge-user", purge_user),  # дискретная команда
        ]
    )
