from aiohttp import web
from aiohttp_swagger3 import SwaggerDocs, SwaggerUiSettings, SwaggerInfo

from src.api.handlers import add_event, get_events, get_stats


# -----------------------------
# Инициализация приложения
# -----------------------------
app = web.Application()

swagger = SwaggerDocs(
    app,
    swagger_ui_settings=SwaggerUiSettings(path="/docs"),
    info=SwaggerInfo(
        title="Events API", version="1.0.0", description="API для работы с событиями"
    ),
)

# -----------------------------
# РОУТИНГ
# -----------------------------
swagger.add_routes(
    [
        web.post("/events", add_event),
        web.get("/events", get_events),
        web.get("/stats", get_stats),
    ]
)


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=8080)
