from datetime import datetime
from typing import Dict

from aiohttp import web
from aiohttp_swagger3 import SwaggerDocs, SwaggerUiSettings, SwaggerInfo
from pydantic import BaseModel
from clickhouse_connect.driver import create_client

from src.api.handlers import add_event, get_events


# -----------------------------
# МОДЕЛИ
# -----------------------------
class Event(BaseModel):
    user_id: int
    event_type: str
    page: str
    timestamp: datetime


# -----------------------------
# УТИЛИТЫ
# -----------------------------
def serialize_event(event: Dict) -> Dict:
    """Преобразует datetime в ISO строку для JSON"""
    return {
        k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in event.items()
    }


# -----------------------------
# ClickHouse клиент
# -----------------------------
ch_client = create_client(
    host="clickhouse",
    port=8123,
    username="default",
    password="",
    database="default",
)


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
swagger.add_routes([web.post("/events", add_event), web.get("/events", get_events)])


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=8080)
