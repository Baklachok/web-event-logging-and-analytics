import json
from typing import Any
import pytest
from unittest.mock import AsyncMock
from aiohttp import web
from src.api import handlers


# -----------------------------
# Тесты
# -----------------------------


@pytest.mark.asyncio
async def test_add_event_queued(app: web.Application):
    """Проверка добавления события через RabbitMQ"""
    request = AsyncMock()
    payload: dict[str, Any] = {
        "user_id": 1,
        "event_type": "click",
        "page": "home",
        "timestamp": "2025-11-21T10:00:00",
    }
    request.json = AsyncMock(return_value=payload)
    request.app = app

    response = await handlers.add_event(request)

    # Проверяем, что publish был вызван один раз с нужными данными
    app["rabbit"].publish.assert_awaited_once_with(payload)

    assert response.status == 201
    body = json.loads(response.text or "{}")

    assert body["status"] == "queued"


@pytest.mark.asyncio
async def test_get_events(app: web.Application):
    """Проверка получения всех событий"""
    request = AsyncMock()
    request.app = app

    response = await handlers.get_events(request)

    app["clickhouse"].query.assert_called_once_with("SELECT * FROM events")

    body = json.loads(response.text or "{}")

    assert isinstance(body, list)
    assert body[0]["user_id"] == 1


@pytest.mark.asyncio
async def test_get_stats(app: web.Application):
    """Проверка получения статистики событий"""
    request = AsyncMock()
    request.query = {"event_type": "click"}
    request.app = app

    # Подмена функции build_filters
    import src.utils.clickhouse_utils as utils

    original_build_filters = utils.build_filters
    utils.build_filters = lambda **kwargs: "WHERE event_type='click'"

    try:
        response = await handlers.get_stats(request)
        app["clickhouse"].query.assert_called_once()

        body = json.loads(response.text or "{}")

        assert body == {"click": 1}
    finally:
        utils.build_filters = original_build_filters
