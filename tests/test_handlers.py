import json

import pytest
from unittest.mock import AsyncMock, MagicMock
from aiohttp import web
from src.api import handlers


@pytest.fixture
def cli_queue():
    """Мок очереди событий"""
    queue = AsyncMock()
    return queue


@pytest.fixture
def cli_ch_client():
    """Мок ClickHouse клиента"""
    client = MagicMock()
    # Для query возвращаем объект с атрибутами result_rows и column_names
    client.query.return_value = MagicMock(
        result_rows=[(1, "click", "home", "2025-11-21T10:00:00")],
        column_names=["user_id", "event_type", "page", "timestamp"],
    )
    return client


@pytest.fixture
def app(cli_queue, cli_ch_client):
    app = web.Application()
    app["event_queue"] = cli_queue
    app["clickhouse"] = cli_ch_client
    return app


@pytest.mark.asyncio
async def test_add_event_queued(app):
    request = AsyncMock()
    request.json = AsyncMock(
        return_value={
            "user_id": 1,
            "event_type": "click",
            "page": "home",
            "timestamp": "2025-11-21T10:00:00",
        }
    )
    request.app = app

    response: web.Response = await handlers.add_event(request)

    # Проверяем, что событие добавлено в очередь
    app["event_queue"].put.assert_awaited_once()
    assert response.status == 201

    # Правильный способ получить JSON из web.Response
    body = response.text
    if isinstance(body, bytes):
        body = body.decode()
    json_body = json.loads(body)

    assert json_body["status"] == "queued"


@pytest.mark.asyncio
async def test_get_events(app):
    request = AsyncMock()
    request.app = app

    response = await handlers.get_events(request)

    app["clickhouse"].query.assert_called_once_with("SELECT * FROM events")

    body = response.text
    if isinstance(body, bytes):
        body = body.decode()
    json_body = json.loads(body)

    assert isinstance(json_body, list)
    assert json_body[0]["user_id"] == 1


@pytest.mark.asyncio
async def test_get_stats(app):
    request = AsyncMock()
    request.query = {"event_type": "click"}
    request.app = app

    # Подмена результата build_filters
    from src.utils.clickhouse_utils import build_filters

    build_filters_orig = build_filters
    try:
        import src.utils.clickhouse_utils as utils

        utils.build_filters = lambda **kwargs: "WHERE event_type='click'"

        response = await handlers.get_stats(request)

        app["clickhouse"].query.assert_called_once()

        body = response.text
        if isinstance(body, bytes):
            body = body.decode()
        json_body = json.loads(body)

        assert json_body == {"click": 1}
    finally:
        utils.build_filters = build_filters_orig
