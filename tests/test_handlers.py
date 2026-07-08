import json
from unittest.mock import AsyncMock

import aio_pika
import pytest
from aiohttp import web

from src.api import handlers
from src.api.schemas import Event


# -----------------------------
# Тесты: add_event
# -----------------------------
@pytest.mark.asyncio
async def test_add_event_queued(app, mock_request):
    payload = {
        "user_id": 1,
        "event_type": "click",
        "page": "home",
        "timestamp": "2025-11-21T10:00:00",
    }
    request = mock_request(json_payload=payload)
    response = await handlers.add_event(request)

    expected = Event.model_validate(payload).model_dump(mode="json")
    app["kafka"].publish.assert_awaited_once_with(expected)  # не rabbit!
    app["rabbit"].publish.assert_not_awaited()  # событие НЕ должно течь в rabbit

    assert response.status == 201
    assert json.loads(response.text or "{}")["status"] == "queued"


@pytest.mark.asyncio
async def test_add_event_invalid_payload(app, mock_request):
    request = mock_request()
    request.json.side_effect = ValueError("invalid json")
    with pytest.raises(ValueError):
        await handlers.add_event(request)


@pytest.mark.asyncio
async def test_add_event_logging(app, mock_request, caplog):
    payload = {
        "user_id": 1,
        "event_type": "click",
        "page": "home",
        "timestamp": "2025-11-21T10:00:00",
    }  # валидный
    request = mock_request(json_payload=payload)
    caplog.set_level("INFO", logger="src.api.handlers")
    await handlers.add_event(request)
    logs = "\n".join(caplog.messages)
    assert "Получено новое событие" in logs
    assert "опубликовано" in logs.lower()  # под новый текст лога


@pytest.mark.asyncio
async def test_add_event_rejects_invalid_schema(app, mock_request):
    """Неполный payload не должен доехать до топика."""
    request = mock_request(json_payload={"user_id": 1, "event_type": "click"})
    with pytest.raises(web.HTTPUnprocessableEntity):
        await handlers.add_event(request)
    app["kafka"].publish.assert_not_awaited()  # ключевая гарантия миграции


# -----------------------------
# Тесты: get_events
# -----------------------------
@pytest.mark.asyncio
async def test_get_events(app, mock_request):
    request = mock_request()
    response = await handlers.get_events(request)

    app["clickhouse"].query.assert_called_once_with("SELECT * FROM events")
    body = json.loads(response.text or "{}")
    assert isinstance(body, list)
    assert body[0]["user_id"] == 1


@pytest.mark.asyncio
async def test_get_events_empty(app, mock_request):
    request = mock_request()
    mock_result = AsyncMock()
    mock_result.result_rows = []
    mock_result.column_names = []
    app["clickhouse"].query.return_value = mock_result

    response = await handlers.get_events(request)
    assert json.loads(response.text or "{}") == []


# -----------------------------
# Тесты: get_stats
# -----------------------------
@pytest.mark.asyncio
async def test_get_stats(app, mock_request):
    request = mock_request(query_params={"event_type": "click"})

    app["clickhouse"].query.return_value = AsyncMock(
        result_rows=[("click", 1)],
        column_names=["event_type", "count"],
    )

    response = await handlers.get_stats(request)
    app["clickhouse"].query.assert_called_once()
    _, kwargs = app["clickhouse"].query.call_args
    assert kwargs["parameters"]["event_type"] == "click"
    assert json.loads(response.text or "{}") == {"click": 1}


@pytest.mark.asyncio
async def test_purge_user_accepted(app, mock_request):
    request = mock_request(json_payload={"user_id": 42})
    response = await handlers.purge_user(request)

    assert response.status == 202
    assert json.loads(response.text or "{}")["status"] == "accepted"

    app["rabbit"].publish.assert_awaited_once()
    _, kwargs = app["rabbit"].publish.call_args
    assert kwargs["routing_key"] == "commands.purge"  # команда поехала своим маршрутом
    app["kafka"].publish.assert_not_awaited()  # и НЕ в поток событий


@pytest.mark.asyncio
async def test_purge_user_routes_to_commands(app, mock_request):
    request = mock_request(json_payload={"user_id": 42})
    response = await handlers.purge_user(request)

    assert response.status == 202
    app["rabbit"].publish.assert_awaited_once()
    _, kwargs = app["rabbit"].publish.call_args
    assert kwargs["routing_key"] == "commands.purge"  # команда идёт своим маршрутом
    assert kwargs["mandatory"] is True  # и не теряется тихо, если некому роутить


@pytest.mark.asyncio
async def test_purge_user_unroutable_returns_503(app, mock_request):
    request = mock_request(json_payload={"user_id": 42})
    # arg signature varies slightly across aio-pika versions; (message, frame)
    app["rabbit"].publish.side_effect = aio_pika.exceptions.DeliveryError(None, None)

    with pytest.raises(web.HTTPServiceUnavailable):
        await handlers.purge_user(request)

    app["kafka"].publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_purge_user_rejects_invalid_schema(app, mock_request):
    request = mock_request(json_payload={})  # user_id отсутствует
    with pytest.raises(web.HTTPUnprocessableEntity):
        await handlers.purge_user(request)
    app["rabbit"].publish.assert_not_awaited()
    app["kafka"].publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_add_event_publishes_utc_wire_format(app, mock_request):
    request = mock_request(
        json_payload={
            "user_id": 1,
            "event_type": "click",
            "page": "home",
            "timestamp": "2025-11-21T13:00:00+03:00",
        }
    )
    await handlers.add_event(request)
    app["kafka"].publish.assert_awaited_once_with(
        {
            "user_id": 1,
            "event_type": "click",
            "page": "home",
            "timestamp": "2025-11-21 10:00:00",  # +03:00 сконвертирован в UTC
        }
    )
