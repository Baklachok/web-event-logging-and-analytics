import json
from unittest.mock import AsyncMock

import pytest
from src.api import handlers


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

    app["rabbit"].publish.assert_awaited_once_with(payload)
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
    payload = {"user_id": 1, "event_type": "click"}
    request = mock_request(json_payload=payload)

    caplog.set_level("INFO", logger="src.api.handlers")
    await handlers.add_event(request)

    logs = "\n".join(caplog.messages)
    assert "Получено новое событие" in logs
    assert "Публикация события" in logs


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

    import src.utils.clickhouse_utils as utils

    original_build_filters = utils.build_filters
    utils.build_filters = lambda **kwargs: "WHERE event_type='click'"

    try:
        response = await handlers.get_stats(request)
        app["clickhouse"].query.assert_called_once()
        assert json.loads(response.text or "{}") == {"click": 1}
    finally:
        utils.build_filters = original_build_filters
