from aiohttp import web

from app import Event, ch_client, serialize_event


async def add_event(request: web.Request) -> web.Response:
    """Добавляет событие"""
    data = await request.json()
    event = Event(**data)

    ch_client.insert(
        "events",
        [[event.user_id, event.event_type, event.page, event.timestamp]],
        column_names=["user_id", "event_type", "page", "timestamp"],
    )

    return web.json_response({"status": "ok"}, status=201)


async def get_events(request: web.Request) -> web.Response:
    """Получает все события"""
    result = ch_client.query("SELECT * FROM events")

    rows = result.result_rows
    columns = result.column_names

    events: list[dict] = [serialize_event(dict(zip(columns, row))) for row in rows]

    return web.json_response(events)
