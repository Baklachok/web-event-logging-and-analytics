from aiohttp import web

from src.api.schemas import Event
from src.db.clickhouse import ch_client
from src.utils.clickhouse_utils import rows_to_dicts, build_filters


async def add_event(request: web.Request) -> web.Response:
    """
    Добавляет событие
    ---
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              user_id:
                type: integer
              event_type:
                type: string
              page:
                type: string
              timestamp:
                type: string
                format: date-time
    responses:
      '201':
        description: Событие добавлено
    """
    data = await request.json()
    event = Event(**data)

    ch_client.insert(
        table="events",
        data=[[event.user_id, event.event_type, event.page, event.timestamp]],
        column_names=["user_id", "event_type", "page", "timestamp"],
    )

    return web.json_response({"status": "ok"}, status=201)


async def get_events(request: web.Request) -> web.Response:
    """
    Получает все события
    ---
    responses:
      '200':
        description: Список событий
        content:
          application/json:
            schema:
              type: array
              items:
                type: object
                properties:
                  user_id:
                    type: integer
                  event_type:
                    type: string
                  page:
                    type: string
                  timestamp:
                    type: string
                    format: date-time
    """
    result = ch_client.query("SELECT * FROM events")
    events = rows_to_dicts(result.result_rows, result.column_names)
    return web.json_response(events)


async def get_stats(request: web.Request) -> web.Response:
    """
    Получение аналитики по событиям
    ---
    parameters:
      - name: event_type
        in: query
        required: false
        schema:
          type: string
      - name: date_from
        in: query
        required: false
        schema:
          type: string
          format: date-time
      - name: date_to
        in: query
        required: false
        schema:
          type: string
          format: date-time
    responses:
      '200':
        description: Статистика событий
        content:
          application/json:
            schema:
              type: object
              additionalProperties:
                type: integer
    """
    event_type = request.query.get("event_type")
    date_from = request.query.get("date_from")
    date_to = request.query.get("date_to")

    where_clause = build_filters(event_type, date_from, date_to)

    query = f"""
        SELECT event_type, COUNT(*) AS count
        FROM events
        {where_clause}
        GROUP BY event_type
        ORDER BY count DESC
    """
    result = ch_client.query(query)
    stats = {row[0]: row[1] for row in result.result_rows}
    return web.json_response(stats)
