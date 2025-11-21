from typing import Any

from aiohttp import web

from src.db.types import ClickHouseClientProtocol
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
    data: dict[str, Any] = await request.json()

    queue = request.app["event_queue"]
    await queue.put(data)

    return web.json_response({"status": "queued"}, status=201)


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
    client: ClickHouseClientProtocol = request.app["clickhouse"]
    result = client.query("SELECT * FROM events")

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

    where_clause = build_filters(
        event_type=event_type,
        date_from=date_from,
        date_to=date_to,
    )

    query = f"""
        SELECT event_type, COUNT(*) AS count
        FROM events
        {where_clause}
        GROUP BY event_type
        ORDER BY count DESC
    """

    client: ClickHouseClientProtocol = request.app["clickhouse"]
    result = client.query(query)

    stats = {row[1]: row[0] for row in result.result_rows}
    return web.json_response(stats)
