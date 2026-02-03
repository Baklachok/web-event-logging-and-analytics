from dataclasses import dataclass
from typing import Any, Mapping

from aiohttp import web

from src.db.types import ClickHouseClientProtocol
from src.utils.clickhouse_utils import rows_to_dicts, build_filters
from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class StatsQueryParams:
    event_type: str | None
    date_from: str | None
    date_to: str | None


def _get_clickhouse(app: web.Application) -> ClickHouseClientProtocol:
    return app["clickhouse"]


def _get_stats_params(query: Mapping[str, str]) -> StatsQueryParams:
    return StatsQueryParams(
        event_type=query.get("event_type"),
        date_from=query.get("date_from"),
        date_to=query.get("date_to"),
    )


def _build_stats_query(
    where_clause: str,
) -> str:
    return (
        "SELECT event_type, COUNT(*) AS count "
        "FROM events "
        f"{where_clause} "
        "GROUP BY event_type "
        "ORDER BY count DESC"
    )


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
    logger.info("Получено новое событие: %s", data)

    rabbit = request.app["rabbit"]

    logger.info("Публикация события в RabbitMQ...")
    await rabbit.publish(data)
    logger.info("Событие успешно опубликовано")

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
    client = _get_clickhouse(request.app)
    logger.info("Выполнение запроса к ClickHouse: SELECT * FROM events")
    result = client.query("SELECT * FROM events")
    events = rows_to_dicts(result.result_rows, result.column_names)

    logger.info("Получено %d событий", len(events))
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
    stats_params = _get_stats_params(request.query)

    logger.info(
        "Формирование статистики: event_type=%s, date_from=%s, date_to=%s",
        stats_params.event_type,
        stats_params.date_from,
        stats_params.date_to,
    )

    where_clause, sql_params = build_filters(
        event_type=stats_params.event_type,
        date_from=stats_params.date_from,
        date_to=stats_params.date_to,
    )
    logger.info("Сформирован WHERE-клауз: %s", where_clause)

    query = _build_stats_query(where_clause)
    logger.info("Выполнение запроса к ClickHouse: %s", query)

    client = _get_clickhouse(request.app)
    result = client.query(query, parameters=sql_params)

    stats = {row[0]: row[1] for row in result.result_rows}
    logger.info("Получена статистика по событиям: %s", stats)

    return web.json_response(stats)
