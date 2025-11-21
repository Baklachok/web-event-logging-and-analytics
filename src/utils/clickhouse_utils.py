from typing import Optional, Dict, List, Sequence

from src.api.schemas import Event
from src.utils.encoders import serialize_event


def build_filters(
    event_type: Optional[str], date_from: Optional[str], date_to: Optional[str]
) -> str:
    """
    Создает WHERE-клаузу для SQL-запроса к ClickHouse.

    :param event_type: фильтр по типу события
    :param date_from: начало диапазона даты (ISO string)
    :param date_to: конец диапазона даты (ISO string)
    :return: строка WHERE для SQL-запроса
    """
    filters = []
    if event_type:
        filters.append(f"event_type = '{event_type}'")
    if date_from:
        filters.append(f"timestamp >= '{date_from}'")
    if date_to:
        filters.append(f"timestamp <= '{date_to}'")
    return f"WHERE {' AND '.join(filters)}" if filters else ""


def rows_to_dicts(rows: Sequence[Sequence], columns: Sequence[str]) -> List[Dict]:
    """
    Преобразует результат ClickHouse в список словарей с сериализацией datetime.
    """

    return [serialize_event(dict(zip(columns, row))) for row in rows]


def prepare_clickhouse_rows(batch: List[Event]) -> list[tuple]:
    """
    Преобразует список Event в формат для ClickHouse.
    """
    return [(e.user_id, e.event_type, e.page, e.timestamp) for e in batch]
