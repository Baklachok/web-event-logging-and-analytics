from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import web

from src.rabbit.rabbit import RabbitMQ


@pytest.fixture
def cli_ch_client() -> MagicMock:
    """Мок ClickHouse клиента"""
    client = MagicMock()
    client.query.return_value = MagicMock(
        result_rows=[(1, "click", "home", "2025-11-21T10:00:00")],
        column_names=["user_id", "event_type", "page", "timestamp"],
    )
    return client


@pytest.fixture
def cli_rabbit() -> AsyncMock:
    rabbit = AsyncMock(spec=RabbitMQ)  # мок знает реальную сигнатуру publish
    return rabbit


@pytest.fixture
def cli_kafka() -> AsyncMock:
    """Мок Kafka-обёртки с publish"""
    kafka = AsyncMock()
    kafka.publish = AsyncMock()
    return kafka


@pytest.fixture
def app(
    cli_ch_client: MagicMock, cli_rabbit: AsyncMock, cli_kafka: AsyncMock
) -> web.Application:
    application = web.Application()
    application["clickhouse"] = cli_ch_client
    application["rabbit"] = cli_rabbit
    application["kafka"] = cli_kafka  # ← события уезжают сюда
    return application


@pytest.fixture
def mock_request(app: web.Application):
    """
    Возвращает функцию для быстрого создания мок-запроса.

    Usage:
        request = mock_request(json_payload={"key": "value"}, query_params={"param": "1"})
    """

    def _make(json_payload=None, query_params=None):
        request = AsyncMock()
        request.app = app
        request.json = AsyncMock(return_value=json_payload or {})
        request.query = query_params or {}
        return request

    return _make
