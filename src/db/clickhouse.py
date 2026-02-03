from clickhouse_connect import create_client

from src.config import CLICKHOUSE_HOST, CLICKHOUSE_HTTP_PORT

ch_client = create_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_HTTP_PORT,
    username="default",
    password="",
    database="default",
)
