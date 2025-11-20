from clickhouse_connect import create_client

ch_client = create_client(
    host="clickhouse",
    port=8123,
    username="default",
    password="",
    database="default",
)
