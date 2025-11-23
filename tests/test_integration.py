import asyncio
import httpx
import pytest
from clickhouse_driver import Client  # type: ignore

TOTAL_EVENTS = 10_000
BATCH_SIZE = 500
API_URL = "http://127.0.0.1:8080/events"

# ClickHouse клиент
client = Client(host="localhost")


@pytest.mark.asyncio
async def test_events_pipeline():
    client.execute("TRUNCATE TABLE events")

    # 1️⃣ Генерация событий
    events = [
        {
            "user_id": i,
            "event_type": "click",
            "page": "home",
            "timestamp": "2025-11-23T12:00:00+00:00",
        }
        for i in range(1, TOTAL_EVENTS + 1)
    ]

    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)

    async with httpx.AsyncClient(limits=limits) as session:
        for i in range(0, TOTAL_EVENTS, BATCH_SIZE):
            batch = events[i : i + BATCH_SIZE]
            # ограничиваем количество параллельных задач через семафор
            semaphore = asyncio.Semaphore(5)  # не более 10 одновременных запросов

            async def send_event(event):
                async with semaphore:
                    resp = await session.post(API_URL, json=event)
                    assert resp.status_code in (200, 201), f"Batch failed: {resp.text}"

            tasks = [send_event(event) for event in batch]
            await asyncio.gather(*tasks)

    # 3️⃣ Даем worker обработать все сообщения
    await asyncio.sleep(60)  # можно увеличить, если задержка обработки велика

    # 4️⃣ Проверка, что все события в ClickHouse
    result = client.execute("SELECT count() FROM events")[0][0]
    assert result == TOTAL_EVENTS, f"Expected {TOTAL_EVENTS}, got {result}"
