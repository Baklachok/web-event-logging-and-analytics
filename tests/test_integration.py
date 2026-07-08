import asyncio
import time

import httpx
import pytest
from clickhouse_driver import Client  # type: ignore

TOTAL_EVENTS = 10_000
BATCH_SIZE = 500
CONCURRENCY = 10  # одновременных POST-запросов к API
API_URL = "http://127.0.0.1:8080/events"

# Ожидание Kafka Engine: флашит батчами по kafka_flush_interval_ms (~7.5с)
# или по kafka_max_block_size — момент недетерминирован, поэтому поллим.
POLL_TIMEOUT_S = 120
POLL_INTERVAL_S = 2

# Нативный протокол ClickHouse (порт 9000)
client = Client(host="localhost")


async def _wait_for_ingested(expected: int) -> tuple[int, int]:
    """Поллит ClickHouse, пока uniqExact(user_id) не достигнет expected
    или не выйдет таймаут. Возвращает (total_rows, distinct_users)."""
    deadline = time.monotonic() + POLL_TIMEOUT_S
    total = distinct = 0
    while time.monotonic() < deadline:
        total, distinct = client.execute(
            "SELECT count(), uniqExact(user_id) FROM events"
        )[0]
        if distinct >= expected:
            return total, distinct
        await asyncio.sleep(POLL_INTERVAL_S)
    return total, distinct


@pytest.mark.asyncio
async def test_events_pipeline():
    # TRUNCATE чистит MergeTree, но НЕ оффсеты консьюмер-группы Kafka Engine:
    # уже закоммиченные сообщения не перечитываются — новые события польются поверх.
    client.execute("TRUNCATE TABLE events")

    events = [
        {
            "user_id": i,
            "event_type": "click",
            "page": "home",
            "timestamp": "2025-11-23T12:00:00+00:00",
        }
        for i in range(1, TOTAL_EVENTS + 1)
    ]

    # Один семафор на весь прогон бьёт глобальную конкурентность, а не на батч.
    semaphore = asyncio.Semaphore(CONCURRENCY)
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)

    async with httpx.AsyncClient(limits=limits, timeout=30.0) as session:

        async def send_event(event: dict) -> None:
            async with semaphore:
                resp = await session.post(API_URL, json=event)
                assert resp.status_code in (200, 201), (
                    f"POST failed: {resp.status_code} {resp.text}"
                )

        # Батчим постановку задач, чтобы не держать 10k корутин разом в памяти;
        # реальную конкурентность всё равно ограничивает семафор.
        for i in range(0, TOTAL_EVENTS, BATCH_SIZE):
            batch = events[i : i + BATCH_SIZE]
            await asyncio.gather(*(send_event(e) for e in batch))

    # Ждём, пока Kafka Engine → MV → events домолотит батчами (не фиксированный sleep).
    total, distinct = await _wait_for_ingested(TOTAL_EVENTS)

    # Инвариант «нет потерь»: каждый уникальный user_id доехал.
    assert distinct == TOTAL_EVENTS, (
        f"Data loss: expected {TOTAL_EVENTS} distinct users, got {distinct} "
        f"(timed out after {POLL_TIMEOUT_S}s)"
    )
    # Kafka Engine — at-least-once: сырых строк может быть >= из-за пере-доставки.
    # Это не ошибка пайплайна; отмечаем дубликаты для диагностики.
    assert total >= TOTAL_EVENTS, f"Unexpected: total {total} < {TOTAL_EVENTS}"
    if total != TOTAL_EVENTS:
        print(f"[info] {total - TOTAL_EVENTS} duplicate rows (at-least-once delivery)")
