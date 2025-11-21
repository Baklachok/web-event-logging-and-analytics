import asyncio
from typing import List, Dict, Any, Optional

from src.api.schemas import Event
from src.utils.clickhouse_utils import prepare_clickhouse_rows
from src.utils.event_helpers import validate_events


class EventQueue:
    """
    Асинхронная очередь для событий с поддержкой батчинга
    и фоновой отправки в ClickHouse.
    """

    def __init__(self, batch_size: int, batch_interval: float) -> None:
        self.queue: asyncio.Queue[List[Event]] = asyncio.Queue()
        self.batch_size: int = batch_size
        self.batch_interval: float = batch_interval
        self._worker_task: Optional[asyncio.Task] = None
        self.app: Optional[Any] = None  # aiohttp app с client['clickhouse']

    def bind_app(self, app: Any) -> None:
        """Привязывает aiohttp app для использования ClickHouse клиента."""
        self.app = app

    async def put(self, events: List[Dict[str, Any]]) -> None:
        """
        Добавление событий в очередь.
        Валидирует каждое событие через Pydantic Event.
        """
        validated_events = validate_events(events)
        await self.queue.put(validated_events)

    async def start(self) -> None:
        """Запускает фоновый воркер очереди."""
        if not self._worker_task:
            self._worker_task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        """Останавливает фонового воркера."""
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

    # ================= Internal Worker =================

    async def _worker(self) -> None:
        """Фоновый воркер, который собирает события в батчи и отправляет их."""
        print("EventQueue worker started!")
        batch: List[Event] = []

        while True:
            try:
                try:
                    item: List[Event] = await asyncio.wait_for(
                        self.queue.get(), timeout=self.batch_interval
                    )
                    batch.extend(item)

                    while len(batch) >= self.batch_size:
                        await self._flush(batch[: self.batch_size])
                        batch = batch[self.batch_size :]

                except asyncio.TimeoutError:
                    if batch:
                        await self._flush(batch)
                        batch.clear()

            except Exception as e:
                print("QUEUE ERROR:", e)
                await asyncio.sleep(1)

    # ================= Flush =================

    async def _flush(self, batch: List[Event]) -> None:
        """Отправка батча в ClickHouse."""
        if not batch:
            return

        if self.app is None:
            raise RuntimeError("EventQueue.app is not bound. Call bind_app(app) first.")

        client = self.app["clickhouse"]
        rows = prepare_clickhouse_rows(batch)

        client.insert(
            "events", rows, column_names=["user_id", "event_type", "page", "timestamp"]
        )

        print(f"Flushed {len(batch)} events to ClickHouse")
