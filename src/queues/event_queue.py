import asyncio
from typing import List, Dict, Any

from src.api.schemas import Event  # Pydantic-модель


class EventQueue:
    def __init__(self, batch_size: int, batch_interval: float):
        self.queue: asyncio.Queue[Event] = asyncio.Queue()
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self._task: asyncio.Task | None = None
        self.app = None  # сюда подставляется aiohttp app

    def bind_app(self, app):
        """Позволяет очереди работать через app['clickhouse']"""
        self.app = app

    async def put(self, event: Dict[str, Any]):
        """
        Добавление события в очередь.
        Здесь валидируем timestamp и делаем datetime вместо строки.
        """
        validated = Event(**event)  # timestamp превращается в datetime
        await self.queue.put(validated)

    async def start(self):
        if not self._task:
            self._task = asyncio.create_task(self._worker())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _worker(self) -> None:
        print("Worker started!")
        batch: List[Event] = []

        while True:
            try:
                try:
                    # Ждём событие batch_interval секунд
                    item = await asyncio.wait_for(
                        self.queue.get(), timeout=self.batch_interval
                    )
                    batch.append(item)

                    # если батч заполнен — отправляем
                    if len(batch) >= self.batch_size:
                        await self._flush(batch)
                        batch.clear()

                except asyncio.TimeoutError:
                    # если истёк таймаут — отправляем накопленное
                    if batch:
                        await self._flush(batch)
                        batch.clear()

            except Exception as e:
                print("QUEUE ERROR:", e)
                await asyncio.sleep(1)

    async def _flush(self, batch: List[Event]) -> None:
        print("Flush starting", batch)

        if not batch:
            return

        if self.app is None:
            raise RuntimeError("EventQueue.app is not bound. Call bind_app(app) first.")

        client = self.app["clickhouse"]

        rows = [
            (
                ev.user_id,
                ev.event_type,
                ev.page,
                ev.timestamp,
            )
            for ev in batch
        ]

        client.insert(
            "events", rows, column_names=["user_id", "event_type", "page", "timestamp"]
        )

        print(f"Flushed {len(batch)} events to ClickHouse")
