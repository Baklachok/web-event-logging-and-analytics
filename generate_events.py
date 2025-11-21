import asyncio
import aiohttp
import random
import string
from datetime import datetime

API_URL = "http://127.0.0.1:8080/events"  # Замени на свой endpoint
TOTAL_EVENTS = 1000  # Количество событий для генерации
BATCH_SIZE = 100  # Размер батча
CONCURRENT_WORKERS = 5  # Одновременно отправляемых батчей


def random_string(length=8):
    """Генерируем случайную строку для данных события."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_event():
    return {
        "user_id": random.randint(1, 1000),
        "event_type": random.choice(["click", "view", "purchase", "login"]),
        "timestamp": datetime.utcnow().isoformat(),  # <- теперь совпадает с моделью
        "page": random.choice(["home", "product", "checkout", "profile"]),
        "data": random_string(16),
    }


def generate_batches(total_events, batch_size):
    """Генератор пачек событий."""
    batch = []
    for _ in range(total_events):
        batch.append(generate_event())
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


async def send_batch(session, batch):
    """Отправка одного батча событий в API."""
    try:
        async with session.post(API_URL, json=batch) as resp:
            if resp.status == 200 or resp.status == 201:
                print(f"Отправлено {len(batch)} событий")
            else:
                print(f"Ошибка {resp.status} при отправке батча")
    except Exception as e:
        print(f"Ошибка при отправке батча: {e}")


async def main():
    """Главная асинхронная функция."""
    batches = list(generate_batches(TOTAL_EVENTS, BATCH_SIZE))
    print(f"Всего батчей: {len(batches)}")

    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENT_WORKERS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [send_batch(session, batch) for batch in batches]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
