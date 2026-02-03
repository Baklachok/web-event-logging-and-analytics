# Aiohttp Events Pipeline

Сервис приёма событий на aiohttp с очередью RabbitMQ и хранилищем ClickHouse.

## Обзор

- API принимает события и публикует их в RabbitMQ.
- Worker читает сообщения из RabbitMQ и пишет в ClickHouse.
- ClickHouse хранит события и позволяет строить аналитику.

## Архитектура

```
Client -> aiohttp API -> RabbitMQ -> worker -> ClickHouse
```

## API эндпоинты

- `POST /events` — поставить событие в очередь
- `GET /events` — список всех событий
- `GET /stats` — агрегаты по `event_type`

### Схема события

```
{
  "user_id": 1,
  "event_type": "click",
  "page": "home",
  "timestamp": "2025-11-23T12:00:00+00:00"
}
```

## Запуск локально (Docker)

1) Поднять сервисы:

```
docker-compose up -d
```

2) API доступен по адресу:

- `http://127.0.0.1:8080`
- Swagger UI: `http://127.0.0.1:8080/docs`

## Переменные окружения

Используются API и worker контейнерами (см. `docker-compose.yml`).

- `HOST` (по умолчанию `0.0.0.0`)
- `PORT` (по умолчанию `8080`)
- `RABBIT_URL` (по умолчанию `amqp://guest:guest@rabbitmq:5672/`)
- `CLICKHOUSE_HOST` (по умолчанию `clickhouse`)
- `CLICKHOUSE_HTTP_PORT` (по умолчанию `8123`) — для API (clickhouse_connect)
- `CLICKHOUSE_NATIVE_PORT` (по умолчанию `9000`) — для worker (clickhouse_driver)

## Тесты

Юнит‑тесты:

```
pytest -k "not integration"
```

Интеграционный тест (нужны поднятые сервисы):

```
pytest tests/test_integration.py
```

## Структура проекта

- `app.py` — создание aiohttp приложения и запуск
- `src/api/` — HTTP хендлеры и схемы
- `src/rabbit/` — клиент RabbitMQ и wiring
- `worker/worker.py` — consumer, пишет в ClickHouse
- `src/db/` — инициализация клиента ClickHouse
- `tests/` — юнит и интеграционные тесты

## Примечания

- Схема ClickHouse инициализируется из `init_db.sql` при старте контейнера.
- Целевая цепочка ingestion: API -> RabbitMQ -> worker -> ClickHouse.
