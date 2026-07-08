# Aiohttp Events & Commands Pipeline

Сервис на aiohttp с **двумя намеренно разделёнными потоками**: высокочастотные события идут через Kafka (Redpanda) напрямую в ClickHouse, а дискретные операционные команды — через RabbitMQ на воркер. Хранилище аналитики — ClickHouse.

## Обзор

Система разводит два принципиально разных типа нагрузки по двум брокерам — это осознанный архитектурный выбор, а не историческая случайность:

**Поток событий (Kafka / Redpanda) — стрим.** `POST /events` валидирует payload через pydantic и публикует в топик `events`. ClickHouse потребляет топик **сам**, через Kafka Engine — промежуточный воркер не нужен. Kafka выбран потому, что события — это append-only поток с высокой частотой: важны пропускная способность, партиционирование (по `user_id` → порядок в пределах пользователя) и то, что ClickHouse умеет читать Kafka нативно. Потеря отдельного события в таком потоке терпима.

**Поток команд (RabbitMQ) — дискретные операции.** `POST /commands/purge-user` публикует команду с routing key `commands.purge` в topic-exchange. Воркер потребляет её и выполняет `ALTER TABLE events DELETE`. RabbitMQ выбран потому, что команда — это единичный императив «сделай один раз», где потеря недопустима (недоудалённый пользователь), а нужны точная маршрутизация по ключу, `mandatory`-доставка и **dead-letter queue** для сообщений, которые не удалось обработать. DLQ из коробки — сильная сторона RabbitMQ, которой нет у Kafka без дополнительной обвязки.

Контраст читается и в HTTP-контракте: событие → `201 queued` (положено в поток), команда → `202 accepted` (принята к исполнению).

## Архитектура

```
                          ┌─────────────────────────────────────────────┐
                          │                  ClickHouse                  │
                          │                                              │
  events (стрим)          │   events_kafka ──MV──> events (MergeTree)    │
  ┌──────────┐   Kafka    │   (Kafka Engine)   parseDateTimeBestEffort   │
  │          │─POST /events─────────> [topic: events, 3 партиции]        │
  │  Client  │            │        ClickHouse потребляет топик сам       │
  │          │            │                                              │
  │          │─POST /commands/purge-user                                 │
  └──────────┘            │                        ▲                     │
       │                  └────────────────────────┼─────────────────────┘
       │  commands (дискретно)                     │ ALTER TABLE DELETE
       ▼                                           │
  ┌──────────┐   RabbitMQ (topic)          ┌───────────────┐
  │  aiohttp │──[commands.purge]──────────>│    worker     │
  │   API    │   commands_exchange         │ command-consumer
  └──────────┘        │                    └───────────────┘
                      │ при фейле обработки        │
                      ▼ (x-dead-letter-exchange)   │ nack (requeue=False)
                 ┌─────────────┐                   │
                 │ commands.dlq│<──────────────────┘
                 └─────────────┘
```

Два потока не пересекаются: события никогда не идут через RabbitMQ, команды никогда не идут через Kafka. Воркер, который в прошлой версии писал события, перепрофилирован в command-consumer.

## API эндпоинты

- `POST /events` — валидировать и опубликовать событие в Kafka → `201 {"status": "queued"}`
- `GET /events` — список всех событий из ClickHouse
- `GET /stats` — агрегаты по `event_type` (с фильтрами `event_type`, `date_from`, `date_to`)
- `POST /commands/purge-user` — команда на удаление данных пользователя через RabbitMQ → `202 {"status": "accepted"}`

### Схема события (`POST /events`)

```json
{
  "user_id": 1,
  "event_type": "click",
  "page": "home",
  "timestamp": "2025-11-23T12:00:00+00:00"
}
```

Payload валидируется pydantic-моделью `Event` до публикации — невалидное событие получает `422` и не попадает в топик (ClickHouse Kafka Engine на структурный мусор явно не пожалуется, поэтому отсекаем на входе). Поле `timestamp` принимается в ISO-8601 с таймзоной; в Kafka-таблице оно хранится как `String` и парсится в `DateTime` через `parseDateTimeBestEffort()` в materialized view — так транспорт не завязан на формат даты.

### Схема команды (`POST /commands/purge-user`)

```json
{
  "user_id": 42
}
```

Валидируется моделью `PurgeUserCommand`. Команда публикуется с `mandatory=True`: если её некому маршрутизировать (нет привязанной очереди), брокер вернёт ошибку, а API ответит `503` вместо тихого `202` — деструктивная операция не должна пропадать незаметно.

## Потоки данных

### События: `POST /events` → Kafka → ClickHouse

1. API валидирует payload (`Event`) и публикует в топик `events` (ключ = `user_id`).
2. ClickHouse `events_kafka` (Kafka Engine) подписан на топик и вычитывает сообщения.
3. Materialized view `events_mv` парсит `timestamp` и переносит строки в `events` (MergeTree).

Воркер в этом потоке отсутствует — ClickHouse потребляет Kafka напрямую.

### Команды: `POST /commands/purge-user` → RabbitMQ → worker

1. API валидирует команду (`PurgeUserCommand`) и публикует в `commands_exchange` с ключом `commands.purge` (`mandatory=True`).
2. Воркер потребляет очередь `commands`, выполняет `ALTER TABLE events DELETE WHERE user_id = ...` синхронно (`mutations_sync=2`), затем подтверждает сообщение.
3. При ошибке обработки — `nack (requeue=False)` → сообщение уходит в `commands.dlq` через `x-dead-letter-exchange`.

Топология очереди принадлежит воркеру-консьюмеру: API-продюсер объявляет только exchange, очередь и её DLQ объявляет воркер. Это исключает конфликт `PRECONDITION_FAILED` при рассинхроне аргументов очереди.

## Запуск локально (Docker)

Поднять сервисы:

```
docker compose up -d --build
```

Доступные адреса:

- API: `http://127.0.0.1:8080`
- Swagger UI: `http://127.0.0.1:8080/docs`
- Redpanda Console (UI для топиков — аналог RabbitMQ Management): `http://127.0.0.1:8081`
- RabbitMQ Management: `http://127.0.0.1:15672` (guest / guest)
- ClickHouse HTTP: `http://127.0.0.1:8123`

Топик `events` создаётся с 3 партициями при старте (сервис `redpanda-init`), схема ClickHouse (`events`, `events_kafka`, `events_mv`) — из `init_db.sql`.

## Сервисы (docker-compose)

- `aiohttp_app` — API (порт 8080)
- `worker` — command-consumer для RabbitMQ (пишет `ALTER ... DELETE` в ClickHouse)
- `redpanda` — Kafka-совместимый брокер (внутренний listener `redpanda:9092`, внешний `localhost:19092`)
- `redpanda-console` — веб-UI для топиков (порт 8081)
- `redpanda-init` — one-shot: создаёт топик `events` с 3 партициями
- `rabbitmq` — брокер команд (порты 5672 / 15672)
- `clickhouse` — хранилище (порты 8123 / 9000)

## Переменные окружения

Используются контейнерами API и worker (см. `docker-compose.yml`).

- `HOST` (по умолчанию `0.0.0.0`)
- `PORT` (по умолчанию `8080`)
- `KAFKA_BOOTSTRAP_SERVERS` (по умолчанию `redpanda:9092`) — брокер событий для API
- `KAFKA_EVENTS_TOPIC` (по умолчанию `events`) — топик, куда публикуются события
- `RABBIT_URL` (по умолчанию `amqp://guest:guest@rabbitmq:5672/`) — брокер команд
- `CLICKHOUSE_HOST` (по умолчанию `clickhouse`)
- `CLICKHOUSE_HTTP_PORT` (по умолчанию `8123`) — для API (clickhouse_connect)
- `CLICKHOUSE_NATIVE_PORT` (по умолчанию `9000`) — для worker (clickhouse_driver)

## Тесты

Юнит-тесты:

```
pytest -k "not integration"
```

Интеграционный тест (нужны поднятые сервисы, проверяет сквозной поток событий):

```
pytest tests/test_integration.py
```

## Структура проекта

- `app.py` — создание aiohttp приложения и wiring (Kafka + RabbitMQ + ClickHouse)
- `src/api/` — HTTP хендлеры (`handlers.py`) и pydantic-схемы (`schemas.py`)
- `src/kafka/` — обёртка продюсера Kafka и lifecycle
- `src/rabbit/` — обёртка RabbitMQ (продюсер команд) и wiring
- `worker/worker.py` — command-consumer: слушает `commands`, выполняет purge, поддерживает DLQ
- `src/db/` — инициализация клиента ClickHouse
- `init_db.sql` — схема ClickHouse: `events` (MergeTree), `events_kafka` (Kafka Engine), `events_mv`
- `tests/` — юнит и интеграционные тесты

## Примечания

- Схема ClickHouse инициализируется из `init_db.sql` при **первом** старте контейнера (том пустой). При изменении схемы на существующем томе init-скрипт не переигрывается — накатывайте DDL вручную или пересоздавайте том (`docker compose down -v`).
- Поток событий: `API → Kafka (топик events) → ClickHouse Kafka Engine → MergeTree`.
- Поток команд: `API → RabbitMQ (commands_exchange) → worker → ALTER TABLE DELETE`, с DLQ `commands.dlq` для необработанных команд.