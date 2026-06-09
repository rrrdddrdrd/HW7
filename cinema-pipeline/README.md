# Cinema Analytics Pipeline

Pet-pipeline для онлайн-кинотеатра: собирает события просмотров через Kafka, складывает их в ClickHouse, считает бизнес-метрики и выкладывает их в Postgres, Grafana и S3.

В основе лежит идея: есть условный "Movie Service", который отправляет события (юзер начал смотреть, поставил на паузу, лайкнул, досмотрел и т.д.), а мы эти события обрабатываем и превращаем в аналитику для продактов.

## Из чего всё состоит

```
 producer (FastAPI + фоновый генератор)
        │
        ▼
     Kafka  — 2 брокера, репликация 2, 3 партиции
        │    + Schema Registry с Avro-схемой
        ▼
  ClickHouse — Kafka Engine читает топик,
               Materialized View перекладывает в movie_events (ReplacingMergeTree)
        │
        ▼
  aggregation (FastAPI + APScheduler)
        ├──► PostgreSQL  — витрина агрегатов, идемпотентный upsert
        └──► MinIO       — Parquet-архив по дням
                              ▲
     Grafana ──────────────────┘  дашборды поверх ClickHouse-агрегатов
```

Сырые события в ClickHouse лежат в `cinema.movie_events`, предрассчитанные агрегаты — в отдельных `cinema.agg_*` таблицах. Grafana читает только агрегаты, никаких тяжёлых запросов по сырым данным.

## Как запустить

Всё разворачивается через docker-compose одной командой:

```bash
docker compose up -d
```

Первый запуск займёт пару минут — нужно скачать образы Kafka, ClickHouse, Grafana и т.д. При старте автоматически:

- создаётся Kafka-топик `movie-events` с RF=2 и 3 партициями
- регистрируется Avro-схема в Schema Registry
- прогоняются миграции ClickHouse и Postgres
- создаётся бакет `movie-analytics` в MinIO
- стартует фоновый генератор событий в продюсере

То есть ничего руками доделывать не нужно — через минуту-две после `up -d` данные уже текут по пайплайну.

Посмотреть логи:

```bash
docker compose logs -f producer aggregation
```

Прогнать интеграционные тесты:

```bash
docker compose --profile test run --rm tests
```

Тесты проверяют сквозной путь: отправили HTTP-событие в продюсер → убедились, что оно доехало до ClickHouse и поля совпадают, плюс проверяют идемпотентность и валидацию.

## Куда тыкать

| Что                   | Где                            | Как войти                 |
|-----------------------|--------------------------------|---------------------------|
| Producer (Swagger)    | http://localhost:8000/docs     | —                         |
| Aggregation (Swagger) | http://localhost:8001/docs     | —                         |
| Grafana               | http://localhost:3000          | admin / admin             |
| ClickHouse HTTP       | http://localhost:8123          | cinema_user / cinema_pass |
| Schema Registry       | http://localhost:8081          | —                         |
| MinIO Console         | http://localhost:9091          | minioadmin / minioadmin   |
| PostgreSQL            | localhost:5432                 | cinema_user / cinema_pass |

Grafana поднимается с уже прошитым датасорсом и импортированным дашбордом — идёшь в папку "Cinema" и смотришь.

## Как отправить событие руками

```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_0001",
    "movie_id": "movie_001",
    "event_type": "VIEW_STARTED",
    "device_type": "DESKTOP",
    "session_id": "550e8400-e29b-41d4-a716-446655440000",
    "progress_seconds": 0
  }'
```

Ответ (HTTP 202):

```json
{
  "event_id": "a1b2c3...",
  "status": "published",
  "topic": "movie-events",
  "partition": 1,
  "offset": 42
}
```

Через несколько секунд это событие уже будет в ClickHouse, а ещё через час (или сразу, если дёрнуть `/aggregate` руками) — попадёт в Postgres и S3.

## Немного про архитектурные решения

**Ключ партиционирования — `user_id`.** Не `event_id` (тогда события одного юзера разлетелись бы по разным партициям и порядок бы сломался), не `movie_id` (популярный фильм превратил бы одну партицию в бутылочное горлышко). `user_id` даёт и хорошее распределение нагрузки, и гарантию порядка внутри пользовательской сессии — что нужно для корректного retention и сессионной аналитики.

**Avro, а не JSON/Protobuf.** Protobuf классный, но с Schema Registry и Kafka Engine в ClickHouse пара Avro + Schema Registry поддерживается из коробки через формат `AvroConfluent` — никаких proto-компилей, магических байт и прочего ручного кода. JSON можно было бы, но тогда теряем контроль схемы и компактность.

**ClickHouse читает Kafka сам.** Через Kafka Engine + Materialized View — это стандартный паттерн. Не нужен отдельный consumer-сервис, который бы перекладывал данные. ClickHouse сам поддерживает группу консьюмеров, сам коммитит офсеты. Минимум движущихся частей.

**ReplacingMergeTree для `movie_events`.** Если одно и то же событие прилетит дважды (сетевой ретрай, ребаланс консьюмеров), `ReplacingMergeTree` сам схлопнет дубли по ключу сортировки. Для аналитики этого достаточно, а `FINAL` в запросах используем только когда точность критична (например, в тесте на идемпотентность).

**Идемпотентность в Postgres через `ON CONFLICT`.** Таблица `cinema_aggregates` имеет `UNIQUE(metric_date, metric_name)`, запись идёт через `INSERT ... ON CONFLICT DO UPDATE`. Повторный пересчёт за ту же дату просто обновит значения, не создаст дубликатов.

**S3 как cold storage.** Parquet-файлы лежат по пути `daily/YYYY-MM-DD/aggregates.parquet` — детерминированный ключ, повторная заливка перезаписывает файл. Формат колоночный, сжатый, читается из DuckDB/Spark/Athena без импорта — удобно для долгосрочного архива.

## Бизнес-метрики

Считаются сервисом агрегации раз в час (cron настраивается через `AGGREGATION_SCHEDULE`), все запросы идут по дневной гранулярности.

- **DAU** — `uniq(user_id)` за день
- **Average watch time** — `avg(progress_seconds)` по событиям `VIEW_FINISHED`
- **Top movies** — `uniq(user_id)` на фильм, сортировка по убыванию
- **Conversion rate** — `sumIf(VIEW_FINISHED) / sumIf(VIEW_STARTED)`
- **Retention D1..D7** — когортный анализ: находим юзеров с первым `VIEW_STARTED` в день N, смотрим сколько из них вернулось в каждый из следующих 7 дней
- **Device distribution** — раскладка событий по типам устройств

Результаты лежат одновременно в трёх местах: в ClickHouse (для Grafana, ReplacingMergeTree таблицы `cinema.agg_*`), в Postgres (витрина в `cinema_aggregates`, JSONB-значения) и в S3 (Parquet-архив).

## Дашборд

Grafana при старте подтягивает дашборд `Cinema Analytics` из provisioning. В нём 7 панелей:

1. DAU — временной ряд
2. Conversion Rate — временной ряд
3. Top 10 Movies — барчарт за последние 7 дней
4. Average Watch Time — временной ряд
5. **Retention Cohort Heatmap** — когортная таблица Day 0..Day 7
6. Device Distribution — пирог
7. D1 vs D7 Retention — два ряда на одном графике

Ключевая панель — cohort heatmap. Она построена на основе предрассчитанной таблицы `cinema.agg_retention`, где для каждой даты-когорты хранится 8 строк (день 0, 1, 2, ..., 7) с процентом удержания. В Grafana это разворачивается в широкую таблицу через `maxIf(..., day_number = N)`.

## Отказоустойчивость

На что обратили внимание:

- Kafka — 2 брокера, у топика `replication.factor=2`, `min.insync.replicas=1`. Падение одного брокера пайплайн переживёт (продюсер с `acks=all` продолжит писать через живой).
- Внутренние топики Kafka (`__consumer_offsets`, `__transaction_state`, `_schemas`) тоже реплицируются с RF=2.
- Продюсер: `acks=all` + `enable.idempotence=true` + экспоненциальный ретрай.
- Агрегация: tenacity с `wait_exponential` поверх записи в Postgres и S3, до 5 попыток. Если совсем не получилось — ошибка в логи, следующий крон пересчитает.
- Health checks стоят у всех сервисов, `depends_on` с `condition: service_healthy` — то есть контейнеры стартуют в правильном порядке, без гонок.
- `restart: unless-stopped` на всех долгоживущих контейнерах.

## Если хочется потыкать самому

Сгенерить исторические данные за последние 8 дней (реалистичные — с тирами пользователей, зипф-распределением фильмов и падающим retention):

```bash
python seed_demo_data.py
```

Запустить агрегацию за конкретную дату вручную:

```bash
curl -X POST "http://localhost:8001/aggregate?target_date=2026-04-23"
```

Посмотреть, что уехало в S3:

```bash
# UI: http://localhost:9091 → bucket movie-analytics → daily/...
# или через mc CLI в контейнере init-minio
```

Заглянуть в Postgres:

```bash
docker exec -it postgres psql -U cinema_user -d cinema_aggregates \
  -c "SELECT * FROM v_daily_metrics ORDER BY metric_date DESC LIMIT 7;"
```

## Структура репозитория

```
cinema-pipeline/
├── producer/            # FastAPI-сервис публикации + фоновый генератор
├── aggregation/         # FastAPI-сервис расчёта метрик, экспорта в PG и S3
├── clickhouse/init/     # SQL-миграции (Kafka Engine, MV, агрегатные таблицы)
├── postgres/init/       # SQL-миграции для витрины агрегатов
├── grafana/provisioning # datasource + dashboard, импортируется на старте
├── schemas/             # Avro-схема события (и html-визуализация)
├── scripts/             # init-kafka.sh — создание топика и регистрация схемы
├── tests/               # pytest интеграционных тестов
├── seed_demo_data.py    # генератор демо-данных за 8 дней истории
└── docker-compose.yml
```

## Конфигурация

Всё настраивается через env-переменные в `docker-compose.yml`. Самые полезные:

| Переменная             | Дефолт         | Что делает                                |
|------------------------|----------------|-------------------------------------------|
| `AGGREGATION_SCHEDULE` | `0 * * * *`    | Cron-расписание пересчёта (раз в час)     |
| `GENERATOR_ENABLED`    | `true`         | Включает фоновую генерацию событий        |
| `GENERATOR_INTERVAL_MS`| `500`          | Период между синтетическими сессиями      |
| `KAFKA_ACKS`           | `all`          | Подтверждение от всех ISR-реплик          |
| `S3_ENDPOINT`          | `minio:9000`   | Адрес S3-совместимого хранилища           |

Менять — в docker-compose.yml и перезапускать соответствующий сервис (`docker compose up -d --force-recreate aggregation`).

## CI/CD

GitHub Actions pipeline (`.github/workflows/ci.yml`) запускается на каждый push и PR. Стадии:

1. **build** — сборка Docker-образов producer, aggregation, tests.
2. **unit-tests** — запускаются напрямую на раннере без Docker (`pytest tests/unit/`), не требуют инфраструктуры.
3. **integration-e2e-tests** — поднимает `docker compose up`, ждёт `service_healthy` у producer и ClickHouse, запускает `docker compose --profile test run --rm tests`. Падает если любой тест упал.
4. **load-tests** — поднимает стек заново, запускает k6 через `--profile load-test`, затем проверяет SLI через Prometheus API. Артефакты: `load-test-results/`, `metrics-check-result.json`.

## Мониторинг

После `docker compose up -d` доступны:

| Что               | Где                        | Логин           |
|-------------------|----------------------------|-----------------|
| Prometheus        | http://localhost:9095      | —               |
| Alertmanager      | http://localhost:9094      | —               |
| Grafana           | http://localhost:3000      | admin / admin   |

Grafana поднимается с тремя дашбордами в папке **Cinema**:
- **Cinema Analytics** — бизнес-метрики (DAU, retention, conversion) из ClickHouse
- **Cinema Services Metrics** — HTTP метрики (latency p50/p95/p99, error rate, throughput) из Prometheus
- **Cinema Infrastructure** — состояние Kafka, PostgreSQL, ClickHouse из экспортеров

### Метрики сервисов

Оба сервиса экспортируют стандартные метрики на `/metrics`:

| Метрика | Тип | Labels |
|---------|-----|--------|
| `http_requests_total` | Counter | method, endpoint, status |
| `http_request_errors_total` | Counter | method, endpoint, error_type |
| `http_request_duration_seconds` | Histogram | method, endpoint |
| `cinema_events_published_total` | Counter | event_type |
| `cinema_aggregation_runs_total` | Counter | status |
| `cinema_aggregation_duration_seconds` | Histogram | — |

### Alert rules

Алерты определены в `prometheus/alerts.yml`. Alertmanager поднимается на порту 9094.

| Алерт | Условие | Длительность |
|-------|---------|--------------|
| `HighErrorRate` | error rate > 5% | 5m |
| `HighLatencyP95` | p95 > 1s | 5m |
| `ServiceDown` | target down | 1m |
| `KafkaConsumerLag` | lag > 1000 | 5m |
| `SLO_AvailabilityBreach` | availability < 95% | 2m |
| `SLO_LatencyBreach` | p95 > 1s | 2m |

## SLI / SLO

Три SLI описывают «здоровье» системы с точки зрения пользователя. Проверяются в CI через `scripts/check_metrics.py` после нагрузочного теста.

### SLI 1 — API Latency (p95)

**Что измеряется:** 95-й перцентиль времени ответа producer-сервиса на запросы публикации событий.

**PromQL:**
```promql
histogram_quantile(
  0.95,
  sum(rate(http_request_duration_seconds_bucket{job="producer"}[5m])) by (le)
)
```

| | Значение |
|-|---------|
| SLO | < 500 ms |
| Порог отказа | > 1000 ms |

**Обоснование:** Время ответа < 500 ms достаточно для real-time публикации событий просмотра; > 1 с — пользователь уже замечает задержку и буферизация на стороне клиента начинает переполняться.

---

### SLI 2 — Availability (процент успешных запросов)

**Что измеряется:** Доля запросов к producer, завершившихся без ошибки (HTTP 2xx/3xx).

**PromQL:**
```promql
1 - (
  sum(rate(http_request_errors_total{job="producer"}[5m]))
  /
  sum(rate(http_requests_total{job="producer"}[5m]))
)
```

| | Значение |
|-|---------|
| SLO | > 99.5% |
| Порог отказа | < 95% |

**Обоснование:** 99.5% — стандартный уровень для non-critical API, который допускает кратковременные рестарты. < 95% означает массовые сбои (перегрузка Kafka, падение брокера) и является основанием для SLO-алерта.

---

### SLI 3 — Kafka Consumer Lag

**Что измеряется:** Суммарное отставание консьюмер-группы ClickHouse от топика `movie-events`.

**PromQL:**
```promql
sum(kafka_consumergroup_lag{consumergroup="clickhouse-raw-consumer"})
```

| | Значение |
|-|---------|
| SLO | < 1 000 сообщений |
| Порог отказа | > 10 000 сообщений |

**Обоснование:** Lag < 1000 означает, что ClickHouse успевает за продюсером в real-time (при 2 событиях/с это < 8 минут отставания). Lag > 10 000 — ClickHouse перестал читать (зависание, нехватка CPU/памяти), данные устаревают на часы.

---

Все три SLI реализованы в CI: при нарушении порогов отказа `scripts/check_metrics.py` возвращает `exit code 1` и пайплайн падает. Тот же набор порогов продублирован в alert rules Prometheus (`SLO_AvailabilityBreach`, `SLO_LatencyBreach`).

