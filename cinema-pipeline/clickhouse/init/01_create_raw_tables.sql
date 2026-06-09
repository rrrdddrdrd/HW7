CREATE DATABASE IF NOT EXISTS cinema;

CREATE TABLE IF NOT EXISTS cinema.movie_events_kafka
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       String,
    `timestamp`      Int64,
    device_type      String,
    session_id       String,
    progress_seconds Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka1:29092,kafka2:29092',
    kafka_topic_list           = 'movie-events',
    kafka_group_name           = 'clickhouse-raw-consumer',
    kafka_format               = 'AvroConfluent',
    format_avro_schema_registry_url  = 'http://schema-registry:8081',
    kafka_num_consumers        = 1,
    kafka_max_block_size       = 65536,
    kafka_skip_broken_messages = 10;


CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    event_ts         DateTime('UTC'),
    event_date       Date,
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Nullable(Int32),
    inserted_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, event_id)
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.mv_kafka_to_events
TO cinema.movie_events
AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    toDateTime(intDiv(`timestamp`, 1000), 'UTC') AS event_ts,
    toDate(toDateTime(intDiv(`timestamp`, 1000), 'UTC'))  AS event_date,
    device_type,
    session_id,
    progress_seconds
FROM cinema.movie_events_kafka;
