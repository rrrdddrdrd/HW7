CREATE TABLE IF NOT EXISTS cinema.agg_dau
(
    metric_date Date,
    dau         UInt64,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;


CREATE TABLE IF NOT EXISTS cinema.agg_avg_watch_time
(
    metric_date Date,
    avg_seconds Float64,
    total_views UInt64,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;


CREATE TABLE IF NOT EXISTS cinema.agg_top_movies
(
    metric_date Date,
    movie_id    String,
    view_count  UInt64,
    rank        UInt16,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, rank);


CREATE TABLE IF NOT EXISTS cinema.agg_conversion
(
    metric_date     Date,
    started         UInt64,
    finished        UInt64,
    conversion_rate Float64,
    computed_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;


CREATE TABLE IF NOT EXISTS cinema.agg_retention
(
    cohort_date    Date,
    day_number     UInt8,
    cohort_size    UInt64,
    returned       UInt64,
    retention_rate Float64,
    computed_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (cohort_date, day_number);


CREATE TABLE IF NOT EXISTS cinema.user_first_event
(
    user_id     String,
    first_date  Date,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY user_id;


CREATE TABLE IF NOT EXISTS cinema.agg_device_distribution
(
    metric_date  Date,
    device_type  LowCardinality(String),
    event_count  UInt64,
    computed_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, device_type);
