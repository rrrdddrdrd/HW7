CREATE TABLE IF NOT EXISTS cinema_aggregates (
    id           BIGSERIAL PRIMARY KEY,
    metric_date  DATE        NOT NULL,
    metric_name  TEXT        NOT NULL,
    metric_value JSONB       NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_cinema_aggregates_date_name UNIQUE (metric_date, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_cinema_aggregates_date
    ON cinema_aggregates (metric_date DESC);

CREATE INDEX IF NOT EXISTS idx_cinema_aggregates_name
    ON cinema_aggregates (metric_name);

CREATE OR REPLACE VIEW v_daily_metrics AS
SELECT
    metric_date,
    MAX(CASE WHEN metric_name = 'dau'               THEN (metric_value->>'value')::numeric END) AS dau,
    MAX(CASE WHEN metric_name = 'avg_watch_seconds' THEN (metric_value->>'value')::numeric END) AS avg_watch_seconds,
    MAX(CASE WHEN metric_name = 'conversion_rate'   THEN (metric_value->>'value')::numeric END) AS conversion_rate,
    MAX(computed_at) AS last_computed_at
FROM cinema_aggregates
GROUP BY metric_date
ORDER BY metric_date DESC;
