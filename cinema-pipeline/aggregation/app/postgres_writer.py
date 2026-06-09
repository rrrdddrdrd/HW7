from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

import psycopg2
import psycopg2.extras
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from .config import settings

logger = logging.getLogger(__name__)


def _get_conn():
    return psycopg2.connect(settings.postgres_dsn)


@retry(
    retry=retry_if_exception_type(psycopg2.OperationalError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def write_aggregates(target_date: date, metrics: dict[str, Any]) -> None:
    rows = [
        (str(target_date), metric_name, json.dumps({"value": value}))
        for metric_name, value in metrics.items()
    ]

    upsert_sql = """
        INSERT INTO cinema_aggregates (metric_date, metric_name, metric_value)
        VALUES (%s, %s, %s::jsonb)
        ON CONFLICT (metric_date, metric_name)
        DO UPDATE SET
            metric_value = EXCLUDED.metric_value,
            computed_at  = now()
    """

    try:
        conn = _get_conn()
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, upsert_sql, rows)
        conn.close()
        logger.info("Wrote %d metric rows to PostgreSQL for %s", len(rows), target_date)
    except Exception:
        logger.exception("PostgreSQL write failed for %s", target_date)
        raise
