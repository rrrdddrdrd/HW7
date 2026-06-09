from __future__ import annotations

import io
import json
import logging
from datetime import date

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from .config import settings

logger = logging.getLogger(__name__)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name="us-east-1",
    )


def _fetch_metrics(target_date: date) -> list[dict]:
    conn = psycopg2.connect(settings.postgres_dsn)
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT metric_date, metric_name, metric_value, computed_at
                FROM cinema_aggregates
                WHERE metric_date = %s
                ORDER BY metric_name
                """,
                (str(target_date),),
            )
            rows = cur.fetchall()
    conn.close()

    if not rows:
        logger.warning("No metrics in PostgreSQL for %s – skipping export", target_date)
        return []

    records = []
    for r in rows:
        value_field = r["metric_value"] if isinstance(r["metric_value"], dict) else json.loads(r["metric_value"])
        records.append(
            {
                "metric_date": str(r["metric_date"]),
                "metric_name": r["metric_name"],
                "value": value_field.get("value"),
                "computed_at": str(r["computed_at"]),
            }
        )
    return records


@retry(
    retry=retry_if_exception_type((BotoCoreError, ClientError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def export_to_s3(target_date: date) -> str:
    records = _fetch_metrics(target_date)
    if not records:
        return ""

    serializable = []
    for r in records:
        serializable.append(
            {
                "metric_date": r["metric_date"],
                "metric_name": r["metric_name"],
                "value": json.dumps(r["value"], ensure_ascii=False, default=str),
                "computed_at": r["computed_at"],
            }
        )

    df = pd.DataFrame(serializable)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    s3_key = f"daily/{target_date}/aggregates.parquet"
    s3 = _s3_client()

    try:
        s3.put_object(
            Bucket=settings.s3_bucket,
            Key=s3_key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        logger.info("Exported %d rows to s3://%s/%s", len(df), settings.s3_bucket, s3_key)
    except Exception:
        logger.exception("S3 export failed for %s", target_date)
        raise

    return s3_key
