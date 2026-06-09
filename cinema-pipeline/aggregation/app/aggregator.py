from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from clickhouse_driver import Client as CHClient

from .config import settings

logger = logging.getLogger(__name__)


def _ch_client() -> CHClient:
    return CHClient(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_db,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )


def compute_dau(ch: CHClient, target_date: date) -> dict:
    rows = ch.execute(
        """
        SELECT uniq(user_id) AS dau
        FROM cinema.movie_events
        WHERE event_date = %(d)s
        """,
        {"d": target_date},
    )
    dau = rows[0][0] if rows else 0
    return {"metric_date": target_date, "dau": dau}


def compute_avg_watch_time(ch: CHClient, target_date: date) -> dict:
    rows = ch.execute(
        """
        SELECT
            avg(progress_seconds)   AS avg_seconds,
            count()                 AS total_views
        FROM cinema.movie_events
        WHERE event_date = %(d)s
          AND event_type = 'VIEW_FINISHED'
          AND progress_seconds IS NOT NULL
        """,
        {"d": target_date},
    )
    avg_s, total = (rows[0][0] or 0.0, rows[0][1]) if rows else (0.0, 0)
    return {"metric_date": target_date, "avg_seconds": round(avg_s, 2), "total_views": total}


def compute_top_movies(ch: CHClient, target_date: date, top_n: int = 10) -> list[dict]:
    rows = ch.execute(
        """
        SELECT
            movie_id,
            uniq(user_id) AS view_count
        FROM cinema.movie_events
        WHERE event_date = %(d)s
          AND event_type = 'VIEW_STARTED'
        GROUP BY movie_id
        ORDER BY view_count DESC
        LIMIT %(n)s
        """,
        {"d": target_date, "n": top_n},
    )
    return [
        {"metric_date": target_date, "movie_id": r[0], "view_count": r[1], "rank": idx + 1}
        for idx, r in enumerate(rows)
    ]


def compute_conversion(ch: CHClient, target_date: date) -> dict:
    rows = ch.execute(
        """
        SELECT
            sumIf(1, event_type = 'VIEW_STARTED')  AS started,
            sumIf(1, event_type = 'VIEW_FINISHED') AS finished
        FROM cinema.movie_events
        WHERE event_date = %(d)s
          AND event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
        """,
        {"d": target_date},
    )
    started, finished = (rows[0][0], rows[0][1]) if rows else (0, 0)
    rate = round(finished / started, 4) if started > 0 else 0.0
    return {
        "metric_date": target_date,
        "started": started,
        "finished": finished,
        "conversion_rate": rate,
    }


def compute_device_distribution(ch: CHClient, target_date: date) -> list[dict]:
    rows = ch.execute(
        """
        SELECT
            device_type,
            count() AS event_count
        FROM cinema.movie_events
        WHERE event_date = %(d)s
        GROUP BY device_type
        ORDER BY event_count DESC
        """,
        {"d": target_date},
    )
    return [
        {"metric_date": target_date, "device_type": r[0], "event_count": r[1]}
        for r in rows
    ]


def compute_retention(ch: CHClient, target_date: date) -> list[dict]:
    cohort_rows = ch.execute(
        """
        SELECT user_id
        FROM cinema.movie_events
        WHERE event_type = 'VIEW_STARTED'
        GROUP BY user_id
        HAVING min(event_date) = %(d)s
        """,
        {"d": target_date},
    )
    cohort_users = {r[0] for r in cohort_rows}
    cohort_size = len(cohort_users)

    if cohort_size == 0:
        return []

    results = []
    for day_offset in range(8):
        check_date = target_date + timedelta(days=day_offset)
        returned_rows = ch.execute(
            """
            SELECT uniq(user_id) AS cnt
            FROM cinema.movie_events
            WHERE event_date = %(cd)s
              AND user_id IN %(users)s
            """,
            {"cd": check_date, "users": tuple(cohort_users)},
        )
        returned = returned_rows[0][0] if returned_rows else 0
        results.append(
            {
                "cohort_date": target_date,
                "day_number": day_offset,
                "cohort_size": cohort_size,
                "returned": returned,
                "retention_rate": round(returned / cohort_size, 4) if cohort_size > 0 else 0.0,
            }
        )
    return results


def _upsert_ch(ch: CHClient, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())
    data = [[r[c] for c in cols] for r in rows]
    ch.execute(f"INSERT INTO cinema.{table} ({', '.join(cols)}) VALUES", data)
    logger.debug("Inserted %d rows into cinema.%s", len(rows), table)


def _rows_to_serializable(rows: list[dict]) -> list[dict]:
    serialized = []
    for row in rows:
        item = {}
        for k, v in row.items():
            if isinstance(v, date):
                item[k] = v.isoformat()
            else:
                item[k] = v
        serialized.append(item)
    return serialized


def run_aggregation(target_date: Optional[date] = None) -> dict:
    if target_date is None:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    logger.info("=== Aggregation cycle START for %s ===", target_date)
    t0 = time.perf_counter()

    ch = _ch_client()
    summary = {"date": str(target_date), "metrics": {}}
    records_processed = 0

    try:
        dau = compute_dau(ch, target_date)
        _upsert_ch(ch, "agg_dau", [dau])
        summary["metrics"]["dau"] = dau["dau"]
        records_processed += 1

        awt = compute_avg_watch_time(ch, target_date)
        _upsert_ch(ch, "agg_avg_watch_time", [awt])
        summary["metrics"]["avg_watch_seconds"] = awt["avg_seconds"]
        summary["metrics"]["total_finished_views"] = awt["total_views"]
        records_processed += 1

        top = compute_top_movies(ch, target_date)
        _upsert_ch(ch, "agg_top_movies", top)
        summary["metrics"]["top_movies"] = _rows_to_serializable(top)
        records_processed += len(top)

        conv = compute_conversion(ch, target_date)
        _upsert_ch(ch, "agg_conversion", [conv])
        summary["metrics"]["conversion_rate"] = conv["conversion_rate"]
        summary["metrics"]["started"] = conv["started"]
        summary["metrics"]["finished"] = conv["finished"]
        records_processed += 1

        ret = compute_retention(ch, target_date)
        _upsert_ch(ch, "agg_retention", ret)
        summary["metrics"]["retention"] = _rows_to_serializable(ret)
        for r in ret:
            if r["day_number"] == 1:
                summary["metrics"]["retention_d1"] = r["retention_rate"]
            elif r["day_number"] == 7:
                summary["metrics"]["retention_d7"] = r["retention_rate"]
        records_processed += len(ret)

        dev = compute_device_distribution(ch, target_date)
        _upsert_ch(ch, "agg_device_distribution", dev)
        summary["metrics"]["device_distribution"] = _rows_to_serializable(dev)
        records_processed += len(dev)

        ch.execute(
            """
            INSERT INTO cinema.user_first_event (user_id, first_date)
            SELECT user_id, min(event_date) AS first_date
            FROM cinema.movie_events
            WHERE event_type = 'VIEW_STARTED'
              AND event_date = %(d)s
              AND user_id NOT IN (SELECT user_id FROM cinema.user_first_event FINAL)
            GROUP BY user_id
            """,
            {"d": target_date},
        )

    except Exception:
        logger.exception("Aggregation failed for %s", target_date)
        raise
    finally:
        ch.disconnect()

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info(
        "=== Aggregation cycle END for %s | processed=%d records | elapsed=%.2fs ===",
        target_date,
        records_processed,
        elapsed,
    )
    summary["elapsed_seconds"] = elapsed
    summary["records_processed"] = records_processed
    return summary
