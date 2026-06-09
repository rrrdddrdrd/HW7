from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .aggregator import run_aggregation
from .config import settings
from .metrics import AGGREGATION_DURATION, AGGREGATION_RUNS, MetricsMiddleware
from .postgres_writer import write_aggregates
from .s3_exporter import export_to_s3

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

_last_result: Optional[dict] = None
_scheduler: Optional[BackgroundScheduler] = None


def _scheduled_job():
    global _last_result
    target = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    t0 = time.perf_counter()
    try:
        summary = run_aggregation(target)
        write_aggregates(target, summary["metrics"])
        export_to_s3(target)
        summary["pg_written"] = True
        summary["s3_exported"] = True
        _last_result = summary
        AGGREGATION_RUNS.labels(status="success").inc()
        AGGREGATION_DURATION.observe(time.perf_counter() - t0)
    except Exception as exc:
        logger.error("Scheduled aggregation failed: %s", exc)
        _last_result = {"error": str(exc), "date": str(target)}
        AGGREGATION_RUNS.labels(status="failure").inc()
        AGGREGATION_DURATION.observe(time.perf_counter() - t0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    _scheduler = BackgroundScheduler()
    cron_parts = settings.aggregation_schedule.split()
    trigger = CronTrigger(
        minute=cron_parts[0],
        hour=cron_parts[1],
        day=cron_parts[2],
        month=cron_parts[3],
        day_of_week=cron_parts[4],
    )
    _scheduler.add_job(_scheduled_job, trigger, id="aggregation")
    _scheduler.start()
    logger.info("Aggregation scheduler started (%s)", settings.aggregation_schedule)
    yield
    _scheduler.shutdown(wait=False)
    logger.info("Aggregation scheduler stopped")


app = FastAPI(title="Cinema Aggregation Service", version="1.0.0", lifespan=lifespan)
app.add_middleware(MetricsMiddleware)


@app.get("/health")
async def health():
    return {"status": "ok", "schedule": settings.aggregation_schedule}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/aggregate")
async def trigger_aggregation(
    target_date: Optional[date] = Query(
        default=None,
        description="Date to aggregate (YYYY-MM-DD). Defaults to yesterday.",
    )
):
    global _last_result
    if target_date is None:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    t0 = time.perf_counter()
    try:
        summary = run_aggregation(target_date)
        write_aggregates(target_date, summary["metrics"])
        s3_key = export_to_s3(target_date)
        summary["pg_written"] = True
        summary["s3_key"] = s3_key
        _last_result = summary
        AGGREGATION_RUNS.labels(status="success").inc()
        AGGREGATION_DURATION.observe(time.perf_counter() - t0)
        return summary
    except Exception as exc:
        AGGREGATION_RUNS.labels(status="failure").inc()
        AGGREGATION_DURATION.observe(time.perf_counter() - t0)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/export")
async def trigger_export(
    target_date: Optional[date] = Query(
        default=None,
        description="Date to export (YYYY-MM-DD). Defaults to yesterday.",
    )
):
    if target_date is None:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    try:
        s3_key = export_to_s3(target_date)
        return {"status": "exported", "s3_key": s3_key, "date": str(target_date)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/aggregate/last")
async def last_result():
    if _last_result is None:
        return {"status": "no aggregation run yet"}
    return _last_result
