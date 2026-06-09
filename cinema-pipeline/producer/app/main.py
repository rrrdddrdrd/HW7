from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .config import settings
from .event_generator import EventGenerator
from .kafka_producer import CinemaProducer
from .metrics import MetricsMiddleware
from .models import MovieEvent, PublishResponse

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

_producer: Optional[CinemaProducer] = None
_generator: Optional[EventGenerator] = None
_publish_count: int = 0
_error_count: int = 0
_counter_lock = threading.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _producer, _generator
    logger.info("Starting producer service …")
    _producer = CinemaProducer()

    if settings.generator_enabled:
        _generator = EventGenerator(_producer)
        _generator.start()

    yield

    logger.info("Shutting down producer service …")
    if _generator:
        _generator.stop()
    if _producer:
        _producer.close()


app = FastAPI(title="Cinema Producer", version="1.0.0", lifespan=lifespan)
app.add_middleware(MetricsMiddleware)


@app.get("/health")
async def health():
    return {"status": "ok", "generator": settings.generator_enabled}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/events", response_model=PublishResponse, status_code=status.HTTP_202_ACCEPTED)
async def publish_event(event: MovieEvent):
    global _publish_count, _error_count
    if _producer is None:
        raise HTTPException(status_code=503, detail="Producer not initialised")
    try:
        meta = _producer.publish(event)
        with _counter_lock:
            _publish_count += 1
        return PublishResponse(
            event_id=event.event_id,
            topic=settings.kafka_topic,
            partition=meta.get("partition"),
            offset=meta.get("offset"),
        )
    except Exception as exc:
        with _counter_lock:
            _error_count += 1
        logger.error("Failed to publish event %s: %s", event.event_id, exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/metrics/info")
async def metrics_info():
    with _counter_lock:
        published = _publish_count
        errors = _error_count
    return {
        "published": published,
        "errors": errors,
        "generator_enabled": settings.generator_enabled,
        "topic": settings.kafka_topic,
    }
