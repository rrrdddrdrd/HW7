from __future__ import annotations

import time

from prometheus_client import Counter, Histogram
from starlette.middleware.base import BaseHTTPMiddleware

REQUESTS = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

ERRORS = Counter(
    "http_request_errors_total",
    "Total HTTP request errors",
    ["method", "endpoint", "error_type"],
)

DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

EVENTS_PUBLISHED = Counter(
    "cinema_events_published_total",
    "Total events published to Kafka",
    ["event_type"],
)

_EXCLUDED_PATHS = {"/metrics", "/health", "/docs", "/openapi.json"}


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if request.url.path in _EXCLUDED_PATHS:
            return await call_next(request)

        method = request.method
        endpoint = request.url.path
        start = time.perf_counter()
        status = "500"

        try:
            response = await call_next(request)
            status = str(response.status_code)
            if response.status_code >= 400:
                error_type = "client_error" if response.status_code < 500 else "server_error"
                ERRORS.labels(method=method, endpoint=endpoint, error_type=error_type).inc()
            return response
        except Exception as exc:
            ERRORS.labels(
                method=method, endpoint=endpoint, error_type=type(exc).__name__
            ).inc()
            raise
        finally:
            REQUESTS.labels(method=method, endpoint=endpoint, status=status).inc()
            DURATION.labels(method=method, endpoint=endpoint).observe(
                time.perf_counter() - start
            )
