from __future__ import annotations

import json
import os
import sys
import time

import requests

PROMETHEUS_URL = "http://localhost:9095"
OUTPUT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "metrics_check_result.json"
)
WARMUP_SECONDS = 30


def query(promql: str) -> float | None:
    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": promql},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        result = data["data"]["result"]
        if result:
            return float(result[0]["value"][1])
        return None
    except Exception as exc:
        print(f"Query failed [{promql!r}]: {exc}", file=sys.stderr)
        return None


def main() -> int:
    print(f"Waiting {WARMUP_SECONDS}s for Prometheus to collect post-load data …")
    time.sleep(WARMUP_SECONDS)

    checks: dict = {}

    p95 = query(
        "histogram_quantile(0.95,"
        " sum(rate(http_request_duration_seconds_bucket{job=\"producer\"}[5m])) by (le)"
        ")"
    )
    checks["sli1_p95_latency_ms"] = round((p95 or 0.0) * 1000, 2)
    checks["sli1_p95_latency_ok"] = p95 is None or p95 < 0.5

    total = query('sum(rate(http_requests_total{job="producer"}[5m]))')
    errors = query('sum(rate(http_request_errors_total{job="producer"}[5m]))')
    if total and total > 0:
        error_rate = (errors or 0.0) / total
    else:
        error_rate = 0.0
    checks["sli2_error_rate_pct"] = round(error_rate * 100, 4)
    checks["sli2_error_rate_ok"] = error_rate < 0.01

    consumer_lag = query(
        'sum(kafka_consumergroup_lag{consumergroup="clickhouse-raw-consumer"})'
    )
    checks["sli3_kafka_lag"] = int(consumer_lag or 0)
    checks["sli3_kafka_lag_ok"] = consumer_lag is None or consumer_lag < 10000

    producer_up = query('up{job="producer"}')
    checks["producer_up"] = producer_up == 1.0 if producer_up is not None else False
    checks["producer_up_ok"] = checks["producer_up"]

    with open(OUTPUT_FILE, "w") as fh:
        json.dump(checks, fh, indent=2)

    print(json.dumps(checks, indent=2))

    failed = [k for k, v in checks.items() if k.endswith("_ok") and not v]
    if failed:
        print(f"\nFAILED checks: {failed}", file=sys.stderr)
        return 1

    print("\nAll SLI checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
