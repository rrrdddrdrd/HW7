from __future__ import annotations

import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import psycopg2
import pytest
import requests
from clickhouse_driver import Client as CHClient
from tenacity import retry, stop_after_attempt, wait_fixed

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8000")
AGGREGATION_URL = os.getenv("AGGREGATION_URL", "http://localhost:8001")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_DB = os.getenv("CLICKHOUSE_DB", "cinema")
CH_USER = os.getenv("CLICKHOUSE_USER", "cinema_user")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "cinema_pass")
PG_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://cinema_user:cinema_pass@postgres:5432/cinema_aggregates",
)

MAX_WAIT = 60
POLL_INTERVAL = 3


def ch_client() -> CHClient:
    return CHClient(
        host=CH_HOST,
        port=CH_PORT,
        database=CH_DB,
        user=CH_USER,
        password=CH_PASS,
    )


@retry(stop=stop_after_attempt(10), wait=wait_fixed(3))
def wait_for_producer():
    resp = requests.get(f"{PRODUCER_URL}/health", timeout=5)
    resp.raise_for_status()


@retry(stop=stop_after_attempt(10), wait=wait_fixed(3))
def wait_for_clickhouse():
    c = ch_client()
    c.execute("SELECT 1")
    c.disconnect()


@retry(stop=stop_after_attempt(10), wait=wait_fixed(3))
def wait_for_aggregation():
    resp = requests.get(f"{AGGREGATION_URL}/health", timeout=5)
    resp.raise_for_status()


class TestPipelineEndToEnd:
    _created_event_ids: list[str] = []

    @pytest.fixture(autouse=True, scope="class")
    def infrastructure_ready(self):
        wait_for_producer()
        wait_for_clickhouse()

    @pytest.fixture(autouse=True)
    def cleanup_events(self):
        self._created_event_ids = []
        yield
        if self._created_event_ids:
            ch = ch_client()
            for eid in self._created_event_ids:
                ch.execute(
                    "ALTER TABLE cinema.movie_events DELETE WHERE event_id = %(eid)s",
                    {"eid": eid},
                )
            ch.disconnect()

    def _publish_event(self, event_id: str | None = None) -> dict:
        if event_id is None:
            event_id = str(uuid.uuid4())

        payload = {
            "event_id": event_id,
            "user_id": f"test_user_{uuid.uuid4().hex[:8]}",
            "movie_id": "test_movie_001",
            "event_type": "VIEW_STARTED",
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "device_type": "DESKTOP",
            "session_id": str(uuid.uuid4()),
            "progress_seconds": 0,
        }
        resp = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
        assert resp.status_code == 202, f"Unexpected status: {resp.status_code} – {resp.text}"
        result = resp.json()
        assert result["event_id"] == event_id
        self._created_event_ids.append(event_id)
        return payload

    def _poll_clickhouse(self, event_id: str) -> list:
        ch = ch_client()
        deadline = time.time() + MAX_WAIT
        while time.time() < deadline:
            rows = ch.execute(
                "SELECT event_id, user_id, movie_id, event_type, event_ts, "
                "device_type, session_id, progress_seconds "
                "FROM cinema.movie_events "
                "WHERE event_id = %(eid)s "
                "LIMIT 1",
                {"eid": event_id},
            )
            if rows:
                ch.disconnect()
                return rows
            time.sleep(POLL_INTERVAL)
        ch.disconnect()
        return []

    def test_event_appears_in_clickhouse(self):
        payload = self._publish_event()
        rows = self._poll_clickhouse(payload["event_id"])
        assert rows, (
            f"Event {payload['event_id']} did NOT appear in ClickHouse within {MAX_WAIT}s"
        )

    def test_event_fields_match(self):
        payload = self._publish_event()
        rows = self._poll_clickhouse(payload["event_id"])
        assert rows, f"Event {payload['event_id']} not found in ClickHouse"

        row = rows[0]
        assert row[0] == payload["event_id"]
        assert row[1] == payload["user_id"]
        assert row[2] == payload["movie_id"]
        assert row[3] == payload["event_type"]
        assert row[5] == payload["device_type"]
        assert row[6] == payload["session_id"]
        assert row[7] == payload["progress_seconds"]

    def test_idempotent_duplicate_ignored(self):
        payload = self._publish_event()
        resp = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
        assert resp.status_code == 202

        time.sleep(POLL_INTERVAL * 2)

        ch = ch_client()
        rows = ch.execute(
            "SELECT count() FROM cinema.movie_events FINAL WHERE event_id = %(eid)s",
            {"eid": payload["event_id"]},
        )
        ch.disconnect()
        count = rows[0][0]
        assert count <= 1, f"Expected at most 1 row for event {payload['event_id']}, got {count}"

    def test_invalid_event_returns_422(self):
        bad_payload = {"user_id": "u1", "event_type": "NOT_A_REAL_TYPE"}
        resp = requests.post(f"{PRODUCER_URL}/events", json=bad_payload, timeout=10)
        assert resp.status_code == 422, f"Expected 422, got {resp.status_code}"

    def test_health_endpoint(self):
        resp = requests.get(f"{PRODUCER_URL}/health", timeout=5)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_producer_metrics_endpoint(self):
        resp = requests.get(f"{PRODUCER_URL}/metrics", timeout=5)
        assert resp.status_code == 200
        body = resp.text
        assert "http_requests_total" in body
        assert "http_request_duration_seconds" in body


class TestAggregationE2E:
    @pytest.fixture(autouse=True, scope="class")
    def infrastructure_ready(self):
        wait_for_producer()
        wait_for_clickhouse()
        wait_for_aggregation()

    @pytest.fixture
    def seeded_yesterday(self):
        target = datetime.now(timezone.utc).date() - timedelta(days=1)
        prefix = f"e2e_{uuid.uuid4().hex[:8]}"
        users = [f"{prefix}_u{i}" for i in range(5)]
        event_ts = datetime(target.year, target.month, target.day, 12, 0, 0)

        rows = []
        event_ids = []
        for u in users:
            eid = str(uuid.uuid4())
            event_ids.append(eid)
            rows.append({
                "event_id": eid, "user_id": u, "movie_id": "movie_seed",
                "event_type": "VIEW_STARTED", "event_ts": event_ts, "event_date": target,
                "device_type": "DESKTOP", "session_id": eid, "progress_seconds": 0,
            })
        for u in users[:3]:
            eid = str(uuid.uuid4())
            event_ids.append(eid)
            rows.append({
                "event_id": eid, "user_id": u, "movie_id": "movie_seed",
                "event_type": "VIEW_FINISHED", "event_ts": event_ts, "event_date": target,
                "device_type": "DESKTOP", "session_id": eid, "progress_seconds": 1800,
            })

        ch = ch_client()
        ch.execute(
            "INSERT INTO cinema.movie_events "
            "(event_id, user_id, movie_id, event_type, event_ts, event_date, "
            "device_type, session_id, progress_seconds) VALUES",
            rows,
        )
        ch.disconnect()

        yield {"date": target, "users": users}

        ch = ch_client()
        for eid in event_ids:
            ch.execute(
                "ALTER TABLE cinema.movie_events DELETE WHERE event_id = %(eid)s",
                {"eid": eid},
            )
        ch.execute(
            "ALTER TABLE cinema.user_first_event DELETE WHERE user_id IN %(users)s",
            {"users": tuple(users)},
        )
        ch.disconnect()

    @pytest.fixture(autouse=True)
    def cleanup_pg_aggregates(self):
        yield
        try:
            conn = psycopg2.connect(PG_DSN)
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM cinema_aggregates WHERE metric_date = CURRENT_DATE - 1"
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception:
            pass

    def test_aggregation_computes_real_metrics(self, seeded_yesterday):
        target = seeded_yesterday["date"]
        target_iso = target.isoformat()

        ch = ch_client()
        expected_dau = ch.execute(
            "SELECT uniq(user_id) FROM cinema.movie_events WHERE event_date = %(d)s",
            {"d": target},
        )[0][0]
        started_finished = ch.execute(
            "SELECT sumIf(1, event_type = 'VIEW_STARTED'), "
            "sumIf(1, event_type = 'VIEW_FINISHED') "
            "FROM cinema.movie_events "
            "WHERE event_date = %(d)s AND event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')",
            {"d": target},
        )[0]
        ch.disconnect()
        expected_started, expected_finished = started_finished[0], started_finished[1]

        assert expected_dau >= 5
        assert expected_started >= 5
        assert expected_finished >= 3

        resp = requests.post(
            f"{AGGREGATION_URL}/aggregate",
            params={"target_date": target_iso},
            timeout=60,
        )
        assert resp.status_code == 200, f"Aggregation failed: {resp.text}"
        summary = resp.json()
        assert summary.get("pg_written") is True

        metrics = summary["metrics"]
        assert metrics["dau"] == expected_dau
        assert metrics["started"] == expected_started
        assert metrics["finished"] == expected_finished
        assert metrics["avg_watch_seconds"] > 0
        assert metrics["conversion_rate"] == round(expected_finished / expected_started, 4)

        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()
        cur.execute(
            "SELECT (metric_value->>'value')::int FROM cinema_aggregates "
            "WHERE metric_date = %s AND metric_name = 'dau'",
            (target_iso,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        assert row is not None, f"No dau row in Postgres for {target_iso}"
        assert row[0] == expected_dau

    def test_aggregation_health(self):
        resp = requests.get(f"{AGGREGATION_URL}/health", timeout=5)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_aggregation_metrics_endpoint(self):
        resp = requests.get(f"{AGGREGATION_URL}/metrics", timeout=5)
        assert resp.status_code == 200
        assert "http_requests_total" in resp.text
