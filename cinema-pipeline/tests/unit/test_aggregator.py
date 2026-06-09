from __future__ import annotations

import importlib.util
import os
import sys
import types
from datetime import date
from unittest.mock import MagicMock

import pytest

_AGG_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../aggregation/app")
)


def _load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_AGG_DIR, filename))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_pkg = types.ModuleType("aggregation_app")
_pkg.__path__ = [_AGG_DIR]
sys.modules["aggregation_app"] = _pkg
_load_module("aggregation_app.config", "config.py")
_aggregator = _load_module("aggregation_app.aggregator", "aggregator.py")

_rows_to_serializable = _aggregator._rows_to_serializable
compute_avg_watch_time = _aggregator.compute_avg_watch_time
compute_conversion = _aggregator.compute_conversion
compute_dau = _aggregator.compute_dau
compute_device_distribution = _aggregator.compute_device_distribution
compute_retention = _aggregator.compute_retention
compute_top_movies = _aggregator.compute_top_movies


def _ch(return_value):
    ch = MagicMock()
    ch.execute.return_value = return_value
    return ch


TARGET = date(2026, 1, 15)


def test_compute_dau_normal():
    result = compute_dau(_ch([(42,)]), TARGET)
    assert result["dau"] == 42
    assert result["metric_date"] == TARGET


def test_compute_dau_empty():
    result = compute_dau(_ch([]), TARGET)
    assert result["dau"] == 0


def test_compute_avg_watch_time_normal():
    result = compute_avg_watch_time(_ch([(1800.5, 100)]), TARGET)
    assert result["avg_seconds"] == 1800.5
    assert result["total_views"] == 100
    assert result["metric_date"] == TARGET


def test_compute_avg_watch_time_empty():
    result = compute_avg_watch_time(_ch([]), TARGET)
    assert result["avg_seconds"] == 0.0
    assert result["total_views"] == 0


def test_compute_avg_watch_time_rounds():
    result = compute_avg_watch_time(_ch([(1234.5678, 50)]), TARGET)
    assert result["avg_seconds"] == round(1234.5678, 2)


def test_compute_top_movies_normal():
    rows = [("movie_001", 500), ("movie_002", 300), ("movie_003", 100)]
    result = compute_top_movies(_ch(rows), TARGET, top_n=3)
    assert len(result) == 3
    assert result[0]["movie_id"] == "movie_001"
    assert result[0]["view_count"] == 500
    assert result[0]["rank"] == 1
    assert result[1]["rank"] == 2
    assert result[2]["rank"] == 3


def test_compute_top_movies_empty():
    result = compute_top_movies(_ch([]), TARGET)
    assert result == []


def test_compute_top_movies_has_date():
    result = compute_top_movies(_ch([("m", 1)]), TARGET)
    assert result[0]["metric_date"] == TARGET


def test_compute_conversion_normal():
    result = compute_conversion(_ch([(1000, 800)]), TARGET)
    assert result["started"] == 1000
    assert result["finished"] == 800
    assert result["conversion_rate"] == 0.8
    assert result["metric_date"] == TARGET


def test_compute_conversion_zero_started():
    result = compute_conversion(_ch([(0, 0)]), TARGET)
    assert result["conversion_rate"] == 0.0


def test_compute_conversion_rounds():
    result = compute_conversion(_ch([(3, 1)]), TARGET)
    assert result["conversion_rate"] == round(1 / 3, 4)


def test_compute_device_distribution_normal():
    rows = [("DESKTOP", 500), ("MOBILE", 300), ("TV", 100)]
    result = compute_device_distribution(_ch(rows), TARGET)
    assert len(result) == 3
    assert result[0]["device_type"] == "DESKTOP"
    assert result[0]["event_count"] == 500
    assert result[0]["metric_date"] == TARGET


def test_compute_device_distribution_empty():
    result = compute_device_distribution(_ch([]), TARGET)
    assert result == []


def test_compute_retention_empty_cohort():
    result = compute_retention(_ch([]), TARGET)
    assert result == []


def test_compute_retention_with_cohort():
    ch = MagicMock()
    ch.execute.side_effect = [
        [("user_001",), ("user_002",), ("user_003",)],
        [(3,)],
        [(2,)],
        [(2,)],
        [(1,)],
        [(1,)],
        [(1,)],
        [(0,)],
        [(0,)],
    ]
    result = compute_retention(ch, TARGET)
    assert len(result) == 8
    assert result[0]["day_number"] == 0
    assert result[0]["cohort_size"] == 3
    assert result[0]["returned"] == 3
    assert result[0]["retention_rate"] == 1.0
    assert result[1]["day_number"] == 1
    assert result[1]["returned"] == 2


def test_rows_to_serializable_converts_dates():
    rows = [{"metric_date": date(2026, 1, 15), "value": 42}]
    result = _rows_to_serializable(rows)
    assert result[0]["metric_date"] == "2026-01-15"
    assert result[0]["value"] == 42


def test_rows_to_serializable_non_date_unchanged():
    rows = [{"name": "test", "count": 100, "rate": 0.95}]
    result = _rows_to_serializable(rows)
    assert result[0]["name"] == "test"
    assert result[0]["count"] == 100
    assert result[0]["rate"] == 0.95


def test_rows_to_serializable_multiple_rows():
    rows = [
        {"d": date(2026, 1, 1), "v": 1},
        {"d": date(2026, 1, 2), "v": 2},
    ]
    result = _rows_to_serializable(rows)
    assert result[0]["d"] == "2026-01-01"
    assert result[1]["d"] == "2026-01-02"
