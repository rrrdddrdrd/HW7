from __future__ import annotations

import importlib.util
import os
import sys
import uuid

import pytest
from pydantic import ValidationError

_MODELS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../producer/app/models.py")
)
_spec = importlib.util.spec_from_file_location("producer_app_models", _MODELS_PATH)
_models = importlib.util.module_from_spec(_spec)
sys.modules["producer_app_models"] = _models
_spec.loader.exec_module(_models)

DeviceType = _models.DeviceType
EventType = _models.EventType
MovieEvent = _models.MovieEvent
PublishResponse = _models.PublishResponse


def test_event_type_values():
    assert EventType.VIEW_STARTED == "VIEW_STARTED"
    assert EventType.VIEW_FINISHED == "VIEW_FINISHED"
    assert EventType.VIEW_PAUSED == "VIEW_PAUSED"
    assert EventType.VIEW_RESUMED == "VIEW_RESUMED"
    assert EventType.LIKED == "LIKED"
    assert EventType.SEARCHED == "SEARCHED"


def test_device_type_values():
    assert DeviceType.MOBILE == "MOBILE"
    assert DeviceType.DESKTOP == "DESKTOP"
    assert DeviceType.TV == "TV"
    assert DeviceType.TABLET == "TABLET"


def test_movie_event_valid():
    event = MovieEvent(
        user_id="user_0001",
        movie_id="movie_001",
        event_type=EventType.VIEW_STARTED,
        device_type=DeviceType.DESKTOP,
        session_id=str(uuid.uuid4()),
        progress_seconds=0,
    )
    assert event.user_id == "user_0001"
    assert event.movie_id == "movie_001"
    assert event.event_type == EventType.VIEW_STARTED
    assert event.device_type == DeviceType.DESKTOP
    assert event.progress_seconds == 0


def test_movie_event_defaults():
    event = MovieEvent(
        user_id="user_0001",
        movie_id="movie_001",
        event_type=EventType.VIEW_STARTED,
        device_type=DeviceType.DESKTOP,
        session_id=str(uuid.uuid4()),
    )
    assert uuid.UUID(event.event_id)
    assert event.timestamp > 0
    assert event.progress_seconds is None


def test_movie_event_invalid_event_type():
    with pytest.raises(ValidationError):
        MovieEvent(
            user_id="user_0001",
            movie_id="movie_001",
            event_type="INVALID_TYPE",
            device_type=DeviceType.DESKTOP,
            session_id=str(uuid.uuid4()),
        )


def test_movie_event_invalid_device_type():
    with pytest.raises(ValidationError):
        MovieEvent(
            user_id="user_0001",
            movie_id="movie_001",
            event_type=EventType.VIEW_STARTED,
            device_type="FRIDGE",
            session_id=str(uuid.uuid4()),
        )


def test_movie_event_missing_required_fields():
    with pytest.raises(ValidationError):
        MovieEvent(user_id="user_0001")


def test_to_avro_dict_structure():
    session_id = str(uuid.uuid4())
    event = MovieEvent(
        user_id="user_0001",
        movie_id="movie_001",
        event_type=EventType.VIEW_FINISHED,
        device_type=DeviceType.MOBILE,
        session_id=session_id,
        progress_seconds=3600,
    )
    d = event.to_avro_dict()
    expected_keys = {
        "event_id", "user_id", "movie_id", "event_type",
        "timestamp", "device_type", "session_id", "progress_seconds",
    }
    assert set(d.keys()) == expected_keys


def test_to_avro_dict_values():
    session_id = str(uuid.uuid4())
    event = MovieEvent(
        user_id="user_0001",
        movie_id="movie_001",
        event_type=EventType.VIEW_FINISHED,
        device_type=DeviceType.MOBILE,
        session_id=session_id,
        progress_seconds=3600,
    )
    d = event.to_avro_dict()
    assert d["user_id"] == "user_0001"
    assert d["movie_id"] == "movie_001"
    assert d["event_type"] == "VIEW_FINISHED"
    assert d["device_type"] == "MOBILE"
    assert d["session_id"] == session_id
    assert d["progress_seconds"] == 3600
    assert isinstance(d["event_id"], str)
    assert isinstance(d["timestamp"], int)


def test_to_avro_dict_event_type_is_string():
    event = MovieEvent(
        user_id="u",
        movie_id="m",
        event_type=EventType.LIKED,
        device_type=DeviceType.TV,
        session_id="s",
    )
    d = event.to_avro_dict()
    assert d["event_type"] == "LIKED"
    assert isinstance(d["event_type"], str)


def test_to_avro_dict_null_progress():
    event = MovieEvent(
        user_id="u",
        movie_id="m",
        event_type=EventType.SEARCHED,
        device_type=DeviceType.DESKTOP,
        session_id="s",
    )
    d = event.to_avro_dict()
    assert d["progress_seconds"] is None


def test_publish_response_defaults():
    resp = PublishResponse(event_id="abc", topic="movie-events")
    assert resp.status == "published"
    assert resp.partition is None
    assert resp.offset is None


def test_publish_response_full():
    resp = PublishResponse(event_id="abc", topic="movie-events", partition=1, offset=42)
    assert resp.event_id == "abc"
    assert resp.topic == "movie-events"
    assert resp.partition == 1
    assert resp.offset == 42


def test_event_id_is_unique():
    e1 = MovieEvent(
        user_id="u", movie_id="m",
        event_type=EventType.VIEW_STARTED,
        device_type=DeviceType.DESKTOP, session_id="s",
    )
    e2 = MovieEvent(
        user_id="u", movie_id="m",
        event_type=EventType.VIEW_STARTED,
        device_type=DeviceType.DESKTOP, session_id="s",
    )
    assert e1.event_id != e2.event_id
