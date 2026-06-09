from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from .config import settings
from .models import DeviceType, EventType, MovieEvent

if TYPE_CHECKING:
    from .kafka_producer import CinemaProducer

logger = logging.getLogger(__name__)

_USERS = [f"user_{i:04d}" for i in range(1, 101)]
_MOVIES = [f"movie_{i:03d}" for i in range(1, 31)]
_DEVICES = list(DeviceType)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _make_event(
    user_id: str,
    movie_id: str,
    event_type: EventType,
    session_id: str,
    progress: int,
    device: DeviceType,
) -> MovieEvent:
    return MovieEvent(
        user_id=user_id,
        movie_id=movie_id,
        event_type=event_type,
        timestamp=_now_ms(),
        device_type=device,
        session_id=session_id,
        progress_seconds=progress if event_type in {
            EventType.VIEW_STARTED,
            EventType.VIEW_FINISHED,
            EventType.VIEW_PAUSED,
            EventType.VIEW_RESUMED,
        } else None,
    )


def _generate_session(producer: "CinemaProducer") -> None:
    user_id = random.choice(_USERS)
    movie_id = random.choice(_MOVIES)
    session_id = str(uuid.uuid4())
    device = random.choice(_DEVICES)
    movie_duration = random.randint(90, 7200)
    progress = 0

    producer.publish(_make_event(user_id, movie_id, EventType.VIEW_STARTED, session_id, progress, device))
    interval = settings.generator_interval_ms / 1000

    if random.random() < 0.4:
        time.sleep(interval)
        progress = random.randint(10, movie_duration // 2)
        producer.publish(_make_event(user_id, movie_id, EventType.VIEW_PAUSED, session_id, progress, device))
        time.sleep(interval)
        producer.publish(_make_event(user_id, movie_id, EventType.VIEW_RESUMED, session_id, progress, device))

    if random.random() < 0.2:
        time.sleep(interval)
        producer.publish(_make_event(user_id, movie_id, EventType.LIKED, session_id, 0, device))

    time.sleep(interval)
    if random.random() < 0.8:
        progress = movie_duration
        producer.publish(_make_event(user_id, movie_id, EventType.VIEW_FINISHED, session_id, progress, device))

    if random.random() < 0.15:
        time.sleep(interval)
        search_user = random.choice(_USERS)
        searched_movie = random.choice(_MOVIES)
        producer.publish(_make_event(search_user, searched_movie, EventType.SEARCHED, str(uuid.uuid4()), 0, device))


class EventGenerator(threading.Thread):
    def __init__(self, producer: "CinemaProducer") -> None:
        super().__init__(daemon=True, name="EventGenerator")
        self._producer = producer
        self._stop_event = threading.Event()

    def run(self) -> None:
        logger.info("EventGenerator started (interval=%dms)", settings.generator_interval_ms)
        while not self._stop_event.is_set():
            try:
                _generate_session(self._producer)
            except Exception as exc:
                logger.exception("EventGenerator error: %s", exc)
            self._stop_event.wait(settings.generator_interval_ms / 1000)

    def stop(self) -> None:
        self._stop_event.set()
        self.join(timeout=5)
        logger.info("EventGenerator stopped")
