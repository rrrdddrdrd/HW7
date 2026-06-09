from __future__ import annotations
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
import uuid
import time


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    movie_id: str
    event_type: EventType
    timestamp: int = Field(
        default_factory=lambda: int(time.time() * 1000),
        description="UTC timestamp in milliseconds",
    )
    device_type: DeviceType
    session_id: str
    progress_seconds: Optional[int] = None

    def to_avro_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "user_id": self.user_id,
            "movie_id": self.movie_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp,
            "device_type": self.device_type.value,
            "session_id": self.session_id,
            "progress_seconds": self.progress_seconds,
        }


class PublishResponse(BaseModel):
    event_id: str
    status: str = "published"
    topic: str
    partition: Optional[int] = None
    offset: Optional[int] = None
