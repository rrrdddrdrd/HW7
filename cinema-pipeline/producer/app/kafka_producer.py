from __future__ import annotations

import logging
import time
from typing import Optional

import requests
from confluent_kafka import Producer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from .config import settings
from .metrics import EVENTS_PUBLISHED
from .models import MovieEvent

logger = logging.getLogger(__name__)


def _fetch_schema(subject: str) -> str:
    url = f"{settings.schema_registry_url}/subjects/{subject}/versions/latest"
    for attempt in range(1, 6):
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp.json()["schema"]
        except Exception as exc:
            wait = 2 ** attempt
            logger.warning(
                "Schema Registry not ready (attempt %d): %s – retrying in %ds",
                attempt,
                exc,
                wait,
            )
            time.sleep(wait)
    raise RuntimeError(f"Cannot fetch schema for subject {subject!r} after 5 attempts")


class CinemaProducer:
    def __init__(self) -> None:
        self._sr_client = SchemaRegistryClient({"url": settings.schema_registry_url})

        schema_str = _fetch_schema(f"{settings.kafka_topic}-value")

        self._serializer = AvroSerializer(
            self._sr_client,
            schema_str,
            lambda obj, ctx: obj,
        )

        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "acks": settings.kafka_acks,
                "retries": settings.kafka_retries,
                "retry.backoff.ms": settings.kafka_retry_backoff_ms,
                "enable.idempotence": True,
                "compression.type": "snappy",
                "linger.ms": 5,
            }
        )
        logger.info("CinemaProducer initialised (topic=%s)", settings.kafka_topic)

    @staticmethod
    def _delivery_cb(err, msg):
        if err:
            logger.error("Delivery failed: %s", err)
        else:
            logger.debug(
                "Delivered event_id=%s topic=%s partition=%d offset=%d",
                msg.key().decode() if msg.key() else "?",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def publish(self, event: MovieEvent) -> dict:
        avro_dict = event.to_avro_dict()

        value_bytes = self._serializer(
            avro_dict,
            SerializationContext(settings.kafka_topic, MessageField.VALUE),
        )

        key_bytes = event.user_id.encode()

        result: dict = {}

        def _cb(err, msg):
            if err:
                result["error"] = str(err)
            else:
                result["partition"] = msg.partition()
                result["offset"] = msg.offset()
            self._delivery_cb(err, msg)

        self._producer.produce(
            topic=settings.kafka_topic,
            key=key_bytes,
            value=value_bytes,
            on_delivery=_cb,
        )

        self._producer.flush(timeout=10)

        if "error" in result:
            raise RuntimeError(f"Kafka delivery error: {result['error']}")

        EVENTS_PUBLISHED.labels(event_type=event.event_type.value).inc()

        logger.info(
            "Published event_id=%s event_type=%s timestamp=%d",
            event.event_id,
            event.event_type.value,
            event.timestamp,
        )
        return result

    def close(self) -> None:
        self._producer.flush()
        logger.info("CinemaProducer closed")
