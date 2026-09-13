"""Kafka wiring for the rides telemetry platform.

Wraps ``confluent-kafka`` with production-shaped defaults (idempotent
producer, ``acks=all``, retries, snappy compression, per-trip keying)
so downstream code can just call :meth:`EventProducer.produce` with a
pydantic event and get correct-by-default Kafka behaviour.

Public surface:

- :class:`KafkaConfig` — connection + topic mapping.
- :class:`EventProducer` — produces :class:`~rides_telemetry.events.Event`
  instances to the right topic, with the right key, with the right
  serialisation.
- :func:`topic_for` — pure function mapping an event to its topic name.
- :func:`ensure_topics` — admin helper that creates the two rides topics
  if they don't already exist.
"""

from __future__ import annotations

from rides_telemetry.kafka.config import KafkaConfig
from rides_telemetry.kafka.producer import EventProducer, ProducerStats
from rides_telemetry.kafka.topics import TOPIC_GPS, TOPIC_TRIPS, ensure_topics, topic_for

__all__ = [
    "TOPIC_GPS",
    "TOPIC_TRIPS",
    "EventProducer",
    "KafkaConfig",
    "ProducerStats",
    "ensure_topics",
    "topic_for",
]
