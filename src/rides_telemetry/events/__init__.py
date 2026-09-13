"""Event schemas for the rides telemetry platform.

Every event that flows through Kafka / Kinesis / Delta is defined here.
Schemas are pydantic v2 models so we get:

- runtime validation at the producer boundary (bad data never enters the pipe)
- JSON serialization for wire format (``model_dump_json``)
- a natural upgrade path to Avro / Protobuf later — pydantic models already
  expose a JSON Schema (``model_json_schema``) that can be converted
- ergonomic ``from_json`` / ``model_dump`` for downstream consumers

Two families of events live here:

- **Lifecycle events** — low volume, one per trip transition:
  :class:`TripRequested`, :class:`TripAccepted`, :class:`TripStarted`,
  :class:`TripEnded`, :class:`TripCancelled`.
- **Telemetry events** — high volume, emitted continuously while a trip
  is in progress: :class:`GpsPing`.
"""

from __future__ import annotations

from rides_telemetry.events.base import EventBase, EventType
from rides_telemetry.events.gps import GpsPing
from rides_telemetry.events.lifecycle import (
    TripAccepted,
    TripCancelled,
    TripEnded,
    TripRequested,
    TripStarted,
)

Event = TripRequested | TripAccepted | TripStarted | TripEnded | TripCancelled | GpsPing
"""Union of every event type the generator can emit."""

__all__ = [
    "Event",
    "EventBase",
    "EventType",
    "GpsPing",
    "TripAccepted",
    "TripCancelled",
    "TripEnded",
    "TripRequested",
    "TripStarted",
]
