"""High-volume GPS ping event.

Every active trip emits one :class:`GpsPing` per second by default.
This is the event that drives the "millions per minute" throughput
number in the project pitch, and it's what stresses partitioning and
watermarking in later phases.

Design choices:

- **Partition key = ``trip_id``**. Ordering *within* a trip matters
  (a stale ping would break map-matching); ordering *across* trips
  does not. Keying by trip_id gives per-trip ordering guarantees on
  Kafka.
- **``speed_kmh`` and ``heading_deg`` are optional.** The generator
  populates them; a real device might not. Making them optional keeps
  the schema tolerant of upstream reality.
- **Coordinates as ``float``, not ``Decimal``.** GPS is inherently
  imprecise (~5m accuracy at best) and Delta/Parquet handle floats
  natively. Fixed-point precision would be over-engineering here.
"""

from __future__ import annotations

from typing import ClassVar
from uuid import UUID

from pydantic import Field

from rides_telemetry.events.base import EventBase, EventType


class GpsPing(EventBase):
    """A single GPS reading from a vehicle."""

    _event_type: ClassVar[EventType] = EventType.GPS_PING

    trip_id: UUID = Field(description="Trip this ping belongs to. Kafka partition key.")
    driver_id: UUID
    lat: float = Field(ge=-90.0, le=90.0)
    lng: float = Field(ge=-180.0, le=180.0)
    speed_kmh: float | None = Field(default=None, ge=0.0, le=300.0)
    heading_deg: float | None = Field(default=None, ge=0.0, lt=360.0)
    accuracy_m: float | None = Field(default=None, ge=0.0)
