"""Trip lifecycle events.

State machine:

.. code-block:: text

    TripRequested ─► TripAccepted ─► TripStarted ─► TripEnded
                 └─► TripCancelled     ▲              │
                                       └── or ────────► TripCancelled

Every lifecycle event carries the same ``trip_id`` so downstream stream-
stream joins (phase 4) can stitch them back into a single trip fact.
"""

from __future__ import annotations

from typing import ClassVar
from uuid import UUID

from pydantic import Field

from rides_telemetry.events.base import EventBase, EventType


class _TripEvent(EventBase):
    """Common fields for every lifecycle event."""

    trip_id: UUID = Field(description="Stable trip identifier used as the Kafka partition key.")
    rider_id: UUID
    driver_id: UUID | None = Field(
        default=None,
        description="Null until a driver is matched (TripAccepted onwards).",
    )
    pickup_lat: float
    pickup_lng: float
    dropoff_lat: float
    dropoff_lng: float


class TripRequested(_TripEvent):
    """Rider tapped 'request' — no driver assigned yet."""

    _event_type: ClassVar[EventType] = EventType.TRIP_REQUESTED

    # Explicit None for driver_id — it's structurally impossible to have
    # a driver at request time.
    driver_id: None = None


class TripAccepted(_TripEvent):
    """A driver accepted the trip. ``driver_id`` is now populated."""

    _event_type: ClassVar[EventType] = EventType.TRIP_ACCEPTED

    driver_id: UUID  # type: ignore[assignment]  # narrowed from Optional


class TripStarted(_TripEvent):
    """Driver marked the trip as started (rider is in the car)."""

    _event_type: ClassVar[EventType] = EventType.TRIP_STARTED

    driver_id: UUID  # type: ignore[assignment]


class TripEnded(_TripEvent):
    """Trip completed at the dropoff location."""

    _event_type: ClassVar[EventType] = EventType.TRIP_ENDED

    driver_id: UUID  # type: ignore[assignment]
    trip_distance_km: float = Field(ge=0)
    fare_amount_usd: float = Field(ge=0)


class TripCancelled(_TripEvent):
    """Trip cancelled by either party before completion."""

    _event_type: ClassVar[EventType] = EventType.TRIP_CANCELLED

    cancelled_by: str = Field(description="'rider' | 'driver' | 'system'")
