"""Base classes and shared enums for all telemetry events."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, ClassVar, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class EventType(StrEnum):
    """Discriminator value for every event type on the wire.

    Kept as a ``StrEnum`` so it JSON-serializes as a plain string, and so
    downstream Spark schemas can treat it as a ``StringType``.
    """

    TRIP_REQUESTED = "trip_requested"
    TRIP_ACCEPTED = "trip_accepted"
    TRIP_STARTED = "trip_started"
    TRIP_ENDED = "trip_ended"
    TRIP_CANCELLED = "trip_cancelled"
    GPS_PING = "gps_ping"


class EventBase(BaseModel):
    """Common envelope fields shared by every event.

    Design notes:

    - ``event_id`` is a v4 UUID for now. Later phases will migrate to
      UUIDv7 for time-ordered Kafka keys and Delta clustering — pydantic
      accepts either, so the schema won't need to change.
    - ``event_ts`` is the *business* timestamp (when the event happened
      in the domain), always tz-aware UTC. A separate ``ingest_ts``
      shows up in the bronze layer in phase 3 — it's not part of the
      producer contract.
    - ``event_type`` is a discriminator populated automatically by each
      subclass via ``model_post_init``. Consumers can route on it
      without inspecting the class.
    - ``model_config`` freezes instances (``frozen=True``) — events are
      immutable facts, mutating one is always a bug.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        # Serialize enums by value, datetimes as ISO 8601.
        use_enum_values=True,
    )

    # Class-level constant that each subclass overrides.
    _event_type: ClassVar[EventType]

    event_id: UUID = Field(default_factory=uuid4)
    event_ts: datetime
    event_type: EventType

    @classmethod
    def _make(cls, event_ts: datetime, **fields: Any) -> Self:
        """Internal constructor used by the generator.

        Ensures every subclass gets its ``event_type`` set correctly
        without callers having to remember to pass it. Returns ``Self``
        so subclasses keep their concrete type through this helper.
        """
        return cls(event_ts=event_ts, event_type=cls._event_type, **fields)
