"""Schema-level tests for the event models."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from rides_telemetry.events import (
    EventType,
    GpsPing,
    TripAccepted,
    TripCancelled,
    TripEnded,
    TripRequested,
    TripStarted,
)


def _now() -> datetime:
    return datetime(2024, 1, 15, 8, 0, 0, tzinfo=UTC)


def test_gps_ping_json_roundtrip() -> None:
    ping = GpsPing._make(  # noqa: SLF001
        event_ts=_now(),
        trip_id=uuid4(),
        driver_id=uuid4(),
        lat=40.75,
        lng=-73.98,
        speed_kmh=42.5,
        heading_deg=180.0,
        accuracy_m=5.0,
    )
    parsed = GpsPing.model_validate_json(ping.model_dump_json())
    assert parsed == ping
    assert parsed.event_type == EventType.GPS_PING


def test_lifecycle_json_roundtrip() -> None:
    common = {
        "event_ts": _now(),
        "trip_id": uuid4(),
        "rider_id": uuid4(),
        "pickup_lat": 40.75,
        "pickup_lng": -73.98,
        "dropoff_lat": 40.68,
        "dropoff_lng": -73.94,
    }
    driver_id = uuid4()

    for event in (
        TripRequested._make(**common),  # noqa: SLF001
        TripAccepted._make(driver_id=driver_id, **common),  # noqa: SLF001
        TripStarted._make(driver_id=driver_id, **common),  # noqa: SLF001
        TripEnded._make(  # noqa: SLF001
            driver_id=driver_id,
            trip_distance_km=1.2,
            fare_amount_usd=9.5,
            **common,
        ),
        TripCancelled._make(cancelled_by="rider", **common),  # noqa: SLF001
    ):
        parsed = type(event).model_validate_json(event.model_dump_json())
        assert parsed == event


def test_gps_ping_rejects_out_of_range_lat() -> None:
    with pytest.raises(ValidationError):
        GpsPing._make(  # noqa: SLF001
            event_ts=_now(),
            trip_id=uuid4(),
            driver_id=uuid4(),
            lat=100.0,  # invalid
            lng=-73.98,
        )


def test_events_are_frozen() -> None:
    ping = GpsPing._make(  # noqa: SLF001
        event_ts=_now(),
        trip_id=uuid4(),
        driver_id=uuid4(),
        lat=40.75,
        lng=-73.98,
    )
    with pytest.raises(ValidationError):
        ping.lat = 0.0  # type: ignore[misc]
