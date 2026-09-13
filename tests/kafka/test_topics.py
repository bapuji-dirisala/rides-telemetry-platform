"""Topic-routing tests."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from rides_telemetry.events import GpsPing, TripEnded, TripRequested, TripStarted
from rides_telemetry.kafka import KafkaConfig, topic_for


def _ts() -> datetime:
    return datetime(2024, 1, 15, 8, 0, tzinfo=UTC)


def _common() -> dict:
    return {
        "trip_id": uuid4(),
        "rider_id": uuid4(),
        "pickup_lat": 40.75,
        "pickup_lng": -73.98,
        "dropoff_lat": 40.68,
        "dropoff_lng": -73.94,
    }


def test_gps_ping_routes_to_gps_topic() -> None:
    ping = GpsPing._make(  # noqa: SLF001
        event_ts=_ts(),
        trip_id=uuid4(),
        driver_id=uuid4(),
        lat=40.75,
        lng=-73.98,
    )
    assert topic_for(ping) == "rides.gps.v1"


def test_lifecycle_events_route_to_trips_topic() -> None:
    driver_id = uuid4()
    common = _common()
    for ev in (
        TripRequested._make(event_ts=_ts(), **common),  # noqa: SLF001
        TripStarted._make(event_ts=_ts(), driver_id=driver_id, **common),  # noqa: SLF001
        TripEnded._make(  # noqa: SLF001
            event_ts=_ts(),
            driver_id=driver_id,
            trip_distance_km=1.2,
            fare_amount_usd=9.5,
            **common,
        ),
    ):
        assert topic_for(ev) == "rides.trips.v1"


def test_custom_config_topic_names_are_respected() -> None:
    cfg = KafkaConfig(topic_trips="alt.trips", topic_gps="alt.gps")
    ping = GpsPing._make(  # noqa: SLF001
        event_ts=_ts(),
        trip_id=uuid4(),
        driver_id=uuid4(),
        lat=40.75,
        lng=-73.98,
    )
    assert topic_for(ping, cfg) == "alt.gps"
