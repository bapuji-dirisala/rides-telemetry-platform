"""Shared pytest fixtures for the rides-telemetry test suite."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from rides_telemetry.sources.base import RawTrip
from rides_telemetry.sources.in_memory import InMemoryTripSource


@pytest.fixture
def sample_trips() -> list[RawTrip]:
    """A tiny hand-crafted set of trips covering the common shapes.

    Coordinates are inside the NYC bounding box so the invariants
    checks pass without pulling in real TLC data.
    """
    base = datetime(2024, 1, 15, 8, 0, 0, tzinfo=UTC)
    return [
        # 10-minute Manhattan hop.
        RawTrip(
            pickup_ts=base,
            dropoff_ts=base + timedelta(minutes=10),
            pickup_lat=40.7580,
            pickup_lng=-73.9855,
            dropoff_lat=40.7484,
            dropoff_lng=-73.9857,
            trip_distance_km=1.2,
            fare_amount_usd=9.50,
        ),
        # 25-minute Manhattan → Brooklyn.
        RawTrip(
            pickup_ts=base + timedelta(minutes=5),
            dropoff_ts=base + timedelta(minutes=30),
            pickup_lat=40.7831,
            pickup_lng=-73.9712,
            dropoff_lat=40.6782,
            dropoff_lng=-73.9442,
            trip_distance_km=12.4,
            fare_amount_usd=32.10,
        ),
        # 3-minute short hop.
        RawTrip(
            pickup_ts=base + timedelta(minutes=20),
            dropoff_ts=base + timedelta(minutes=23),
            pickup_lat=40.7282,
            pickup_lng=-73.7949,
            dropoff_lat=40.7300,
            dropoff_lng=-73.8000,
            trip_distance_km=0.6,
            fare_amount_usd=6.25,
        ),
    ]


@pytest.fixture
def in_memory_source(sample_trips: list[RawTrip]) -> InMemoryTripSource:
    return InMemoryTripSource(sample_trips)
