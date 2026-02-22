"""Trip-source protocol and the raw-trip record."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RawTrip:
    """A single ride-hailing trip as it exists in the source dataset.

    Intentionally minimal — the simulator derives every lifecycle event
    from just these fields. Coordinates are already resolved to lat/lng
    (the source is responsible for any zone → centroid lookup) so the
    simulator stays geography-agnostic and testable.
    """

    pickup_ts: datetime
    dropoff_ts: datetime
    pickup_lat: float
    pickup_lng: float
    dropoff_lat: float
    dropoff_lng: float
    trip_distance_km: float
    fare_amount_usd: float


class TripSource(Protocol):
    """Anything that can yield :class:`RawTrip` records.

    Implementations may be lazy (streaming from a parquet file) or
    eager (an in-memory list). The simulator only requires iteration.
    """

    def iter_trips(self, limit: int | None = None) -> Iterable[RawTrip]:
        """Yield up to ``limit`` trips.

        Order is source-defined. Real Kafka topics partitioned by
        ``trip_id`` don't provide global time ordering either, so the
        simulator downstream doesn't rely on it.
        """
        ...
