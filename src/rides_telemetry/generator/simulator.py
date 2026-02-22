"""Ride event simulator.

Given a stream of real (or fake) :class:`~rides_telemetry.sources.RawTrip`
records, produces a fully-populated stream of lifecycle and GPS events
that looks like what a real ride-hailing platform would put on Kafka.

Design principles:

1. **Deterministic given a seed.** Fixed input + fixed seed always
   produces the same output. This is a hard requirement — every
   downstream test (silver dedup, gold aggregation) depends on it.

2. **No wall-clock dependency.** Event timestamps come from the source
   trip's ``pickup_ts`` / ``dropoff_ts``, not ``datetime.now()``. This
   keeps output reproducible and lets us replay historical months at
   arbitrary speed in later phases.

3. **Stream-shaped, not batch-shaped.** Events are emitted lazily as
   an iterator so a consumer can pipe them straight into Kafka without
   materializing everything in memory. Trips can be arbitrarily large
   without blowing up the process.

4. **Time-interleaved GPS + lifecycle.** Events are yielded in strict
   ``event_ts`` order so downstream watermark logic in phase 4 has
   monotonically-increasing timestamps to work with. This matches how
   a real Kafka topic behaves when consumers key by ``trip_id``.

State machine per trip:

.. code-block:: text

    request_ts    accept_ts     start_ts                       end_ts
       │             │             │                              │
       ▼             ▼             ▼                              ▼
    Requested ─► Accepted ─► Started ─► ping ─► ping ─► … ─► Ended
                                        │        │
                                        └── 1 Hz cadence by default
"""

from __future__ import annotations

import math
import random
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from rides_telemetry.events import (
    Event,
    GpsPing,
    TripAccepted,
    TripEnded,
    TripRequested,
    TripStarted,
)
from rides_telemetry.sources.base import RawTrip, TripSource


@dataclass(frozen=True, slots=True)
class SimulatorConfig:
    """Knobs for :class:`RideSimulator`."""

    seed: int = 0
    """Master seed. Everything derived from :mod:`random` uses this."""

    ping_interval_s: float = 1.0
    """Seconds between GPS pings for an active trip. 1 Hz by default."""

    request_offset_range_s: tuple[float, float] = (30.0, 120.0)
    """How far before ``pickup_ts`` the trip was requested (uniform sample)."""

    accept_offset_range_s: tuple[float, float] = (5.0, 30.0)
    """Seconds between request and driver acceptance (uniform sample)."""

    speed_jitter_kmh: float = 5.0
    """Random noise added to the reported speed on each GPS ping."""

    coord_jitter_deg: float = 1e-4
    """Per-ping lat/lng jitter (~11 m). Simulates GPS noise on top of the interpolation path."""

    max_pings_per_trip: int = 60 * 60
    """Safety cap so a mis-parsed 40-hour trip can't emit 144 000 pings."""


class RideSimulator:
    """Turns raw trips into an ordered stream of telemetry events."""

    def __init__(
        self,
        source: TripSource,
        config: SimulatorConfig | None = None,
    ) -> None:
        self.source = source
        self.config = config or SimulatorConfig()
        # Master RNG. All per-trip RNGs are derived from this so a single
        # seed reproduces everything.
        self._rng = random.Random(self.config.seed)

    # ---- public ---------------------------------------------------------

    def iter_events(self, max_trips: int | None = None) -> Iterator[Event]:
        """Yield events one trip at a time.

        Within a trip, events come out in strict ``event_ts`` order
        (``TripRequested`` → ``TripAccepted`` → ``TripStarted`` → GPS
        pings → ``TripEnded``). Across trips the stream reflects the
        source's iteration order — the same non-strict ordering a real
        Kafka topic partitioned by ``trip_id`` would exhibit.
        Global time-ordering is a consumer-side concern and lands in
        phase 4 via Structured Streaming watermarks.
        """
        for trip in self.source.iter_trips(limit=max_trips):
            yield from self._events_for_trip(trip)

    # ---- internals ------------------------------------------------------

    def _events_for_trip(self, trip: RawTrip) -> Iterable[Event]:
        # Per-trip RNG derived from master RNG so the trip's events are
        # deterministic even if trips are consumed in parallel later.
        trip_seed = self._rng.getrandbits(64)
        trip_rng = random.Random(trip_seed)

        trip_id = _uuid_from_rng(trip_rng)
        rider_id = _uuid_from_rng(trip_rng)
        driver_id = _uuid_from_rng(trip_rng)

        request_offset = trip_rng.uniform(*self.config.request_offset_range_s)
        accept_offset = trip_rng.uniform(*self.config.accept_offset_range_s)

        request_ts = trip.pickup_ts - timedelta(seconds=request_offset)
        accept_ts = request_ts + timedelta(seconds=accept_offset)
        start_ts = trip.pickup_ts
        end_ts = trip.dropoff_ts

        # ---- lifecycle events -------------------------------------------
        common = {
            "trip_id": trip_id,
            "rider_id": rider_id,
            "pickup_lat": trip.pickup_lat,
            "pickup_lng": trip.pickup_lng,
            "dropoff_lat": trip.dropoff_lat,
            "dropoff_lng": trip.dropoff_lng,
        }

        # ``event_id`` on each event is drawn from the trip-scoped RNG so
        # a fixed seed reproduces every UUID exactly — critical for
        # snapshot / replay tests downstream.
        yield TripRequested._make(  # noqa: SLF001
            event_ts=request_ts, event_id=_uuid_from_rng(trip_rng), **common
        )
        yield TripAccepted._make(  # noqa: SLF001
            event_ts=accept_ts,
            event_id=_uuid_from_rng(trip_rng),
            driver_id=driver_id,
            **common,
        )
        yield TripStarted._make(  # noqa: SLF001
            event_ts=start_ts,
            event_id=_uuid_from_rng(trip_rng),
            driver_id=driver_id,
            **common,
        )

        # ---- GPS pings --------------------------------------------------
        yield from self._pings_for_trip(
            trip=trip,
            trip_id=trip_id,
            driver_id=driver_id,
            start_ts=start_ts,
            end_ts=end_ts,
            trip_rng=trip_rng,
        )

        yield TripEnded._make(  # noqa: SLF001
            event_ts=end_ts,
            event_id=_uuid_from_rng(trip_rng),
            driver_id=driver_id,
            trip_distance_km=trip.trip_distance_km,
            fare_amount_usd=trip.fare_amount_usd,
            **common,
        )

    def _pings_for_trip(
        self,
        *,
        trip: RawTrip,
        trip_id: UUID,
        driver_id: UUID,
        start_ts: datetime,
        end_ts: datetime,
        trip_rng: random.Random,
    ) -> Iterable[GpsPing]:
        duration_s = (end_ts - start_ts).total_seconds()
        n_pings = min(
            self.config.max_pings_per_trip,
            max(1, int(duration_s // self.config.ping_interval_s)),
        )

        # Straight-line average speed for the trip. Reasonable proxy in
        # the absence of real routing — phase 6 can swap this for OSRM.
        avg_speed_kmh = trip.trip_distance_km / (duration_s / 3600.0) if duration_s > 0 else 0.0

        for i in range(n_pings):
            fraction = i / n_pings if n_pings > 0 else 0.0
            ts = start_ts + timedelta(seconds=i * self.config.ping_interval_s)
            lat = _lerp(trip.pickup_lat, trip.dropoff_lat, fraction)
            lng = _lerp(trip.pickup_lng, trip.dropoff_lng, fraction)
            # Add GPS noise on top of the interpolated path.
            lat += trip_rng.uniform(-self.config.coord_jitter_deg, self.config.coord_jitter_deg)
            lng += trip_rng.uniform(-self.config.coord_jitter_deg, self.config.coord_jitter_deg)
            speed_kmh = max(
                0.0,
                avg_speed_kmh
                + trip_rng.uniform(-self.config.speed_jitter_kmh, self.config.speed_jitter_kmh),
            )
            heading = _bearing_deg(
                trip.pickup_lat, trip.pickup_lng, trip.dropoff_lat, trip.dropoff_lng
            )
            yield GpsPing._make(  # noqa: SLF001
                event_ts=ts,
                event_id=_uuid_from_rng(trip_rng),
                trip_id=trip_id,
                driver_id=driver_id,
                lat=lat,
                lng=lng,
                speed_kmh=round(speed_kmh, 2),
                heading_deg=round(heading, 2),
                accuracy_m=round(trip_rng.uniform(3.0, 12.0), 2),
            )


# ---- small math helpers -------------------------------------------------


def _lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def _bearing_deg(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Initial great-circle bearing from point 1 to point 2, in degrees."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dl = math.radians(lng2 - lng1)
    y = math.sin(dl) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dl)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def _uuid_from_rng(rng: random.Random) -> UUID:
    """Draw a deterministic UUID from a seeded RNG."""
    return UUID(int=rng.getrandbits(128))
