"""Trip sources that feed the ride simulator.

A :class:`TripSource` yields :class:`RawTrip` records. Each raw trip is
a real (or fake) ride-hailing trip with enough information — pickup
time, dropoff time, pickup location, dropoff location, distance, fare —
for the simulator to derive a full stream of lifecycle + GPS events.

Two implementations ship in phase 1:

- :class:`NycTlcTripSource` — real Uber/Lyft trips from NYC TLC's public
  HVFHS parquet feed. Free, no API key, canonical dataset.
- :class:`InMemoryTripSource` — a hand-rolled list, used by tests so CI
  never touches the network.
"""

from __future__ import annotations

from rides_telemetry.sources.base import RawTrip, TripSource
from rides_telemetry.sources.in_memory import InMemoryTripSource
from rides_telemetry.sources.nyc_tlc import NycTlcTripSource

__all__ = [
    "InMemoryTripSource",
    "NycTlcTripSource",
    "RawTrip",
    "TripSource",
]
