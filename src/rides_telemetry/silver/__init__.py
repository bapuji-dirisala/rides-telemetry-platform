"""Silver layer — cleaned, deduped, business-usable Delta tables.

Silver is where bronze's raw truth becomes trustworthy data. Every
job in this package reads a bronze Delta table and produces a silver
Delta table with strong invariants:

- **Deduplicated** by ``event_id`` using a bounded watermark so state
  stays finite.
- **Quality-filtered**: obviously wrong rows (NaN coords, impossible
  speeds, pings outside NYC) are dropped, not silently propagated.
- **Business-shaped**: for lifecycle events, the five per-trip event
  types are folded into one row per ``trip_id`` with a derived
  ``status``, so downstream marts can query "current state of trip"
  without re-implementing the fold every time.

Phase 4a ships two jobs:

- :func:`stream_gps_to_silver` — bronze GPS → silver GPS (row-level).
- :func:`stream_trips_to_silver` — bronze lifecycle → silver trip
  facts (per-trip aggregation with idempotent MERGE upsert).

The stream-stream join between silver GPS and silver trips (validating
that each ping belongs to a currently-open trip) ships in Phase 4b.
"""

from __future__ import annotations

from rides_telemetry.silver.gps import stream_gps_to_silver
from rides_telemetry.silver.schemas import (
    SILVER_GPS_SCHEMA,
    SILVER_GPS_TABLE,
    SILVER_TRIPS_SCHEMA,
    SILVER_TRIPS_TABLE,
)
from rides_telemetry.silver.streaming import SilverStreamConfig
from rides_telemetry.silver.trips import stream_trips_to_silver

__all__ = [
    "SILVER_GPS_SCHEMA",
    "SILVER_GPS_TABLE",
    "SILVER_TRIPS_SCHEMA",
    "SILVER_TRIPS_TABLE",
    "SilverStreamConfig",
    "stream_gps_to_silver",
    "stream_trips_to_silver",
]
