"""Gold layer — business-ready marts for downstream consumers.

Gold is the surface the org queries: dashboards, surge-pricing
services, fraud alerting, driver-rider matching. Every mart here is:

- **Pre-aggregated** to the exact granularity a consumer needs
  (e.g. per-borough, per-5-min-window), so serving latency is O(row)
  not O(ping).
- **Enriched at build time** with borough attribution (and, in
  phase 6, H3 hex zones), so consumers don't need geospatial code.
- **Small enough to serve directly** — kilobytes per mart, not
  gigabytes — so a REST layer or a BI tool can pull them without
  a Spark cluster.

Phase 5a ships two marts:

- :func:`stream_active_trips_mart` — rolling per-borough count of
  currently in-progress trips.
- :func:`stream_demand_mart` — 5-min tumbling window of GPS pings
  per borough (distinct trips + drivers), a proxy for demand.

Phase 5b will add fraud detection (impossible-speed pings, same
driver on two active trips).
"""

from __future__ import annotations

from rides_telemetry.gold.active_trips import stream_active_trips_mart
from rides_telemetry.gold.demand import stream_demand_mart
from rides_telemetry.gold.schemas import (
    GOLD_ACTIVE_TRIPS_SCHEMA,
    GOLD_ACTIVE_TRIPS_TABLE,
    GOLD_DEMAND_SCHEMA,
    GOLD_DEMAND_TABLE,
)
from rides_telemetry.gold.streaming import GoldStreamConfig

__all__ = [
    "GOLD_ACTIVE_TRIPS_SCHEMA",
    "GOLD_ACTIVE_TRIPS_TABLE",
    "GOLD_DEMAND_SCHEMA",
    "GOLD_DEMAND_TABLE",
    "GoldStreamConfig",
    "stream_active_trips_mart",
    "stream_demand_mart",
]
