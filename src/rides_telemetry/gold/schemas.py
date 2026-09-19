"""Schemas + tunable windows for the gold layer.

Every mart is defined by a schema here and a streaming job in its
own module. Adding a new mart is: (1) a schema in this file, (2) a
streaming function, (3) tests, (4) a CLI switch — nothing more.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# -------------------------------------------------------------------------
# Table names
# -------------------------------------------------------------------------

GOLD_ACTIVE_TRIPS_TABLE = "gold_active_trips_now"
GOLD_DEMAND_TABLE = "gold_demand_by_borough_5min"
GOLD_FRAUD_TELEPORT_TABLE = "gold_fraud_teleport"
GOLD_FRAUD_DUAL_TRIP_TABLE = "gold_fraud_dual_trip"
GOLD_ACTIVE_TRIPS_H3_TABLE = "gold_active_trips_h3_now"
GOLD_DEMAND_H3_TABLE = "gold_demand_by_h3_5min"


# -------------------------------------------------------------------------
# Tunable window sizes.
# -------------------------------------------------------------------------

ACTIVE_TRIPS_WINDOW: str = "1 minute"
"""Snapshot cadence for gold_active_trips_now.

Each row is "as of the end of a 1-minute window, this many trips
were seen as IN_PROGRESS in this borough". Coarser windows make the
mart cheaper but slow the response of downstream surge pricing.
1 min is a reasonable compromise for a demo dataset."""

DEMAND_WINDOW: str = "5 minutes"
"""Tumbling window for gold_demand_by_borough_5min.

Long enough to smooth out per-ping jitter, short enough to react to
demand shifts. Standard for a first-pass surge signal."""

DEMAND_WATERMARK: str = "10 minutes"
"""Late-arrival tolerance for the demand aggregation.

Matches silver_rides_gps' watermark — a ping that survived silver's
dedup+quality filter is still eligible for demand aggregation up to
10 min after its event_ts."""

H3_RESOLUTION: int = 8
"""Default H3 hex resolution for Phase 6 gold marts.

Res 8 gives ~461 m hex edges and ~0.74 km² per cell — small enough
to resolve individual streets in Manhattan, large enough that
borough-scale queries hit thousands (not millions) of hexes.

Reference:

==========  =============  =============
Resolution  Edge length    Cell area
==========  =============  =============
7           1.2 km         5.2 km²
8           461 m          0.74 km²   ← default
9           174 m          0.11 km²
10          65 m           0.015 km²
==========  =============  =============

Bumping this to res 9 or 10 makes surge / matching decisions more
precise but multiplies row counts in gold — a res-9 mart of the
same window/borough footprint has ~7× more rows than res 8. The
constant lives here so a reviewer sees the trade-off in one place."""


# -------------------------------------------------------------------------
# gold_active_trips_now — one row per (borough, snapshot_window) with
# the count of currently IN_PROGRESS trips seen inside the window.
# Written via foreachBatch + MERGE so a late-arriving ping updates
# the appropriate historical bucket idempotently.
# -------------------------------------------------------------------------

GOLD_ACTIVE_TRIPS_SCHEMA: StructType = StructType(
    [
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("borough", StringType(), nullable=False),
        StructField("active_trips", IntegerType(), nullable=False),
        StructField("gold_processed_ts", TimestampType(), nullable=False),
    ]
)


# -------------------------------------------------------------------------
# gold_demand_by_borough_5min — one row per (borough, 5-min tumbling
# window) with counts of distinct pings, trips, and drivers. Distinct
# trips is the primary demand signal; distinct drivers is the supply
# signal. Ratio (trips / drivers) is a coarse surge input.
# -------------------------------------------------------------------------

GOLD_DEMAND_SCHEMA: StructType = StructType(
    [
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("borough", StringType(), nullable=False),
        StructField("ping_count", LongType(), nullable=False),
        StructField("distinct_trips", LongType(), nullable=False),
        StructField("distinct_drivers", LongType(), nullable=False),
        StructField("gold_processed_ts", TimestampType(), nullable=False),
    ]
)


# -------------------------------------------------------------------------
# Fraud detection tunables. See ``docs/phase-05b-fraud-marts.md`` for the
# reasoning behind each threshold.
# -------------------------------------------------------------------------

MAX_PLAUSIBLE_TELEPORT_KMH: float = 200.0
"""Any implied speed between two consecutive pings above this is a
teleport candidate.

Matches the single-ping speed threshold in
:data:`~rides_telemetry.silver.schemas.MAX_PLAUSIBLE_SPEED_KMH`, but
applies to *movement between pings* rather than a single reported
value. Even highway driving in NYC caps out well below 200 km/h; a
higher implied speed almost always means either a GPS spoof or a
driver swapping devices mid-trip."""

MIN_TELEPORT_TIME_DELTA_SECONDS: float = 1.0
"""Ignore ping pairs closer together than this.

Set at the realistic minimum device ping cadence (1 s). Sub-second
gaps almost always mean the device re-emitted the same GPS fix,
so the derived "speed" is meaningless.

Note: this is deliberately *not* set higher (e.g. 5 s) because a
shorter time delta actually makes a large displacement *more*
implausible — a teleport-scale jump between two pings 1 s apart is
one of the strongest fraud signals we can catch. The jitter class
is handled by :data:`MIN_TELEPORT_DISTANCE_KM`, not by this guard."""

MIN_TELEPORT_DISTANCE_KM: float = 0.5
"""Ignore ping pairs closer together than this (500 m).

Two pings that are essentially co-located ("driver stopped at a
light and GPS drifted 100 m") aren't fraud, no matter what the
implied speed is. Requiring a real distance jump removes the
stationary-jitter false-positive class.

This is the primary jitter defense; the time-delta guard is a
secondary sanity check against duplicated GPS emissions."""

FRAUD_WINDOW: str = "5 minutes"
"""Tumbling window for the dual-trip fraud mart.

5 minutes matches the demand mart window so operators can join
fraud alerts against demand context (was this driver a large share
of a borough's supply?). Long enough to catch a driver actively
juggling two trips; short enough that a legitimate back-to-back
handoff (trip 1 ends, trip 2 starts 30 s later) doesn't trigger."""

FRAUD_WATERMARK: str = "10 minutes"
"""Late-arrival tolerance for fraud aggregations.

Same as :data:`~rides_telemetry.silver.schemas.GPS_WATERMARK` — a
ping that survived silver is eligible for fraud analysis up to
10 min after its event_ts."""


# -------------------------------------------------------------------------
# gold_fraud_teleport — one row per suspicious ping pair.
#
# Non-aggregated: every flagged (prev_ping → curr_ping) pair is
# emitted as its own row so operators can pivot on driver, trip, or
# time. Downstream summarisation (e.g. "top 10 offending drivers this
# hour") is a cheap SQL rollup.
# -------------------------------------------------------------------------

GOLD_FRAUD_TELEPORT_SCHEMA: StructType = StructType(
    [
        # Identity of the flagged ping (the "current" one in the pair)
        StructField("event_id", StringType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("driver_id", StringType(), nullable=False),
        StructField("trip_id", StringType(), nullable=False),
        # Previous ping in this driver's stream
        StructField("prev_event_id", StringType(), nullable=False),
        StructField("prev_event_ts", TimestampType(), nullable=False),
        StructField("prev_trip_id", StringType(), nullable=False),
        # Geometry
        StructField("lat", DoubleType(), nullable=False),
        StructField("lng", DoubleType(), nullable=False),
        StructField("prev_lat", DoubleType(), nullable=False),
        StructField("prev_lng", DoubleType(), nullable=False),
        # Derived signal
        StructField("distance_km", DoubleType(), nullable=False),
        StructField("time_delta_seconds", DoubleType(), nullable=False),
        StructField("implied_speed_kmh", DoubleType(), nullable=False),
        # Ops
        StructField("flagged_ts", TimestampType(), nullable=False),
    ]
)


# -------------------------------------------------------------------------
# gold_fraud_dual_trip — one row per (driver_id, window) where a
# driver was seen on 2+ distinct trip_ids inside the same 5-min
# tumbling window. Includes the sample trip IDs so an operator can
# jump straight to the offending trips without a follow-up query.
# -------------------------------------------------------------------------

GOLD_FRAUD_DUAL_TRIP_SCHEMA: StructType = StructType(
    [
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("driver_id", StringType(), nullable=False),
        StructField("concurrent_trip_count", IntegerType(), nullable=False),
        # Bounded sample — one operator glance shouldn't need to
        # follow up with a "which trips?" query. Capped at 5 so the
        # cell stays readable in a BI tool.
        StructField(
            "sample_trip_ids",
            ArrayType(StringType(), containsNull=False),
            nullable=False,
        ),
        StructField("flagged_ts", TimestampType(), nullable=False),
    ]
)


# -------------------------------------------------------------------------
# Phase 6 — H3 hex-zone marts.
#
# Same aggregation logic as the Phase 5a borough marts, but the
# geospatial dimension is an H3 res-8 cell instead of a nearest-
# centroid borough. Every hex is entirely within one borough at
# this resolution, so a per-borough rollup is a cheap SQL group-by
# on top of these — but the reverse is not true, which is exactly
# why H3 is a strict upgrade for surge / matching consumers.
# -------------------------------------------------------------------------

GOLD_ACTIVE_TRIPS_H3_SCHEMA: StructType = StructType(
    [
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        # H3 cell IDs are 15-character lowercase hex strings.
        StructField("h3_r8", StringType(), nullable=False),
        # Borough kept alongside for cheap drill-through / BI rollups.
        # Derived from the same ping's (lat, lng) via ``borough_of_expr``.
        StructField("borough", StringType(), nullable=True),
        StructField("active_trips", IntegerType(), nullable=False),
        StructField("gold_processed_ts", TimestampType(), nullable=False),
    ]
)


GOLD_DEMAND_H3_SCHEMA: StructType = StructType(
    [
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("h3_r8", StringType(), nullable=False),
        StructField("borough", StringType(), nullable=True),
        StructField("ping_count", LongType(), nullable=False),
        StructField("distinct_trips", LongType(), nullable=False),
        StructField("distinct_drivers", LongType(), nullable=False),
        StructField("gold_processed_ts", TimestampType(), nullable=False),
    ]
)
