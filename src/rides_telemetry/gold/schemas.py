"""Schemas + tunable windows for the gold layer.

Every mart is defined by a schema here and a streaming job in its
own module. Adding a new mart is: (1) a schema in this file, (2) a
streaming function, (3) tests, (4) a CLI switch — nothing more.
"""

from __future__ import annotations

from pyspark.sql.types import (
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
