"""Spark schemas + tunable thresholds for the silver layer.

Design notes:

- Silver rows carry only *business* columns plus a small operational
  footer (``ingest_ts`` from bronze, ``silver_processed_ts`` from
  this layer). Kafka envelope columns are dropped — they belong to
  ingestion, not to consumers.
- ``SILVER_TRIPS_SCHEMA`` is the exact schema the ``foreachBatch``
  writer targets. Adding a field here means adding it to both the
  MERGE ``insert`` and ``update`` sets in :mod:`~rides_telemetry.silver.trips`.
- Quality thresholds live here (not sprinkled in the job code) so
  a reviewer can find them in one place and change them without
  touching Spark plumbing.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# -------------------------------------------------------------------------
# Table names
# -------------------------------------------------------------------------

SILVER_GPS_TABLE = "silver_rides_gps"
SILVER_TRIPS_TABLE = "silver_trip_facts"
SILVER_MATCHED_TABLE = "silver_gps_matched"


# -------------------------------------------------------------------------
# Silver GPS — one row per validated GPS ping, deduped by event_id.
# Nullability mirrors bronze; ``speed_kmh`` / ``heading_deg`` /
# ``accuracy_m`` remain optional because not every device reports them.
# -------------------------------------------------------------------------

SILVER_GPS_SCHEMA: StructType = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("trip_id", StringType(), nullable=False),
        StructField("driver_id", StringType(), nullable=False),
        StructField("lat", DoubleType(), nullable=False),
        StructField("lng", DoubleType(), nullable=False),
        StructField("speed_kmh", DoubleType(), nullable=True),
        StructField("heading_deg", DoubleType(), nullable=True),
        StructField("accuracy_m", DoubleType(), nullable=True),
        StructField("ingest_ts", TimestampType(), nullable=False),
        StructField("silver_processed_ts", TimestampType(), nullable=False),
    ]
)


# -------------------------------------------------------------------------
# Silver trip facts — one row per trip_id, folded from up to five
# lifecycle events. Every ``*_ts`` field is nullable because a trip
# may reach silver in an incomplete state (e.g. a trip that's been
# requested but not yet accepted). The ``status`` column is the
# derived state machine label, so downstream code doesn't have to
# re-run the fold on every read.
# -------------------------------------------------------------------------

SILVER_TRIPS_SCHEMA: StructType = StructType(
    [
        # Identity
        StructField("trip_id", StringType(), nullable=False),
        StructField("rider_id", StringType(), nullable=False),
        StructField("driver_id", StringType(), nullable=True),
        # Lifecycle timestamps — one per event type, all nullable
        StructField("requested_ts", TimestampType(), nullable=True),
        StructField("accepted_ts", TimestampType(), nullable=True),
        StructField("started_ts", TimestampType(), nullable=True),
        StructField("ended_ts", TimestampType(), nullable=True),
        StructField("cancelled_ts", TimestampType(), nullable=True),
        # Geography (from the trip_requested event)
        StructField("pickup_lat", DoubleType(), nullable=True),
        StructField("pickup_lng", DoubleType(), nullable=True),
        StructField("dropoff_lat", DoubleType(), nullable=True),
        StructField("dropoff_lng", DoubleType(), nullable=True),
        # Trip outcome (only populated for ended / cancelled trips)
        StructField("trip_distance_km", DoubleType(), nullable=True),
        StructField("fare_amount_usd", DoubleType(), nullable=True),
        StructField("cancelled_by", StringType(), nullable=True),
        # Derived: current state machine label. One of
        # {PENDING, ACCEPTED, IN_PROGRESS, COMPLETED, CANCELLED}.
        StructField("status", StringType(), nullable=False),
        # Ops
        StructField("first_event_ts", TimestampType(), nullable=False),
        StructField("last_event_ts", TimestampType(), nullable=False),
        StructField("silver_processed_ts", TimestampType(), nullable=False),
    ]
)


# -------------------------------------------------------------------------
# Quality thresholds — the single place to tune silver's row filters.
# -------------------------------------------------------------------------

MAX_PLAUSIBLE_SPEED_KMH: float = 200.0
"""Any GPS ping reporting a speed above this is treated as bogus.

Even highway driving in NYC tops out well below 200 km/h; a value
above this is almost certainly a device glitch (bad Doppler read,
teleport between two towers) rather than a real driver."""

MAX_ACCEPTABLE_ACCURACY_M: float = 500.0
"""GPS accuracy worse than this contributes noise, not signal.

At >500 m radius the ping tells us nothing useful for geospatial
marts (which is roughly a Manhattan avenue block). We drop these
in silver rather than let them dilute surge / matching signals."""

# Watermark tolerances. Bounded state = safe streaming.
GPS_WATERMARK: str = "10 minutes"
"""Late GPS pings beyond 10 minutes are dropped from the dedup state.

10 minutes covers realistic device buffering (a driver goes into a
tunnel and re-uploads a burst) without blowing up the state store."""

TRIP_WATERMARK: str = "2 hours"
"""Longest plausible trip duration + slack.

Even a Manhattan-to-JFK trip in traffic is under 90 minutes; 2 hours
gives room for late-arriving ``trip_ended`` events without holding
per-trip aggregation state indefinitely."""

MATCHED_JOIN_START_SLACK: str = "30 seconds"
"""Clock-skew tolerance for the start of the trip-window join.

A GPS ping's ``event_ts`` may be a fraction of a second earlier than
``trip_started_ts`` due to the driver's device clock. Allow 30 s of
slack so we don't drop legitimate first-few-pings of a trip."""

MATCHED_JOIN_END_SLACK: str = "5 minutes"
"""Slack applied to ``trip_ended_ts`` for the end of the trip window.

Drivers often send a final burst of GPS after ``trip_ended`` fires
(app buffering, brief disconnect). 5 minutes captures these tail
pings without pulling in the *next* trip's first pings for the same
driver."""

MATCHED_TRIP_WATERMARK: str = "30 minutes"
"""Watermark on ``last_event_ts`` for the trip-facts side of the join.

The join's buffered state is bounded by max(GPS_WATERMARK,
MATCHED_TRIP_WATERMARK) + MATCHED_JOIN_END_SLACK. 30 min tolerates
long-duration trips (LGA/JFK runs) without letting per-trip state
grow unbounded."""


# -------------------------------------------------------------------------
# Silver matched — validated GPS pings enriched with their trip context.
# The output of the stream-stream join between silver_rides_gps and
# silver_trip_facts. This is the go-to source for gold marts (surge,
# fraud, matching): every row is (a) a real ping inside a real trip's
# time window and (b) already tagged with rider/driver/pickup/dropoff.
# -------------------------------------------------------------------------

SILVER_MATCHED_SCHEMA: StructType = StructType(
    [
        # GPS ping (from silver_rides_gps)
        StructField("event_id", StringType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("trip_id", StringType(), nullable=False),
        StructField("driver_id", StringType(), nullable=False),
        StructField("lat", DoubleType(), nullable=False),
        StructField("lng", DoubleType(), nullable=False),
        StructField("speed_kmh", DoubleType(), nullable=True),
        StructField("heading_deg", DoubleType(), nullable=True),
        StructField("accuracy_m", DoubleType(), nullable=True),
        # Trip context (from silver_trip_facts)
        StructField("rider_id", StringType(), nullable=False),
        StructField("trip_status", StringType(), nullable=False),
        StructField("trip_requested_ts", TimestampType(), nullable=True),
        StructField("trip_started_ts", TimestampType(), nullable=False),
        StructField("trip_ended_ts", TimestampType(), nullable=True),
        StructField("pickup_lat", DoubleType(), nullable=True),
        StructField("pickup_lng", DoubleType(), nullable=True),
        StructField("dropoff_lat", DoubleType(), nullable=True),
        StructField("dropoff_lng", DoubleType(), nullable=True),
        # Ops
        StructField("ingest_ts", TimestampType(), nullable=False),
        StructField("silver_processed_ts", TimestampType(), nullable=False),
    ]
)
