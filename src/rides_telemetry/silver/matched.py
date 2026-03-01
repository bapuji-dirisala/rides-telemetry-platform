"""Silver GPS-matched — stream-stream join of silver GPS ↔ silver trip facts.

The marquee streaming feature of the lakehouse. Every GPS ping is
validated against its trip's ``[started_ts, ended_ts]`` window and
enriched with the trip's business context (rider, pickup/dropoff,
status) so gold marts don't need to re-join.

## How the join stays bounded

Structured Streaming allows stream-stream joins only when both sides
carry a watermark AND the join predicate constrains the two sides'
timestamps to a bounded interval. Both conditions hold here:

- **GPS side:** ``withWatermark("event_ts", GPS_WATERMARK)`` — 10 min.
- **Trips side:** ``withWatermark("last_event_ts", MATCHED_TRIP_WATERMARK)`` — 30 min.
- **Join predicate:** ``event_ts BETWEEN started_ts - 30s
  AND COALESCE(ended_ts, last_event_ts) + 5 min``.

Spark uses these to size the state store — old rows are evicted once
the watermark advances past their usable range. Without the time
constraint the state would grow unbounded and OOM within an hour.

## Trip stream: ``ignoreChanges``

``silver_trip_facts`` is written with ``MERGE INTO`` (see
``trips.py``), which updates existing rows every time a new event
for a trip arrives. Delta's streaming source treats an updated file
as "changes since checkpoint" and refuses to read it in
``append``-only mode unless we opt in via ``ignoreChanges=true``.
This means the join may see the same ``trip_id`` more than once as
it progresses through its state machine — which is fine, the join
is deterministic on ``(trip_id, event_id)`` and the join key +
window predicate deduplicates naturally.

## Trip filter: started, non-cancelled

We pre-filter the trip stream to ``started_ts IS NOT NULL AND
cancelled_ts IS NULL``. A ping received *after* a cancellation is
almost certainly noise from a device that hadn't got the cancel
signal yet; carrying those through to gold would pollute active-
trip counts and surge signals.

## Output: append-only

Every matched (ping, trip) row is a distinct observation. No MERGE,
just append. Downstream deduplication is on ``event_id``, and
``event_id`` is already unique in silver GPS.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812

from rides_telemetry.silver.schemas import (
    GPS_WATERMARK,
    MATCHED_JOIN_END_SLACK,
    MATCHED_JOIN_START_SLACK,
    MATCHED_TRIP_WATERMARK,
    SILVER_GPS_TABLE,
    SILVER_MATCHED_TABLE,
    SILVER_TRIPS_TABLE,
)
from rides_telemetry.silver.streaming import SilverStreamConfig
from rides_telemetry.spark import default_checkpoint_dir

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

_log = logging.getLogger(__name__)


def stream_matched_to_silver(
    spark: SparkSession,
    config: SilverStreamConfig | None = None,
    *,
    await_termination: bool = True,
) -> StreamingQuery:
    """Silver GPS ⋈ silver trip facts → silver_gps_matched."""
    cfg = config or SilverStreamConfig()

    gps_path = (cfg.warehouse_root / SILVER_GPS_TABLE).resolve()
    trips_path = (cfg.warehouse_root / SILVER_TRIPS_TABLE).resolve()
    matched_path = (cfg.warehouse_root / SILVER_MATCHED_TABLE).resolve()
    checkpoint_path = default_checkpoint_dir("silver_matched").resolve()
    checkpoint_path.mkdir(parents=True, exist_ok=True)

    for label, p in (("silver GPS", gps_path), ("silver trip facts", trips_path)):
        if not p.exists():
            raise FileNotFoundError(
                f"{label} table not found at {p}. "
                f"Run the upstream silver jobs first "
                f"(`python -m rides_telemetry.silver --stream gps|trips`)."
            )

    gps_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", str(cfg.max_files_per_trigger))
        .load(str(gps_path))
    )
    trips_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", str(cfg.max_files_per_trigger))
        # Trip facts rows get MERGE-updated in place; opt into
        # reading updated files as a source.
        .option("ignoreChanges", "true")
        .load(str(trips_path))
    )

    matched = shape_matched(gps_stream, trips_stream)

    writer = (
        matched.writeStream.format("delta")
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
        .queryName("silver_gps_matched")
    )
    if cfg.trigger_processing_time:
        writer = writer.trigger(processingTime=cfg.trigger_processing_time)
    else:
        writer = writer.trigger(availableNow=True)

    _log.info(
        "starting silver matched stream: gps=%s + trips=%s -> %s",
        gps_path,
        trips_path,
        matched_path,
    )
    query = writer.start(str(matched_path))
    if await_termination:
        query.awaitTermination()
    return query


def shape_matched(gps: DataFrame, trips: DataFrame) -> DataFrame:
    """Join GPS pings with their trip fact and project to matched schema.

    Broken out from :func:`stream_matched_to_silver` so tests can drive
    it against static DataFrames without a running stream. The join
    behaves the same in batch mode — the watermarks and time-range
    predicate are meaningful only in the streaming plan, so the batch
    test path just gets a plain inner join.
    """
    gps_w = gps.withWatermark("event_ts", GPS_WATERMARK).alias("g")

    trips_w = (
        trips
        # Only join against trips that have actually started, and that
        # weren't cancelled. Pre-filter to shrink join state.
        .filter(F.col("started_ts").isNotNull() & F.col("cancelled_ts").isNull())
        .withWatermark("last_event_ts", MATCHED_TRIP_WATERMARK)
        .alias("t")
    )

    join_expr = F.expr(
        f"""
        g.trip_id = t.trip_id AND
        g.event_ts >= t.started_ts - interval {MATCHED_JOIN_START_SLACK} AND
        g.event_ts <= COALESCE(t.ended_ts, t.last_event_ts) + interval {MATCHED_JOIN_END_SLACK}
        """
    )

    joined = gps_w.join(trips_w, join_expr, how="inner")

    return joined.select(
        # GPS side
        F.col("g.event_id").alias("event_id"),
        F.col("g.event_ts").alias("event_ts"),
        F.col("g.trip_id").alias("trip_id"),
        F.col("g.driver_id").alias("driver_id"),
        F.col("g.lat").alias("lat"),
        F.col("g.lng").alias("lng"),
        F.col("g.speed_kmh").alias("speed_kmh"),
        F.col("g.heading_deg").alias("heading_deg"),
        F.col("g.accuracy_m").alias("accuracy_m"),
        # Trip context
        F.col("t.rider_id").alias("rider_id"),
        F.col("t.status").alias("trip_status"),
        F.col("t.requested_ts").alias("trip_requested_ts"),
        F.col("t.started_ts").alias("trip_started_ts"),
        F.col("t.ended_ts").alias("trip_ended_ts"),
        F.col("t.pickup_lat").alias("pickup_lat"),
        F.col("t.pickup_lng").alias("pickup_lng"),
        F.col("t.dropoff_lat").alias("dropoff_lat"),
        F.col("t.dropoff_lng").alias("dropoff_lng"),
        # Ops — take ingest_ts from the GPS side (that's when the ping
        # arrived, which is what latency dashboards want).
        F.col("g.ingest_ts").alias("ingest_ts"),
        F.current_timestamp().alias("silver_processed_ts"),
    )
