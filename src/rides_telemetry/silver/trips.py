"""Silver trip facts — bronze_rides_lifecycle → silver_trip_facts.

The tricky one. Bronze has *one row per lifecycle event* (up to five
per trip: requested / accepted / started / ended / cancelled). Silver
has *one row per trip*, with each event contributing its own set of
columns.

Approach: streaming ``foreachBatch`` + Delta ``MERGE``.

    - Read the bronze lifecycle Delta table as a stream.
    - For each micro-batch, group by ``trip_id`` to fold that batch's
      events into partial trip-fact rows.
    - MERGE those partials into ``silver_trip_facts``:
        * If the trip already exists, ``COALESCE`` each field so a
          late-arriving event fills in gaps without clobbering values
          set by earlier events (or by an earlier micro-batch).
        * If not, insert a new row with whatever fields are set.
    - Recompute the derived ``status`` column from whichever
      lifecycle timestamps are now populated.

Why ``foreachBatch`` instead of a streaming aggregation?

    Streaming ``groupBy(...).agg(...)`` on Delta with output mode
    ``update`` would work, but Delta's MERGE is the canonical way to
    do idempotent upserts and it's what Databricks recommends. Each
    micro-batch is one atomic MERGE — restarting mid-way re-processes
    the batch cleanly (idempotent because the same set of events
    produces the same MERGE outcome).

Status state machine (derived at MERGE time, single source of truth):

    CANCELLED (terminal)   ← cancelled_ts set
    COMPLETED (terminal)   ← ended_ts set
    IN_PROGRESS            ← started_ts set
    ACCEPTED               ← accepted_ts set
    PENDING                ← only requested_ts set

Terminal states win — a trip that gets an out-of-order ``trip_started``
event after ``trip_cancelled`` still shows CANCELLED because the CASE
checks the cancelled/ended timestamps first.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812

from rides_telemetry.bronze.schemas import BRONZE_TRIPS_TABLE
from rides_telemetry.silver.schemas import (
    SILVER_TRIPS_SCHEMA,
    SILVER_TRIPS_TABLE,
)
from rides_telemetry.silver.streaming import SilverStreamConfig
from rides_telemetry.spark import default_checkpoint_dir

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery


# Status expression — reusable so ``shape_trip_facts_batch`` (used by
# tests and for insert-time status) and the MERGE update clause both
# stay in sync. Pass the column expressions that resolve to each
# timestamp; the CASE picks the "furthest" state that has a timestamp.
_STATUS_CASE_TEMPLATE = """
CASE
    WHEN {cancelled_ts} IS NOT NULL THEN 'CANCELLED'
    WHEN {ended_ts}     IS NOT NULL THEN 'COMPLETED'
    WHEN {started_ts}   IS NOT NULL THEN 'IN_PROGRESS'
    WHEN {accepted_ts}  IS NOT NULL THEN 'ACCEPTED'
    ELSE 'PENDING'
END
""".strip()


_log = logging.getLogger(__name__)


def stream_trips_to_silver(
    spark: SparkSession,
    config: SilverStreamConfig | None = None,
    *,
    await_termination: bool = True,
) -> StreamingQuery:
    """Bronze lifecycle → silver trip facts (streaming upsert)."""
    cfg = config or SilverStreamConfig()

    bronze_path = (cfg.warehouse_root / BRONZE_TRIPS_TABLE).resolve()
    silver_path = (cfg.warehouse_root / SILVER_TRIPS_TABLE).resolve()
    checkpoint_path = default_checkpoint_dir("silver_trips").resolve()
    checkpoint_path.mkdir(parents=True, exist_ok=True)

    if not bronze_path.exists():
        raise FileNotFoundError(
            f"bronze lifecycle table not found at {bronze_path}. "
            f"Run `python -m rides_telemetry.bronze --topic trips` first."
        )

    _ensure_silver_trips_table(spark, silver_path)

    bronze = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", str(cfg.max_files_per_trigger))
        .load(str(bronze_path))
    )

    writer = (
        bronze.writeStream.queryName("silver_trip_facts")
        .option("checkpointLocation", str(checkpoint_path))
        .foreachBatch(_make_batch_upsert(str(silver_path)))
        .outputMode("update")
    )
    if cfg.trigger_processing_time:
        writer = writer.trigger(processingTime=cfg.trigger_processing_time)
    else:
        writer = writer.trigger(availableNow=True)

    _log.info(
        "starting silver trips stream: %s -> %s (checkpoint=%s)",
        bronze_path,
        silver_path,
        checkpoint_path,
    )
    query = writer.start()
    if await_termination:
        query.awaitTermination()
    return query


# -------------------------------------------------------------------------
# Building blocks (importable + testable in isolation)
# -------------------------------------------------------------------------


def shape_trip_facts_batch(bronze_batch: DataFrame) -> DataFrame:
    """Fold a batch of bronze lifecycle events into partial trip-fact rows.

    One output row per ``trip_id`` in the input batch. Timestamps for
    event types not present in the batch are ``NULL`` — the MERGE
    layer decides whether to COALESCE them against existing values in
    the silver table.

    Also emits ``status`` computed from the batch's own timestamps.
    On MERGE this may be overwritten if the existing silver row has
    further-along timestamps.
    """

    def _ts_when(event_type: str, source_col: str = "event_ts") -> F.Column:
        return F.max(F.when(F.col("event_type") == event_type, F.col(source_col)))

    def _val_when(event_type: str, source_col: str) -> F.Column:
        return F.max(F.when(F.col("event_type") == event_type, F.col(source_col)))

    aggregated = bronze_batch.groupBy("trip_id").agg(
        F.max("rider_id").alias("rider_id"),
        # driver_id is null in trip_requested/cancelled but set from
        # trip_accepted onward. ``max`` on a nullable string picks the
        # non-null value (lexicographic max, but there's only one per trip).
        F.max("driver_id").alias("driver_id"),
        # Per-event-type timestamps
        _ts_when("trip_requested").alias("requested_ts"),
        _ts_when("trip_accepted").alias("accepted_ts"),
        _ts_when("trip_started").alias("started_ts"),
        _ts_when("trip_ended").alias("ended_ts"),
        _ts_when("trip_cancelled").alias("cancelled_ts"),
        # Geography — carried on trip_requested
        _val_when("trip_requested", "pickup_lat").alias("pickup_lat"),
        _val_when("trip_requested", "pickup_lng").alias("pickup_lng"),
        _val_when("trip_requested", "dropoff_lat").alias("dropoff_lat"),
        _val_when("trip_requested", "dropoff_lng").alias("dropoff_lng"),
        # Outcome — carried on trip_ended / trip_cancelled
        _val_when("trip_ended", "trip_distance_km").alias("trip_distance_km"),
        _val_when("trip_ended", "fare_amount_usd").alias("fare_amount_usd"),
        _val_when("trip_cancelled", "cancelled_by").alias("cancelled_by"),
        # Ops
        F.min("event_ts").alias("first_event_ts"),
        F.max("event_ts").alias("last_event_ts"),
    )

    status_expr = _STATUS_CASE_TEMPLATE.format(
        cancelled_ts="cancelled_ts",
        ended_ts="ended_ts",
        started_ts="started_ts",
        accepted_ts="accepted_ts",
    )
    return aggregated.withColumn("status", F.expr(status_expr)).withColumn(
        "silver_processed_ts", F.current_timestamp()
    )


# -------------------------------------------------------------------------
# Internals
# -------------------------------------------------------------------------


def _ensure_silver_trips_table(spark: SparkSession, table_path) -> None:
    """Create the silver Delta table if it doesn't already exist.

    ``DeltaTable.createIfNotExists`` writes just the ``_delta_log/``
    metadata for an empty table with the target schema, so the first
    ``foreachBatch`` can MERGE into it without failing with
    "table not found".
    """
    from delta.tables import DeltaTable  # local import — pyspark-only path

    (
        DeltaTable.createIfNotExists(spark)
        .location(str(table_path))
        .addColumns(SILVER_TRIPS_SCHEMA)
        .execute()
    )


def _make_batch_upsert(silver_path: str):
    """Return the ``foreachBatch`` callback bound to a target Delta path."""

    def _upsert(batch_df: DataFrame, _batch_id: int) -> None:
        # Local import so this module stays importable without delta on
        # the path (e.g. for schema-only unit tests).
        from delta.tables import DeltaTable

        if batch_df.rdd.isEmpty():  # noqa: SIM108 — clearer than a conditional expr here
            return

        source = shape_trip_facts_batch(batch_df)
        target = DeltaTable.forPath(batch_df.sparkSession, silver_path)

        # COALESCE strategy:
        # - Timestamps: prefer the *existing* value (first-write-wins for
        #   any lifecycle timestamp — a duplicate event shouldn't drift it).
        # - driver_id / geography / outcome: prefer the *new* non-null
        #   value coming from the source, since it's fine to fill in a
        #   previously-unknown driver on a late trip_accepted.
        target.alias("t").merge(
            source.alias("s"),
            "t.trip_id = s.trip_id",
        ).whenMatchedUpdate(
            set={
                "driver_id": "COALESCE(s.driver_id, t.driver_id)",
                "requested_ts": "COALESCE(t.requested_ts, s.requested_ts)",
                "accepted_ts": "COALESCE(t.accepted_ts, s.accepted_ts)",
                "started_ts": "COALESCE(t.started_ts, s.started_ts)",
                "ended_ts": "COALESCE(t.ended_ts, s.ended_ts)",
                "cancelled_ts": "COALESCE(t.cancelled_ts, s.cancelled_ts)",
                "pickup_lat": "COALESCE(t.pickup_lat, s.pickup_lat)",
                "pickup_lng": "COALESCE(t.pickup_lng, s.pickup_lng)",
                "dropoff_lat": "COALESCE(t.dropoff_lat, s.dropoff_lat)",
                "dropoff_lng": "COALESCE(t.dropoff_lng, s.dropoff_lng)",
                "trip_distance_km": "COALESCE(t.trip_distance_km, s.trip_distance_km)",
                "fare_amount_usd": "COALESCE(t.fare_amount_usd, s.fare_amount_usd)",
                "cancelled_by": "COALESCE(t.cancelled_by, s.cancelled_by)",
                "first_event_ts": "LEAST(t.first_event_ts, s.first_event_ts)",
                "last_event_ts": "GREATEST(t.last_event_ts, s.last_event_ts)",
                "silver_processed_ts": "s.silver_processed_ts",
                # Status: recompute from the MERGED timestamps, not the
                # source's alone. Terminal states (CANCELLED/COMPLETED)
                # dominate no matter which side has them.
                "status": _STATUS_CASE_TEMPLATE.format(
                    cancelled_ts="COALESCE(t.cancelled_ts, s.cancelled_ts)",
                    ended_ts="COALESCE(t.ended_ts, s.ended_ts)",
                    started_ts="COALESCE(t.started_ts, s.started_ts)",
                    accepted_ts="COALESCE(t.accepted_ts, s.accepted_ts)",
                ),
            }
        ).whenNotMatchedInsert(
            values={
                "trip_id": "s.trip_id",
                "rider_id": "s.rider_id",
                "driver_id": "s.driver_id",
                "requested_ts": "s.requested_ts",
                "accepted_ts": "s.accepted_ts",
                "started_ts": "s.started_ts",
                "ended_ts": "s.ended_ts",
                "cancelled_ts": "s.cancelled_ts",
                "pickup_lat": "s.pickup_lat",
                "pickup_lng": "s.pickup_lng",
                "dropoff_lat": "s.dropoff_lat",
                "dropoff_lng": "s.dropoff_lng",
                "trip_distance_km": "s.trip_distance_km",
                "fare_amount_usd": "s.fare_amount_usd",
                "cancelled_by": "s.cancelled_by",
                "status": "s.status",
                "first_event_ts": "s.first_event_ts",
                "last_event_ts": "s.last_event_ts",
                "silver_processed_ts": "s.silver_processed_ts",
            }
        ).execute()

    return _upsert
