"""Gold — teleport fraud detection.

For each driver, compare every ping with the driver's *previous*
ping. If the implied speed between them (Haversine distance /
elapsed time) exceeds :data:`MAX_PLAUSIBLE_TELEPORT_KMH`, flag the
pair — this driver's device either jumped impossibly far in a
short time (GPS spoofing) or swapped between two devices (a
common ride-hail account-sharing signal).

## Why ``foreachBatch``

The core operation — ``lag()`` over ``driver_id`` ordered by
``event_ts`` — is a *window function over the whole stream*, which
Spark Structured Streaming rejects with "Non-time-based windows
are not supported on streaming DataFrames/Datasets". Batch mode
allows it freely, so we sink the streaming source into a
``foreachBatch`` handler and do the window + filter + write inside
that handler on the per-micro-batch DataFrame.

## Batch-boundary limitation

Each micro-batch computes ``lag()`` independently. If a driver's
last ping lands at the end of batch N and their next ping arrives
in batch N+1, the two won't be compared and a teleport across
that boundary is missed.

Two mitigations, in order of ambition:

1. **Cadence tuning.** With the default 30-s trigger and typical
   ping rates (~1 per 2-5 s), each driver contributes 6-15 pings
   per batch, so most teleports land inside a batch.
2. **Stateful processing.** Rewrite the shape as
   ``flatMapGroupsWithState`` keyed on ``driver_id``, carrying
   "last ping seen" across batches. That's a Phase 5c-scoped
   change; the ``foreachBatch`` version is enough for a demo mart.

For the one-shot ``--once`` runs used in local verification, the
entire source is drained into a single batch, so no boundary
issues apply.

## Guardrails against false positives

- ``distance_km >= 0.5`` — stationary jitter ("driver at a red
  light, GPS drifts 100 m") isn't fraud even if the implied
  speed is high.
- ``time_delta_seconds >= 5`` — pings closer than 5 s apart have
  a divide-by-near-zero effect on implied speed.
- We keep both raw geometry columns (`lat`/`lng` and `prev_*`) in
  the output so operators can eyeball the flagged pair on a map
  without a follow-up query.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812

from rides_telemetry.geo.spark import haversine_km_expr
from rides_telemetry.gold.schemas import (
    GOLD_FRAUD_TELEPORT_TABLE,
    MAX_PLAUSIBLE_TELEPORT_KMH,
    MIN_TELEPORT_DISTANCE_KM,
    MIN_TELEPORT_TIME_DELTA_SECONDS,
)
from rides_telemetry.gold.streaming import GoldStreamConfig
from rides_telemetry.silver.schemas import SILVER_MATCHED_TABLE
from rides_telemetry.spark import default_checkpoint_dir

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

_log = logging.getLogger(__name__)


def stream_fraud_teleport_mart(
    spark: SparkSession,
    config: GoldStreamConfig | None = None,
    *,
    await_termination: bool = True,
) -> StreamingQuery:
    """silver_gps_matched → gold_fraud_teleport (via foreachBatch)."""
    cfg = config or GoldStreamConfig()

    source_path = (cfg.warehouse_root / SILVER_MATCHED_TABLE).resolve()
    sink_path = (cfg.warehouse_root / GOLD_FRAUD_TELEPORT_TABLE).resolve()
    checkpoint_path = default_checkpoint_dir("gold_fraud_teleport").resolve()
    checkpoint_path.mkdir(parents=True, exist_ok=True)

    if not source_path.exists():
        raise FileNotFoundError(
            f"silver matched table not found at {source_path}. "
            f"Run `python -m rides_telemetry.silver --stream matched` first."
        )

    matched = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", str(cfg.max_files_per_trigger))
        .load(str(source_path))
    )

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        flagged = shape_fraud_teleport(batch_df)
        # ``append`` is safe because every flagged (prev, curr) pair
        # is uniquely identified by ``event_id`` — the current
        # ping's event_id is unique per raw GPS event, and each
        # event_id appears at most once as "current" per batch.
        (flagged.write.format("delta").mode("append").save(str(sink_path)))
        _log.info("gold_fraud_teleport batch %s: wrote %s rows", batch_id, flagged.count())

    writer = (
        matched.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", str(checkpoint_path))
        .queryName("gold_fraud_teleport")
    )
    if cfg.trigger_processing_time:
        writer = writer.trigger(processingTime=cfg.trigger_processing_time)
    else:
        writer = writer.trigger(availableNow=True)

    _log.info(
        "starting gold fraud_teleport: %s -> %s (checkpoint=%s)",
        source_path,
        sink_path,
        checkpoint_path,
    )
    query = writer.start()
    if await_termination:
        query.awaitTermination()
    return query


def shape_fraud_teleport(matched: DataFrame) -> DataFrame:
    """Detect teleport-style GPS anomalies inside a batch.

    Runs in batch mode (called from ``foreachBatch``) so it can use
    the ``lag()`` window function that Structured Streaming rejects.
    Broken out so unit tests can drive it directly against a static
    DataFrame with no streaming plumbing.
    """
    window = Window.partitionBy("driver_id").orderBy("event_ts")

    with_prev = matched.select(
        F.col("event_id"),
        F.col("event_ts"),
        F.col("driver_id"),
        F.col("trip_id"),
        F.col("lat"),
        F.col("lng"),
        F.lag("event_id").over(window).alias("prev_event_id"),
        F.lag("event_ts").over(window).alias("prev_event_ts"),
        F.lag("trip_id").over(window).alias("prev_trip_id"),
        F.lag("lat").over(window).alias("prev_lat"),
        F.lag("lng").over(window).alias("prev_lng"),
    )

    with_signal = (
        # Drop the first ping per driver — no previous to compare
        # against — but keep everything else.
        with_prev.filter(F.col("prev_event_id").isNotNull())
        .withColumn(
            "distance_km",
            haversine_km_expr("prev_lat", "prev_lng", "lat", "lng"),
        )
        .withColumn(
            "time_delta_seconds",
            F.col("event_ts").cast("double") - F.col("prev_event_ts").cast("double"),
        )
        .withColumn(
            # Guard divide-by-zero for defensive testing — the filter
            # below drops these rows anyway.
            "implied_speed_kmh",
            F.when(
                F.col("time_delta_seconds") > F.lit(0.0),
                (F.col("distance_km") / F.col("time_delta_seconds")) * F.lit(3600.0),
            ).otherwise(F.lit(0.0)),
        )
    )

    flagged = with_signal.filter(
        (F.col("implied_speed_kmh") > F.lit(MAX_PLAUSIBLE_TELEPORT_KMH))
        & (F.col("time_delta_seconds") >= F.lit(MIN_TELEPORT_TIME_DELTA_SECONDS))
        & (F.col("distance_km") >= F.lit(MIN_TELEPORT_DISTANCE_KM))
    )

    return flagged.select(
        F.col("event_id"),
        F.col("event_ts"),
        F.col("driver_id"),
        F.col("trip_id"),
        F.col("prev_event_id"),
        F.col("prev_event_ts"),
        F.col("prev_trip_id"),
        F.col("lat"),
        F.col("lng"),
        F.col("prev_lat"),
        F.col("prev_lng"),
        F.col("distance_km"),
        F.col("time_delta_seconds"),
        F.col("implied_speed_kmh"),
        F.current_timestamp().alias("flagged_ts"),
    )
