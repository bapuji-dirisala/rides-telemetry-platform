"""Gold — demand & supply signal per borough, per 5-min window.

For each 5-minute tumbling window and each NYC borough, three
counts drawn from ``silver_gps_matched``:

- ``ping_count`` — total distinct pings (traffic volume)
- ``distinct_trips`` — how many trips produced pings (demand)
- ``distinct_drivers`` — how many drivers reported (supply)

The ratio ``distinct_trips / distinct_drivers`` is a coarse first-
pass surge signal: > 1 means more active riders than drivers can
comfortably service; < 1 means surplus supply. Real surge would
also factor in incoming requests (not just active trips), which
lands in phase 5b.

## Distinct counting caveats

- ``ping_count`` uses ``approx_count_distinct("event_id")`` rather
  than ``count(*)`` because ``silver_gps_matched`` can emit
  duplicate ``event_id``s when the underlying trip fact was
  MERGE-updated (see the phase-4b doc). Distinct counting gives
  the true traffic volume.
- ``distinct_trips`` / ``distinct_drivers`` are naturally correct
  under duplicates — a trip counted twice is still one trip.
- **Why the ``approx`` variant of each:** Spark Structured Streaming
  does not support exact ``countDistinct`` inside a streaming
  aggregation. ``approx_count_distinct`` uses HyperLogLog with a
  default ~5% relative error — for cardinalities under ~100 (which
  covers a busy borough-minute) it's effectively exact, and at
  higher cardinalities the error is well inside the noise floor of
  a surge signal.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812

from rides_telemetry.geo.spark import borough_of_expr
from rides_telemetry.gold.schemas import (
    DEMAND_WATERMARK,
    DEMAND_WINDOW,
    GOLD_DEMAND_TABLE,
)
from rides_telemetry.gold.streaming import GoldStreamConfig
from rides_telemetry.silver.schemas import SILVER_MATCHED_TABLE
from rides_telemetry.spark import default_checkpoint_dir

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

_log = logging.getLogger(__name__)


def stream_demand_mart(
    spark: SparkSession,
    config: GoldStreamConfig | None = None,
    *,
    await_termination: bool = True,
) -> StreamingQuery:
    """silver_gps_matched → gold_demand_by_borough_5min."""
    cfg = config or GoldStreamConfig()

    source_path = (cfg.warehouse_root / SILVER_MATCHED_TABLE).resolve()
    sink_path = (cfg.warehouse_root / GOLD_DEMAND_TABLE).resolve()
    checkpoint_path = default_checkpoint_dir("gold_demand").resolve()
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

    gold = shape_demand(matched)

    writer = (
        gold.writeStream.format("delta")
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
        .queryName("gold_demand_by_borough_5min")
    )
    if cfg.trigger_processing_time:
        writer = writer.trigger(processingTime=cfg.trigger_processing_time)
    else:
        writer = writer.trigger(availableNow=True)

    _log.info(
        "starting gold demand mart: %s -> %s (checkpoint=%s)",
        source_path,
        sink_path,
        checkpoint_path,
    )
    query = writer.start(str(sink_path))
    if await_termination:
        query.awaitTermination()
    return query


def shape_demand(matched: DataFrame) -> DataFrame:
    """Aggregate matched pings into per-borough demand/supply signal."""
    return (
        matched.withWatermark("event_ts", DEMAND_WATERMARK)
        .withColumn("borough", borough_of_expr("lat", "lng"))
        .filter(F.col("borough").isNotNull())
        .groupBy(
            F.window(F.col("event_ts"), DEMAND_WINDOW).alias("w"),
            F.col("borough"),
        )
        .agg(
            F.approx_count_distinct("event_id").alias("ping_count"),
            F.approx_count_distinct("trip_id").alias("distinct_trips"),
            F.approx_count_distinct("driver_id").alias("distinct_drivers"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("borough"),
            F.col("ping_count"),
            F.col("distinct_trips"),
            F.col("distinct_drivers"),
            F.current_timestamp().alias("gold_processed_ts"),
        )
    )
