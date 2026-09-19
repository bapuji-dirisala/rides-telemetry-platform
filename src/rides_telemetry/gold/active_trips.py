"""Gold — active trips now per borough.

Rolling snapshot: for each 1-minute tumbling window and each NYC
borough, how many *distinct* trips were seen as ``IN_PROGRESS`` in
that window. Feeds surge pricing ("supply is tight in Brooklyn
right now") and real-time dashboards ("here's the map right now").

## Pipeline

```text
silver_gps_matched
    → withWatermark("event_ts", 10 min)
    → filter(trip_status == "IN_PROGRESS")
    → borough = borough_of_expr(lat, lng)
    → filter(borough IS NOT NULL)                # defensive
    → groupBy(window(event_ts, 1 min), borough)
    → agg(countDistinct(trip_id) as active_trips)
    → project to gold schema
    → writeStream.format("delta").outputMode("append")
```

## Why append + tumbling windows

Structured Streaming emits a row for a tumbling window only once
the watermark has passed the window's end — meaning "no more events
for this window will arrive". That gives us idempotent, exactly-once
gold rows: no MERGE needed, no partial-window rewrites.

The tradeoff: freshness is bounded by the watermark. A 1-minute
window with a 10-minute watermark on the source means gold rows for
minute X land at approximately X + 10 min. That's fine for a demo /
surge signal; production would tighten the watermark once the
upstream latency budget is measured.

## Why ``approx_count_distinct(trip_id)`` and not ``count(*)``

``silver_gps_matched`` may emit the same ``event_id`` more than once
if the underlying trip fact row was updated between windows (the
trip stream reads with ``ignoreChanges=true``). Distinct counting
on ``trip_id`` sidesteps that — the trip is counted at most once per
window regardless of how many pings or duplicated ping-rows it
produced.

**Why the ``approx`` variant?** Spark Structured Streaming does not
support exact ``countDistinct`` inside a streaming aggregation
(see the Spark docs — "Distinct aggregations are not supported on
streaming DataFrames/Datasets"). ``approx_count_distinct`` uses
HyperLogLog and gives us a bounded relative error (default ~5%),
which for a surge signal is well inside the noise floor of the
underlying data. For cardinalities under ~100 (the realistic per-
borough per-minute range) HLL is effectively exact.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812

from rides_telemetry.geo.spark import borough_of_expr
from rides_telemetry.gold.schemas import (
    ACTIVE_TRIPS_WINDOW,
    GOLD_ACTIVE_TRIPS_TABLE,
)
from rides_telemetry.gold.streaming import GoldStreamConfig
from rides_telemetry.silver.schemas import GPS_WATERMARK, SILVER_MATCHED_TABLE
from rides_telemetry.spark import default_checkpoint_dir

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

_log = logging.getLogger(__name__)


def stream_active_trips_mart(
    spark: SparkSession,
    config: GoldStreamConfig | None = None,
    *,
    await_termination: bool = True,
) -> StreamingQuery:
    """silver_gps_matched → gold_active_trips_now."""
    cfg = config or GoldStreamConfig()

    source_path = (cfg.warehouse_root / SILVER_MATCHED_TABLE).resolve()
    sink_path = (cfg.warehouse_root / GOLD_ACTIVE_TRIPS_TABLE).resolve()
    checkpoint_path = default_checkpoint_dir("gold_active_trips").resolve()
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

    gold = shape_active_trips(matched)

    writer = (
        gold.writeStream.format("delta")
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
        .queryName("gold_active_trips_now")
    )
    if cfg.trigger_processing_time:
        writer = writer.trigger(processingTime=cfg.trigger_processing_time)
    else:
        writer = writer.trigger(availableNow=True)

    _log.info(
        "starting gold active_trips: %s -> %s (checkpoint=%s)",
        source_path,
        sink_path,
        checkpoint_path,
    )
    query = writer.start(str(sink_path))
    if await_termination:
        query.awaitTermination()
    return query


def shape_active_trips(matched: DataFrame) -> DataFrame:
    """Aggregate silver_gps_matched into per-borough per-minute counts.

    Broken out so the transformation can be tested against a batch
    DataFrame — the watermark is a no-op in batch mode, and the
    windowed aggregation behaves identically.
    """
    return (
        matched.withWatermark("event_ts", GPS_WATERMARK)
        .filter(F.col("trip_status") == "IN_PROGRESS")
        .withColumn("borough", borough_of_expr("lat", "lng"))
        .filter(F.col("borough").isNotNull())
        .groupBy(
            F.window(F.col("event_ts"), ACTIVE_TRIPS_WINDOW).alias("w"),
            F.col("borough"),
        )
        .agg(F.approx_count_distinct("trip_id").alias("active_trips"))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("borough"),
            F.col("active_trips").cast("int").alias("active_trips"),
            F.current_timestamp().alias("gold_processed_ts"),
        )
    )
