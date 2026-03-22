"""Gold — dual-trip driver fraud detection.

For each ``(driver_id, 5-minute tumbling window)``, count how many
*distinct* trips produced pings under that driver. If more than
one, the driver is claiming to be on two trips simultaneously — a
classic ride-hail fraud signature (account sharing, multi-apping
without ending the prior trip, etc.).

## Pipeline

```text
silver_gps_matched
    → withWatermark("event_ts", 10 min)
    → borough = borough_of_expr(lat, lng)   # (not needed; kept out)
    → groupBy(window(event_ts, 5 min), driver_id)
    → agg(
        approx_count_distinct(trip_id) as concurrent_trip_count,
        collect_set(trip_id)           as sample_trip_ids_raw,
      )
    → filter(concurrent_trip_count > 1)
    → project + slice(sample_trip_ids_raw, 5) as sample_trip_ids
    → writeStream.format("delta").outputMode("append")
```

## Trade-offs

- **``approx_count_distinct`` (HyperLogLog).** Same reason as the
  Phase 5a marts: Structured Streaming forbids exact ``countDistinct``
  in a streaming aggregation. For per-driver-per-window cardinality
  (which is 1 in the normal case and ~2-3 in the fraud case), HLL
  is exact. If a driver genuinely juggles more than ~100 distinct
  trip IDs in a 5-min window, we have bigger problems than an ~5%
  count error.
- **``collect_set`` state cost.** ``collect_set`` is a
  ``TypedImperativeAggregate`` — its per-group state is proportional
  to the number of distinct trip_ids in the window, which is small
  (single-digit) even for fraudulent drivers. We then slice to 5
  in the projection so the output row stays readable in a BI tool.
- **``outputMode="append"`` + 10-min watermark.** A ``(driver_id,
  window)`` row is emitted only after the watermark passes the
  window's end — exactly-once, no MERGE, one-watermark latency
  floor. Fine for a fraud mart (the operator's response cycle is
  much longer than 10 min anyway).
- **We deliberately don't filter on borough.** A driver running
  two trips simultaneously *across* boroughs (which our normal
  data doesn't allow, but a fraudster might fake) is exactly the
  kind of thing this mart should catch.

## Detection strength

This is a lightweight signal — it only catches drivers whose
fraud manifests as pings on multiple trip_ids inside a 5-min window.
It doesn't catch:

- Drivers who serially defraud (end trip → immediately claim to
  start a new one within seconds). :mod:`~rides_telemetry.gold.fraud_teleport`
  is the complement for that pattern (impossible movement between
  pings).
- Drivers who spoof GPS to a fixed location. Phase 6 (H3 zones +
  hexbin baseline) is where that lands.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812

from rides_telemetry.gold.schemas import (
    FRAUD_WATERMARK,
    FRAUD_WINDOW,
    GOLD_FRAUD_DUAL_TRIP_TABLE,
)
from rides_telemetry.gold.streaming import GoldStreamConfig
from rides_telemetry.silver.schemas import SILVER_MATCHED_TABLE
from rides_telemetry.spark import default_checkpoint_dir

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

_log = logging.getLogger(__name__)

# Cap on the number of trip IDs surfaced in the ``sample_trip_ids``
# column. Keeps the output row compact for BI tools; operators can
# always drill into silver_gps_matched for the full picture.
_MAX_SAMPLE_TRIP_IDS = 5


def stream_fraud_dual_trip_mart(
    spark: SparkSession,
    config: GoldStreamConfig | None = None,
    *,
    await_termination: bool = True,
) -> StreamingQuery:
    """silver_gps_matched → gold_fraud_dual_trip."""
    cfg = config or GoldStreamConfig()

    source_path = (cfg.warehouse_root / SILVER_MATCHED_TABLE).resolve()
    sink_path = (cfg.warehouse_root / GOLD_FRAUD_DUAL_TRIP_TABLE).resolve()
    checkpoint_path = default_checkpoint_dir("gold_fraud_dual_trip").resolve()
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

    gold = shape_fraud_dual_trip(matched)

    writer = (
        gold.writeStream.format("delta")
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
        .queryName("gold_fraud_dual_trip")
    )
    if cfg.trigger_processing_time:
        writer = writer.trigger(processingTime=cfg.trigger_processing_time)
    else:
        writer = writer.trigger(availableNow=True)

    _log.info(
        "starting gold fraud_dual_trip: %s -> %s (checkpoint=%s)",
        source_path,
        sink_path,
        checkpoint_path,
    )
    query = writer.start(str(sink_path))
    if await_termination:
        query.awaitTermination()
    return query


def shape_fraud_dual_trip(matched: DataFrame) -> DataFrame:
    """Aggregate per-driver per-5-min-window and flag dual trips."""
    return (
        matched.withWatermark("event_ts", FRAUD_WATERMARK)
        .groupBy(
            F.window(F.col("event_ts"), FRAUD_WINDOW).alias("w"),
            F.col("driver_id"),
        )
        .agg(
            F.approx_count_distinct("trip_id").alias("concurrent_trip_count"),
            F.collect_set("trip_id").alias("_all_trip_ids"),
        )
        .filter(F.col("concurrent_trip_count") > F.lit(1))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("driver_id"),
            F.col("concurrent_trip_count").cast("int").alias("concurrent_trip_count"),
            # Slice to keep the row compact. ``sort_array`` first so
            # the sample is deterministic across re-runs of the same
            # batch (helpful for tests + operator sanity).
            F.slice(
                F.sort_array(F.col("_all_trip_ids")),
                F.lit(1),
                F.lit(_MAX_SAMPLE_TRIP_IDS),
            ).alias("sample_trip_ids"),
            F.current_timestamp().alias("flagged_ts"),
        )
    )
