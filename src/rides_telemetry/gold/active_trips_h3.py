"""Gold — active trips per H3 hex zone.

The Phase 6 upgrade of :mod:`~rides_telemetry.gold.active_trips`.
Same 1-minute tumbling window over ``silver_gps_matched``, same
``approx_count_distinct`` on ``trip_id``, same ``outputMode=append``
semantics — but the geospatial dimension is an **H3 res-8 hex
cell** (~461 m edge, ~0.74 km² area) instead of a nearest-centroid
borough (~5 km cells).

## Why parallel marts, not a schema swap

The Phase 5a borough marts stay untouched — they're the coarse
rollup that dashboards and human operators want to eyeball
("Manhattan is at 500 active trips right now"). H3 hex marts are
the fine-grained analytical surface that surge engines and
matching services consume ("Times Sq hex is at 12 concurrent
trips right now").

Both marts live in parallel because a per-hex mart can be rolled
up to per-borough with a cheap SQL group-by, but the reverse is
lossy. Consumers that want borough get O(6) rows per window;
consumers that want hex get O(1000-2000) rows per window (only
hexes with pings emit a row).

## Borough column

Every H3 hex at res 8 is entirely within one borough, so we
project ``borough`` alongside for cheap drill-through in BI tools.
The value is derived from the same ping's ``(lat, lng)`` via the
existing :func:`borough_of_expr`, so it stays consistent with the
Phase 5a marts by construction.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812

from rides_telemetry.geo.spark import borough_of_expr, h3_index_of_expr
from rides_telemetry.gold.schemas import (
    ACTIVE_TRIPS_WINDOW,
    GOLD_ACTIVE_TRIPS_H3_TABLE,
    H3_RESOLUTION,
)
from rides_telemetry.gold.streaming import GoldStreamConfig
from rides_telemetry.silver.schemas import GPS_WATERMARK, SILVER_MATCHED_TABLE
from rides_telemetry.spark import default_checkpoint_dir

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

_log = logging.getLogger(__name__)


def stream_active_trips_h3_mart(
    spark: SparkSession,
    config: GoldStreamConfig | None = None,
    *,
    await_termination: bool = True,
) -> StreamingQuery:
    """silver_gps_matched → gold_active_trips_h3_now."""
    cfg = config or GoldStreamConfig()

    source_path = (cfg.warehouse_root / SILVER_MATCHED_TABLE).resolve()
    sink_path = (cfg.warehouse_root / GOLD_ACTIVE_TRIPS_H3_TABLE).resolve()
    checkpoint_path = default_checkpoint_dir("gold_active_trips_h3").resolve()
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

    gold = shape_active_trips_h3(matched)

    writer = (
        gold.writeStream.format("delta")
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
        .queryName("gold_active_trips_h3_now")
    )
    if cfg.trigger_processing_time:
        writer = writer.trigger(processingTime=cfg.trigger_processing_time)
    else:
        writer = writer.trigger(availableNow=True)

    _log.info(
        "starting gold active_trips_h3 (res=%d): %s -> %s (checkpoint=%s)",
        H3_RESOLUTION,
        source_path,
        sink_path,
        checkpoint_path,
    )
    query = writer.start(str(sink_path))
    if await_termination:
        query.awaitTermination()
    return query


def shape_active_trips_h3(matched: DataFrame) -> DataFrame:
    """Aggregate silver_gps_matched into per-hex per-minute counts.

    Broken out so the transformation can be exercised in batch
    mode from unit tests. The watermark is a no-op in batch and
    the windowed aggregation behaves identically.
    """
    return (
        matched.withWatermark("event_ts", GPS_WATERMARK)
        .filter(F.col("trip_status") == "IN_PROGRESS")
        .withColumn("h3_r8", h3_index_of_expr("lat", "lng", H3_RESOLUTION))
        .withColumn("borough", borough_of_expr("lat", "lng"))
        .filter(F.col("h3_r8").isNotNull())
        .groupBy(
            F.window(F.col("event_ts"), ACTIVE_TRIPS_WINDOW).alias("w"),
            F.col("h3_r8"),
            F.col("borough"),
        )
        .agg(F.approx_count_distinct("trip_id").alias("active_trips"))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("h3_r8"),
            F.col("borough"),
            F.col("active_trips").cast("int").alias("active_trips"),
            F.current_timestamp().alias("gold_processed_ts"),
        )
    )
