"""Gold — demand & supply per H3 hex zone (5-min windows).

The Phase 6 upgrade of :mod:`~rides_telemetry.gold.demand`. Same
5-min tumbling window, same three distinct counts (pings, trips,
drivers) — but the geospatial dimension is an H3 res-8 cell.

Consumers get a **street-scale surge signal**: instead of "Manhattan
has 15 trips per driver right now" (borough-level, meaningless
because Manhattan is huge), they get "hex ``882a100d67fffff`` (Times
Sq) has 4.5 trips per driver right now" — the actual grain a
matching engine needs to place a surge multiplier.

See the sibling module docs for the shared design decisions
(``approx_count_distinct`` in streaming, append output mode with a
watermark, why the borough column travels alongside).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812

from rides_telemetry.geo.spark import borough_of_expr, h3_index_of_expr
from rides_telemetry.gold.schemas import (
    DEMAND_WATERMARK,
    DEMAND_WINDOW,
    GOLD_DEMAND_H3_TABLE,
    H3_RESOLUTION,
)
from rides_telemetry.gold.streaming import GoldStreamConfig
from rides_telemetry.silver.schemas import SILVER_MATCHED_TABLE
from rides_telemetry.spark import default_checkpoint_dir

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

_log = logging.getLogger(__name__)


def stream_demand_h3_mart(
    spark: SparkSession,
    config: GoldStreamConfig | None = None,
    *,
    await_termination: bool = True,
) -> StreamingQuery:
    """silver_gps_matched → gold_demand_by_h3_5min."""
    cfg = config or GoldStreamConfig()

    source_path = (cfg.warehouse_root / SILVER_MATCHED_TABLE).resolve()
    sink_path = (cfg.warehouse_root / GOLD_DEMAND_H3_TABLE).resolve()
    checkpoint_path = default_checkpoint_dir("gold_demand_h3").resolve()
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

    gold = shape_demand_h3(matched)

    writer = (
        gold.writeStream.format("delta")
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
        .queryName("gold_demand_by_h3_5min")
    )
    if cfg.trigger_processing_time:
        writer = writer.trigger(processingTime=cfg.trigger_processing_time)
    else:
        writer = writer.trigger(availableNow=True)

    _log.info(
        "starting gold demand_h3 (res=%d): %s -> %s (checkpoint=%s)",
        H3_RESOLUTION,
        source_path,
        sink_path,
        checkpoint_path,
    )
    query = writer.start(str(sink_path))
    if await_termination:
        query.awaitTermination()
    return query


def shape_demand_h3(matched: DataFrame) -> DataFrame:
    """Aggregate matched pings into per-hex demand/supply signal."""
    return (
        matched.withWatermark("event_ts", DEMAND_WATERMARK)
        .withColumn("h3_r8", h3_index_of_expr("lat", "lng", H3_RESOLUTION))
        .withColumn("borough", borough_of_expr("lat", "lng"))
        .filter(F.col("h3_r8").isNotNull())
        .groupBy(
            F.window(F.col("event_ts"), DEMAND_WINDOW).alias("w"),
            F.col("h3_r8"),
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
            F.col("h3_r8"),
            F.col("borough"),
            F.col("ping_count"),
            F.col("distinct_trips"),
            F.col("distinct_drivers"),
            F.current_timestamp().alias("gold_processed_ts"),
        )
    )
