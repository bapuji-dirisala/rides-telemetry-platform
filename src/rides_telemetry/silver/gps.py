"""Silver GPS streaming — bronze_rides_gps → silver_rides_gps.

Pipeline shape:

1. Read the bronze GPS Delta table as a stream.
2. Attach a watermark on ``event_ts`` (business time, not ingest
   time — replays should be idempotent).
3. Deduplicate by ``event_id``. The watermark bounds the state
   store: any duplicate that arrives more than
   :data:`~rides_telemetry.silver.schemas.GPS_WATERMARK` after the
   original is *not* deduped (it would land as a second row), but
   the state doesn't grow without bound.
4. Filter with the composite quality predicate from
   :mod:`~rides_telemetry.silver.quality`.
5. Project to the silver schema (drop Kafka envelope + raw_json,
   add ``silver_processed_ts``).
6. Append to the silver Delta table with its own checkpoint.

Why append-only (no MERGE)?

    Every silver GPS row is a distinct observation; there's nothing
    to update. Append + dedup gives exactly-once semantics at rest
    without the MERGE overhead. Trip-fact rows *do* need MERGE — see
    ``trips.py``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812

from rides_telemetry.bronze.schemas import BRONZE_GPS_TABLE
from rides_telemetry.silver.quality import gps_ping_is_valid
from rides_telemetry.silver.schemas import (
    GPS_WATERMARK,
    SILVER_GPS_TABLE,
)
from rides_telemetry.silver.streaming import SilverStreamConfig
from rides_telemetry.spark import default_checkpoint_dir

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

_log = logging.getLogger(__name__)


def stream_gps_to_silver(
    spark: SparkSession,
    config: SilverStreamConfig | None = None,
    *,
    await_termination: bool = True,
) -> StreamingQuery:
    """Bronze GPS → silver GPS. Returns the running StreamingQuery."""
    cfg = config or SilverStreamConfig()

    bronze_path = (cfg.warehouse_root / BRONZE_GPS_TABLE).resolve()
    silver_path = (cfg.warehouse_root / SILVER_GPS_TABLE).resolve()
    checkpoint_path = default_checkpoint_dir("silver_gps").resolve()
    checkpoint_path.mkdir(parents=True, exist_ok=True)

    if not bronze_path.exists():
        raise FileNotFoundError(
            f"bronze GPS table not found at {bronze_path}. "
            f"Run `python -m rides_telemetry.bronze --topic gps` first."
        )

    bronze = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", str(cfg.max_files_per_trigger))
        .load(str(bronze_path))
    )

    silver = shape_silver_gps(bronze)

    writer = (
        silver.writeStream.format("delta")
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
        .queryName("silver_rides_gps")
    )
    if cfg.trigger_processing_time:
        writer = writer.trigger(processingTime=cfg.trigger_processing_time)
    else:
        writer = writer.trigger(availableNow=True)

    _log.info(
        "starting silver GPS stream: %s -> %s (checkpoint=%s)",
        bronze_path,
        silver_path,
        checkpoint_path,
    )
    query = writer.start(str(silver_path))
    if await_termination:
        query.awaitTermination()
    return query


def shape_silver_gps(bronze: DataFrame) -> DataFrame:
    """Transform bronze GPS rows into the silver GPS schema.

    Broken out from :func:`stream_gps_to_silver` so it can be tested
    against a static DataFrame in unit tests (no streaming machinery
    needed). The batch- and stream-mode plans differ, but this
    transformation is identical in both — Structured Streaming
    inherits the same DataFrame API.
    """
    return (
        bronze
        # Watermark FIRST — dropDuplicates uses it to bound state.
        # Bronze already carries ``event_ts`` (business time), which
        # is exactly what we want to watermark on for replay-safety.
        .withWatermark("event_ts", GPS_WATERMARK)
        .dropDuplicates(["event_id"])
        .filter(gps_ping_is_valid())
        .select(
            F.col("event_id"),
            F.col("event_ts"),
            F.col("trip_id"),
            F.col("driver_id"),
            F.col("lat"),
            F.col("lng"),
            F.col("speed_kmh"),
            F.col("heading_deg"),
            F.col("accuracy_m"),
            F.col("ingest_ts"),
            F.current_timestamp().alias("silver_processed_ts"),
        )
    )
