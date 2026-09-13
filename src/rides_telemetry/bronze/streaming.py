"""Bronze streaming jobs — Kafka → Delta.

Two symmetric jobs, one per topic. Each:

1. Subscribes to a Kafka topic via Spark Structured Streaming.
2. Parses the JSON value into typed columns using the schemas in
   :mod:`~rides_telemetry.bronze.schemas`, keeping the raw JSON in a
   ``raw_json`` column for lossless replay.
3. Enriches every row with Kafka metadata (topic, partition, offset,
   broker timestamp) and an ``ingest_ts``.
4. Writes to a Delta table under the local lakehouse warehouse with
   a checkpoint directory that guarantees exactly-once processing.

Design decisions:

- **``failOnDataLoss=false`` is *not* set.** We want the job to fail
  loud if the checkpoint's committed offsets no longer exist in Kafka
  (retention lapse, topic rebuild). Silently skipping data is worse
  than dying.
- **Trigger defaults to ``processingTime="5 seconds"``.** Enough for
  a laptop-friendly cadence without turning bronze into a batch job.
  The CLI can flip to ``Trigger.AvailableNow`` for a bounded run.
- **``mergeSchema=true`` is *not* enabled.** Schema drift in bronze
  must be explicit — adding a field requires updating the payload
  schema in :mod:`schemas` and shipping it. Silent schema evolution
  is a fast way to silently corrupt downstream marts.
- **Delta table location is derived from the warehouse dir**, not
  hard-coded. Same code lands data in a Databricks Unity Catalog
  external location in phase 10 by changing the Spark session config.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import StructType

from rides_telemetry.bronze.schemas import (
    BRONZE_GPS_TABLE,
    BRONZE_TRIPS_TABLE,
    GPS_PAYLOAD_SCHEMA,
    TRIPS_PAYLOAD_SCHEMA,
)
from rides_telemetry.spark import LAKEHOUSE_ROOT, default_checkpoint_dir

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BronzeStreamConfig:
    """Runtime knobs for the bronze streaming jobs."""

    bootstrap_servers: str = "localhost:19092"
    """Same default as the Kafka producer — targets local Redpanda."""

    starting_offsets: str = "earliest"
    """``earliest`` on a fresh run; the checkpoint overrides this on restart."""

    trigger_processing_time: str | None = "5 seconds"
    """Continuous-ish micro-batches. Set to ``None`` to use ``availableNow``."""

    max_offsets_per_trigger: int = 100_000
    """Cap on messages per micro-batch. Keeps the first backfill bounded."""

    warehouse_root: Path = field(default_factory=lambda: LAKEHOUSE_ROOT / "warehouse")
    """Where the Delta table directories live on disk."""

    extra_kafka_options: dict[str, str] = field(default_factory=dict)
    """Escape hatch for SASL / SSL / consumer group overrides."""


# -------------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------------


def stream_trips_to_bronze(
    spark: SparkSession,
    config: BronzeStreamConfig | None = None,
    *,
    topic: str = "rides.trips.v1",
    await_termination: bool = True,
) -> StreamingQuery:
    """Kafka ``rides.trips.v1`` → Delta ``bronze_rides_lifecycle``."""
    return _run(
        spark,
        config or BronzeStreamConfig(),
        topic=topic,
        payload_schema=TRIPS_PAYLOAD_SCHEMA,
        table_name=BRONZE_TRIPS_TABLE,
        checkpoint_name="bronze_trips",
        await_termination=await_termination,
    )


def stream_gps_to_bronze(
    spark: SparkSession,
    config: BronzeStreamConfig | None = None,
    *,
    topic: str = "rides.gps.v1",
    await_termination: bool = True,
) -> StreamingQuery:
    """Kafka ``rides.gps.v1`` → Delta ``bronze_rides_gps``."""
    return _run(
        spark,
        config or BronzeStreamConfig(),
        topic=topic,
        payload_schema=GPS_PAYLOAD_SCHEMA,
        table_name=BRONZE_GPS_TABLE,
        checkpoint_name="bronze_gps",
        await_termination=await_termination,
    )


# -------------------------------------------------------------------------
# Internals
# -------------------------------------------------------------------------


def _run(  # noqa: PLR0913 — one place to configure, deliberately verbose
    spark: SparkSession,
    config: BronzeStreamConfig,
    *,
    topic: str,
    payload_schema: StructType,
    table_name: str,
    checkpoint_name: str,
    await_termination: bool,
) -> StreamingQuery:
    raw = _read_kafka(spark, config, topic)
    bronze = _shape_bronze(raw, payload_schema)

    table_path = (config.warehouse_root / table_name).resolve()
    checkpoint_path = default_checkpoint_dir(checkpoint_name).resolve()
    checkpoint_path.mkdir(parents=True, exist_ok=True)

    writer = (
        bronze.writeStream.format("delta")
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
        # A named query makes it easy to find in the Spark UI at
        # http://localhost:4040/StreamingQuery/
        .queryName(f"bronze_{topic.replace('.', '_')}")
    )
    if config.trigger_processing_time:
        writer = writer.trigger(processingTime=config.trigger_processing_time)
    else:
        writer = writer.trigger(availableNow=True)

    _log.info(
        "starting bronze stream: topic=%s -> %s (checkpoint=%s)",
        topic,
        table_path,
        checkpoint_path,
    )
    query = writer.start(str(table_path))
    if await_termination:
        query.awaitTermination()
    return query


def _read_kafka(spark: SparkSession, config: BronzeStreamConfig, topic: str) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", config.starting_offsets)
        .option("maxOffsetsPerTrigger", str(config.max_offsets_per_trigger))
        # ``includeHeaders`` is off — we don't ship any headers yet.
    )
    for k, v in config.extra_kafka_options.items():
        reader = reader.option(k, v)
    return reader.load()


def _shape_bronze(raw: DataFrame, payload_schema: StructType) -> DataFrame:
    """Parse the JSON value and stitch on Kafka metadata + ingest_ts.

    Column order matches :func:`~rides_telemetry.bronze.schemas.bronze_table_schema`
    so downstream code can rely on positional selection where useful.
    """
    parsed = raw.select(
        # Parsed payload fields (all nullable-safe via ``from_json``)
        F.from_json(F.col("value").cast("string"), payload_schema).alias("payload"),
        # Kafka envelope
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        # Raw payload preserved for lossless replay
        F.col("value").cast("string").alias("raw_json"),
    )
    payload_cols = [F.col(f"payload.{f.name}").alias(f.name) for f in payload_schema.fields]
    return parsed.select(
        *payload_cols,
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("kafka_key"),
        F.current_timestamp().alias("ingest_ts"),
        F.col("raw_json"),
    )
