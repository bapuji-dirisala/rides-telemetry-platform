"""Spark Structured Streaming reader for the Firehose → S3 bronze path.

Firehose delivers JSONL (or Parquet, see Terraform) into a
Hive-partitioned S3 prefix. We tail it with Spark's cloudFiles /
file-source streaming so the equivalent of the Kafka bronze job in
Phase 3 lands in a Delta table — same schema, same table name
convention — but sourced from an AWS-managed stream.

Design decisions:

- **Reads NDJSON, not Parquet**, in the default path. Firehose can be
  configured either way; JSONL is friendlier for the local fake
  because we can hand-craft files without Arrow. The Terraform module
  defines a ``data_format_conversion_configuration`` so prod can flip
  to Parquet with no reader change beyond the ``format`` arg.
- **Partition pruning**. We register the Hive-style partition columns
  (year/month/day/hour/event_type) by setting
  ``basePath`` to the top-level prefix. Spark will push predicate
  pushdown on them for incremental backfills.
- **Idempotent writes.** Same checkpoint + Delta exactly-once semantics
  as the Kafka bronze job in Phase 3.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from rides_telemetry.bronze.schemas import (
    BRONZE_GPS_TABLE,
    BRONZE_TRIPS_TABLE,
    GPS_PAYLOAD_SCHEMA,
    TRIPS_PAYLOAD_SCHEMA,
)
from rides_telemetry.spark import LAKEHOUSE_ROOT, default_checkpoint_dir

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming import StreamingQuery


_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class KinesisBronzeConfig:
    """Points the reader at the Firehose output prefix on S3 (or local)."""

    stream: str  # "gps" | "trips"
    source_root: Path
    processing_time: str = "10 seconds"
    once: bool = False
    file_format: str = "json"

    def table_name(self) -> str:
        return BRONZE_GPS_TABLE if self.stream == "gps" else BRONZE_TRIPS_TABLE

    def event_type_filter(self) -> list[str]:
        if self.stream == "gps":
            return ["gps_ping"]
        return [
            "trip_requested",
            "trip_accepted",
            "trip_started",
            "trip_ended",
            "trip_cancelled",
        ]


def read_firehose_bronze(
    spark: SparkSession,
    config: KinesisBronzeConfig,
) -> StreamingQuery:  # pragma: no cover — needs Spark session
    """Launch (or return) a streaming query that tails the Firehose
    output and writes to the bronze Delta table.
    """
    from pyspark.sql import functions as F  # noqa: N812

    schema = GPS_PAYLOAD_SCHEMA if config.stream == "gps" else TRIPS_PAYLOAD_SCHEMA
    base_path = str(config.source_root)
    reader = (
        spark.readStream.format(config.file_format)
        .option("basePath", base_path)
        .option("recursiveFileLookup", "false")
        .schema(schema)
        .load(base_path)
    )

    # Filter down to this stream's event types; drop everything else so
    # a shared Firehose can feed both the gps and trips bronze jobs.
    reader = reader.filter(F.col("event_type").isin(config.event_type_filter()))

    reader = reader.withColumn("ingest_ts", F.current_timestamp()).withColumn(
        "source_system", F.lit("kinesis-firehose")
    )

    table_path = str(LAKEHOUSE_ROOT / "warehouse" / config.table_name())
    checkpoint = default_checkpoint_dir(f"kinesis-{config.stream}")

    writer = (
        reader.writeStream.format("delta")
        .option("checkpointLocation", str(checkpoint))
        .outputMode("append")
    )
    if config.once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=config.processing_time)
    return writer.start(table_path)
