"""AWS Kinesis Data Firehose alternative path.

Phase 7 pairs the Kafka-managed path (phases 2–6) with an AWS-native
alternative:

    producer  →  Kinesis Firehose delivery stream  →  S3 bronze  →  Delta

Firehose buffers records (size + time) and lands them as Parquet on S3
in a Hive-partitioned layout (``year=YYYY/month=MM/day=DD/hour=HH/``)
that a Databricks Auto Loader or Spark ``readStream.format("cloudFiles")``
can tail idempotently.

The producer path is defined by :class:`FirehoseConfig` + the
:func:`put_event_batch` helper. For local development we ship an
in-memory fake that writes NDJSON to disk in the same partitioned
layout so the downstream reader can be exercised Docker-free.
"""

from __future__ import annotations

from .config import FirehoseConfig, hive_partition_prefix
from .producer import FirehoseBackend, LocalFirehoseFake, put_event_batch
from .reader import KinesisBronzeConfig, read_firehose_bronze

__all__ = [
    "FirehoseBackend",
    "FirehoseConfig",
    "KinesisBronzeConfig",
    "LocalFirehoseFake",
    "hive_partition_prefix",
    "put_event_batch",
    "read_firehose_bronze",
]
