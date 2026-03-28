"""Firehose configuration + S3 layout helpers.

Kept pure (no AWS client) so unit tests and the Terraform module share
the same partition-prefix convention.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True)
class FirehoseConfig:
    """Where Firehose writes + how aggressively it batches.

    - ``buffer_size_mb`` and ``buffer_interval_seconds`` must match the
      Terraform ``extended_s3_configuration`` block exactly, otherwise
      the Delta reader's partition pruning predicate is wrong.
    - ``s3_prefix`` is the top-level bronze folder. Firehose then
      appends the Hive-style ``year=/month=/day=/hour=/`` suffix.
    """

    delivery_stream_name: str
    s3_bucket: str
    s3_prefix: str = "bronze/rides"
    region: str = "us-east-1"
    buffer_size_mb: int = 5
    buffer_interval_seconds: int = 60
    compression: str = "GZIP"
    error_output_prefix: str = "bronze/_errors/rides"
    extra_tags: dict[str, str] = field(default_factory=dict)


def hive_partition_prefix(
    base_prefix: str,
    event_time: datetime,
    event_type: str,
) -> str:
    """Return the Hive-partitioned S3 key prefix for a given event time.

    Separate function so the local emulator writes to the same path
    layout that Firehose's ``extended_s3_configuration`` emits:

        <base_prefix>/event_type=<type>/year=YYYY/month=MM/day=DD/hour=HH/
    """
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=UTC)
    utc = event_time.astimezone(UTC)
    return (
        f"{base_prefix.rstrip('/')}"
        f"/event_type={event_type}"
        f"/year={utc.year:04d}"
        f"/month={utc.month:02d}"
        f"/day={utc.day:02d}"
        f"/hour={utc.hour:02d}/"
    )
