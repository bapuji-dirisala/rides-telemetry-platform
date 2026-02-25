"""Bronze layer — raw Kafka events landed in Delta.

Bronze is the *thinnest* possible transformation of the wire format:

- The full JSON payload is preserved as-is (``raw_json`` column) so we
  can always reprocess with a corrected schema without re-reading
  Kafka.
- The parsed fields are materialised into typed columns for query
  ergonomics.
- Kafka metadata (``kafka_topic``, ``kafka_partition``, ``kafka_offset``,
  ``kafka_timestamp``) is preserved for debugging and replay.
- ``ingest_ts`` records when the row landed in bronze — distinct from
  ``event_ts`` (business time) and ``kafka_timestamp`` (broker append time).

No dedup, no watermarks, no joins — those all belong in silver
(phase 4). Bronze's only job is to durably capture whatever landed on
the wire.
"""

from __future__ import annotations

from rides_telemetry.bronze.schemas import (
    BRONZE_GPS_TABLE,
    BRONZE_TRIPS_TABLE,
    GPS_PAYLOAD_SCHEMA,
    TRIPS_PAYLOAD_SCHEMA,
)
from rides_telemetry.bronze.streaming import (
    BronzeStreamConfig,
    stream_gps_to_bronze,
    stream_trips_to_bronze,
)

__all__ = [
    "BRONZE_GPS_TABLE",
    "BRONZE_TRIPS_TABLE",
    "GPS_PAYLOAD_SCHEMA",
    "TRIPS_PAYLOAD_SCHEMA",
    "BronzeStreamConfig",
    "stream_gps_to_bronze",
    "stream_trips_to_bronze",
]
