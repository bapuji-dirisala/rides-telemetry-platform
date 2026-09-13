"""Spark schemas for the bronze layer.

Every field here mirrors a field on the phase-1 pydantic event models.
The mapping is exercised by a consistency test in ``tests/bronze/`` so
adding a field to a pydantic model without updating the Spark schema
(or vice versa) fails CI loudly.

Why hand-write instead of deriving from pydantic?

- Spark type choices deserve human judgement (``timestamp`` vs
  ``timestamp_ntz``, ``string`` vs ``binary`` for UUIDs, precision on
  floats, nullable-ness). Auto-derivation gives you *a* schema, not
  necessarily *the right* schema.
- Reading the schema tells a reviewer immediately what bronze looks
  like, without cross-referencing pydantic code.
- When the pydantic models eventually get code-genned from an Avro
  IDL (probably around phase 7 with Schema Registry), the Spark
  schemas here become the source of truth for the wire format.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Managed table names live here so producers, consumers, and docs
# reference exactly one string.
BRONZE_TRIPS_TABLE = "bronze_rides_lifecycle"
BRONZE_GPS_TABLE = "bronze_rides_gps"


# -------------------------------------------------------------------------
# Payload schemas — what the pydantic models look like on the wire.
# These are the schemas passed to ``from_json`` when parsing the Kafka
# ``value`` column.
# -------------------------------------------------------------------------

# Fields common to every trip lifecycle event (request / accept /
# start / end / cancel). Fields that only some subtypes carry
# (``driver_id``, ``trip_distance_km``, ``fare_amount_usd``,
# ``cancelled_by``) are all nullable in bronze; silver enforces the
# per-event-type invariants.
TRIPS_PAYLOAD_SCHEMA: StructType = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("trip_id", StringType(), nullable=False),
        StructField("rider_id", StringType(), nullable=False),
        StructField("driver_id", StringType(), nullable=True),
        StructField("pickup_lat", DoubleType(), nullable=False),
        StructField("pickup_lng", DoubleType(), nullable=False),
        StructField("dropoff_lat", DoubleType(), nullable=False),
        StructField("dropoff_lng", DoubleType(), nullable=False),
        # trip_ended-only:
        StructField("trip_distance_km", DoubleType(), nullable=True),
        StructField("fare_amount_usd", DoubleType(), nullable=True),
        # trip_cancelled-only:
        StructField("cancelled_by", StringType(), nullable=True),
    ]
)


GPS_PAYLOAD_SCHEMA: StructType = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("trip_id", StringType(), nullable=False),
        StructField("driver_id", StringType(), nullable=False),
        StructField("lat", DoubleType(), nullable=False),
        StructField("lng", DoubleType(), nullable=False),
        StructField("speed_kmh", DoubleType(), nullable=True),
        StructField("heading_deg", DoubleType(), nullable=True),
        StructField("accuracy_m", DoubleType(), nullable=True),
    ]
)


# -------------------------------------------------------------------------
# Kafka envelope — bronze rows carry the raw payload PLUS these
# operational columns for debugging and reprocessing.
# -------------------------------------------------------------------------

KAFKA_ENVELOPE_FIELDS: list[StructField] = [
    StructField("kafka_topic", StringType(), nullable=False),
    StructField("kafka_partition", IntegerType(), nullable=False),
    StructField("kafka_offset", LongType(), nullable=False),
    StructField("kafka_timestamp", TimestampType(), nullable=False),
    StructField("kafka_key", StringType(), nullable=True),
    StructField("ingest_ts", TimestampType(), nullable=False),
    StructField("raw_json", StringType(), nullable=False),
]


def bronze_table_schema(payload: StructType) -> StructType:
    """Compose the final bronze table schema: envelope + parsed payload fields."""
    return StructType(list(payload.fields) + KAFKA_ENVELOPE_FIELDS)


BRONZE_TRIPS_SCHEMA: StructType = bronze_table_schema(TRIPS_PAYLOAD_SCHEMA)
BRONZE_GPS_SCHEMA: StructType = bronze_table_schema(GPS_PAYLOAD_SCHEMA)
