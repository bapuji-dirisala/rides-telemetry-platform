"""Spark-mode bronze streaming test.

Marked ``spark`` so it's skipped in the default pytest run — Spark
startup is slow (~15 s) and requires a JDK. Run explicitly with:

.. code-block:: bash

    pytest -m spark tests/bronze/

The test drives the transformation logic directly (no real Kafka),
which is what ``rides_telemetry.bronze.streaming._shape_bronze`` was
factored out for. Real Kafka + Delta wiring is covered by the
producer→bronze end-to-end run documented in the phase doc.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from rides_telemetry.bronze.schemas import GPS_PAYLOAD_SCHEMA, TRIPS_PAYLOAD_SCHEMA
from rides_telemetry.bronze.streaming import _shape_bronze
from rides_telemetry.spark import SparkSessionConfig, build_spark_session

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    # One SparkSession per test module — reused across tests to avoid
    # paying the ~15 s startup cost more than once.
    return build_spark_session(
        SparkSessionConfig(
            app_name="rides-bronze-tests",
            log_level="ERROR",
            shuffle_partitions=2,
        )
    )


def _kafka_row(spark: SparkSession, topic: str, key: str, value: dict) -> dict:
    """Build a dict matching Spark's Kafka source schema so we can feed it in."""
    import datetime as _dt

    return {
        "topic": topic,
        "partition": 0,
        "offset": 0,
        "timestamp": _dt.datetime(2024, 1, 15, 8, 0, tzinfo=_dt.UTC),
        "timestampType": 0,
        "key": key.encode(),
        "value": json.dumps(value).encode(),
        "headers": None,
    }


class TestShapingLogic:
    def test_gps_payload_is_parsed_into_columns(self, spark: SparkSession) -> None:
        row = _kafka_row(
            spark,
            topic="rides.gps.v1",
            key="ab9e3167-1d9a-6927-275a-d2a1217caa8c",
            value={
                "event_id": "44444444-4444-4444-4444-444444444444",
                "event_ts": "2024-01-15T08:00:00Z",
                "event_type": "gps_ping",
                "trip_id": "ab9e3167-1d9a-6927-275a-d2a1217caa8c",
                "driver_id": "39c93a53-9209-69d7-e57f-3fa566366f7b",
                "lat": 40.75,
                "lng": -73.98,
                "speed_kmh": 25.0,
                "heading_deg": 90.0,
                "accuracy_m": 5.0,
            },
        )
        # We construct a batch DataFrame matching the Kafka source's
        # schema — the shaping logic doesn't care that the source is
        # streaming.
        from pyspark.sql.types import (
            BinaryType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        kafka_schema = StructType(
            [
                StructField("topic", StringType(), False),
                StructField("partition", IntegerType(), False),
                StructField("offset", LongType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("timestampType", IntegerType(), False),
                StructField("key", BinaryType(), True),
                StructField("value", BinaryType(), True),
                StructField("headers", StringType(), True),
            ]
        )
        df = spark.createDataFrame([row], schema=kafka_schema)
        bronze = _shape_bronze(df, GPS_PAYLOAD_SCHEMA)

        results = bronze.collect()
        assert len(results) == 1
        r = results[0].asDict()

        assert r["trip_id"] == "ab9e3167-1d9a-6927-275a-d2a1217caa8c"
        assert r["lat"] == 40.75
        assert r["lng"] == -73.98
        assert r["speed_kmh"] == 25.0
        assert r["kafka_topic"] == "rides.gps.v1"
        assert r["kafka_partition"] == 0
        assert r["kafka_offset"] == 0
        assert r["kafka_key"] == "ab9e3167-1d9a-6927-275a-d2a1217caa8c"
        assert r["ingest_ts"] is not None
        # raw_json preserves the original wire payload.
        assert json.loads(r["raw_json"])["event_type"] == "gps_ping"

    def test_trips_payload_handles_optional_lifecycle_fields(self, spark: SparkSession) -> None:
        # trip_requested has no driver_id and no distance/fare — those
        # columns must be null, not raise.
        row = _kafka_row(
            spark,
            topic="rides.trips.v1",
            key="ab9e3167-1d9a-6927-275a-d2a1217caa8c",
            value={
                "event_id": "55555555-5555-5555-5555-555555555555",
                "event_ts": "2024-01-15T07:58:50Z",
                "event_type": "trip_requested",
                "trip_id": "ab9e3167-1d9a-6927-275a-d2a1217caa8c",
                "rider_id": "7e990104-c226-b87d-9273-52377e182b33",
                "driver_id": None,
                "pickup_lat": 40.75,
                "pickup_lng": -73.98,
                "dropoff_lat": 40.68,
                "dropoff_lng": -73.94,
            },
        )
        from pyspark.sql.types import (
            BinaryType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        kafka_schema = StructType(
            [
                StructField("topic", StringType(), False),
                StructField("partition", IntegerType(), False),
                StructField("offset", LongType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("timestampType", IntegerType(), False),
                StructField("key", BinaryType(), True),
                StructField("value", BinaryType(), True),
                StructField("headers", StringType(), True),
            ]
        )
        df = spark.createDataFrame([row], schema=kafka_schema)
        bronze = _shape_bronze(df, TRIPS_PAYLOAD_SCHEMA)

        r = bronze.collect()[0].asDict()
        assert r["event_type"] == "trip_requested"
        assert r["driver_id"] is None
        assert r["trip_distance_km"] is None
        assert r["fare_amount_usd"] is None
        assert r["cancelled_by"] is None


class TestWarehousePathing:
    """Every table location must be inside the lakehouse dir — never scattered."""

    def test_default_warehouse_is_inside_repo(self, tmp_path: Path) -> None:
        # Sanity check the config; the streaming job resolves the path
        # via ``warehouse_root / table_name``.
        from rides_telemetry.bronze.streaming import BronzeStreamConfig

        cfg = BronzeStreamConfig()
        assert cfg.warehouse_root.parts[-2:] == ("lakehouse", "warehouse")
