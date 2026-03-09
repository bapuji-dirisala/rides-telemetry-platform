"""Spark-mode tests for gold mart shaping + borough attribution.

Marked ``spark`` so it's skipped by default. Uses a shared module-
scoped SparkSession to keep JVM startup cost bounded. Calls the
pure-DataFrame helpers (``shape_active_trips``, ``shape_demand``,
``borough_of_expr``) against tiny handcrafted DataFrames — the
streaming machinery itself is exercised by the end-to-end run
documented in the phase doc.

## A note on ``approx_count_distinct``

The mart shape functions use ``approx_count_distinct`` because
Spark Structured Streaming forbids exact ``countDistinct`` inside
a streaming aggregation. These tests exercise the shape functions
in **batch** mode where Spark would allow exact counts, but we
keep the same call to avoid a code split. HyperLogLog with the
default relative error (~5%) is empirically exact for cardinalities
under ~100 — our test inputs have at most a handful of unique
values, so the assertions below use exact equality safely.
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

import pytest

from rides_telemetry.geo.spark import borough_of_expr
from rides_telemetry.gold.active_trips import shape_active_trips
from rides_telemetry.gold.demand import shape_demand
from rides_telemetry.gold.schemas import (
    GOLD_ACTIVE_TRIPS_SCHEMA,
    GOLD_DEMAND_SCHEMA,
)
from rides_telemetry.silver.schemas import SILVER_MATCHED_SCHEMA
from rides_telemetry.spark import SparkSessionConfig, build_spark_session

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return build_spark_session(
        SparkSessionConfig(
            app_name="rides-gold-tests",
            log_level="ERROR",
            shuffle_partitions=2,
        )
    )


# Naive datetimes to sidestep JVM-timezone round-trip (see the phase-4a
# gotcha in ``docs/phase-04-silver-streaming.md``).
_T0 = dt.datetime(2024, 1, 15, 8, 0)


def _matched_row(
    event_id: str,
    trip_id: str,
    driver_id: str,
    event_ts: dt.datetime,
    lat: float,
    lng: float,
    trip_status: str = "IN_PROGRESS",
    rider_id: str = "r1",
) -> dict:
    """Row matching SILVER_MATCHED_SCHEMA."""
    return {
        "event_id": event_id,
        "event_ts": event_ts,
        "trip_id": trip_id,
        "driver_id": driver_id,
        "lat": lat,
        "lng": lng,
        "speed_kmh": 25.0,
        "heading_deg": 90.0,
        "accuracy_m": 5.0,
        "rider_id": rider_id,
        "trip_status": trip_status,
        "trip_requested_ts": event_ts - dt.timedelta(minutes=5),
        "trip_started_ts": event_ts - dt.timedelta(minutes=2),
        "trip_ended_ts": None,
        "pickup_lat": lat,
        "pickup_lng": lng,
        "dropoff_lat": 40.68,
        "dropoff_lng": -73.94,
        "ingest_ts": event_ts,
        "silver_processed_ts": event_ts,
    }


# -------------------------------------------------------------------------
# Borough attribution
# -------------------------------------------------------------------------


class TestBoroughAttribution:
    def test_manhattan_centroid_maps_to_manhattan(self, spark: SparkSession) -> None:
        # Centroids from geo/nyc.py — closest point to itself is itself.
        df = spark.createDataFrame([(40.7831, -73.9712)], schema="lat double, lng double")
        result = df.withColumn("b", borough_of_expr("lat", "lng")).collect()[0]["b"]
        assert result == "Manhattan"

    def test_brooklyn_centroid_maps_to_brooklyn(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(40.6782, -73.9442)], schema="lat double, lng double")
        assert df.withColumn("b", borough_of_expr("lat", "lng")).collect()[0]["b"] == "Brooklyn"

    def test_ewr_airport_maps_to_ewr(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(40.6895, -74.1745)], schema="lat double, lng double")
        assert df.withColumn("b", borough_of_expr("lat", "lng")).collect()[0]["b"] == "EWR"

    def test_outside_nyc_returns_null(self, spark: SparkSession) -> None:
        # San Francisco coordinates fall outside NYC_BBOX.
        df = spark.createDataFrame([(37.77, -122.42)], schema="lat double, lng double")
        assert df.withColumn("b", borough_of_expr("lat", "lng")).collect()[0]["b"] is None

    def test_all_nyc_pings_get_a_borough(self, spark: SparkSession) -> None:
        # A spread of coordinates all inside NYC — every one must resolve.
        pts = [
            (40.75, -73.98),  # near Times Square, Manhattan
            (40.65, -73.95),  # Brooklyn
            (40.75, -73.87),  # Queens
            (40.85, -73.87),  # Bronx
            (40.60, -74.15),  # Staten Island
        ]
        df = spark.createDataFrame(pts, schema="lat double, lng double")
        boroughs = [r["b"] for r in df.withColumn("b", borough_of_expr("lat", "lng")).collect()]
        assert all(b is not None for b in boroughs), f"got NULL boroughs: {boroughs}"


# -------------------------------------------------------------------------
# gold_active_trips_now
# -------------------------------------------------------------------------


class TestActiveTripsMart:
    def test_output_schema_matches_spec(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_matched_row("e1", "A", "d1", _T0, 40.75, -73.98)],
            schema=SILVER_MATCHED_SCHEMA,
        )
        gold = shape_active_trips(df)
        expected = [f.name for f in GOLD_ACTIVE_TRIPS_SCHEMA.fields]
        assert gold.schema.fieldNames() == expected

    def test_counts_distinct_trips_per_borough(self, spark: SparkSession) -> None:
        # 5 pings from 3 different trips, all in Manhattan, same 1-min window.
        rows = [
            _matched_row("e1", "A", "d1", _T0, 40.7831, -73.9712),
            _matched_row("e2", "A", "d1", _T0 + dt.timedelta(seconds=10), 40.7831, -73.9712),
            _matched_row("e3", "B", "d2", _T0 + dt.timedelta(seconds=20), 40.7831, -73.9712),
            _matched_row("e4", "C", "d3", _T0 + dt.timedelta(seconds=30), 40.7831, -73.9712),
            # trip A duplicated — should NOT bump the distinct count
            _matched_row("e5", "A", "d1", _T0 + dt.timedelta(seconds=40), 40.7831, -73.9712),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        gold_rows = shape_active_trips(df).collect()
        assert len(gold_rows) == 1
        r = gold_rows[0].asDict()
        assert r["borough"] == "Manhattan"
        assert r["active_trips"] == 3  # A, B, C — deduped

    def test_completed_trips_excluded(self, spark: SparkSession) -> None:
        rows = [
            _matched_row("e1", "A", "d1", _T0, 40.7831, -73.9712, trip_status="IN_PROGRESS"),
            _matched_row("e2", "B", "d2", _T0, 40.7831, -73.9712, trip_status="COMPLETED"),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        gold_rows = shape_active_trips(df).collect()
        assert len(gold_rows) == 1
        assert gold_rows[0]["active_trips"] == 1

    def test_separate_boroughs_produce_separate_rows(self, spark: SparkSession) -> None:
        rows = [
            _matched_row("m", "A", "d1", _T0, 40.7831, -73.9712),  # Manhattan
            _matched_row("b", "B", "d2", _T0, 40.6782, -73.9442),  # Brooklyn
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        gold_rows = {r["borough"]: r["active_trips"] for r in shape_active_trips(df).collect()}
        assert gold_rows == {"Manhattan": 1, "Brooklyn": 1}


# -------------------------------------------------------------------------
# gold_demand_by_borough_5min
# -------------------------------------------------------------------------


class TestDemandMart:
    def test_output_schema_matches_spec(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_matched_row("e1", "A", "d1", _T0, 40.75, -73.98)],
            schema=SILVER_MATCHED_SCHEMA,
        )
        gold = shape_demand(df)
        expected = [f.name for f in GOLD_DEMAND_SCHEMA.fields]
        assert gold.schema.fieldNames() == expected

    def test_ping_count_uses_distinct_event_id(self, spark: SparkSession) -> None:
        # 3 unique pings + 1 duplicate event_id => ping_count = 3.
        rows = [
            _matched_row("e1", "A", "d1", _T0, 40.7831, -73.9712),
            _matched_row("e2", "A", "d1", _T0 + dt.timedelta(seconds=10), 40.7831, -73.9712),
            _matched_row("e3", "B", "d2", _T0 + dt.timedelta(seconds=20), 40.7831, -73.9712),
            _matched_row("e1", "A", "d1", _T0, 40.7831, -73.9712),  # duplicate
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        r = shape_demand(df).collect()[0].asDict()
        assert r["ping_count"] == 3
        assert r["distinct_trips"] == 2
        assert r["distinct_drivers"] == 2

    def test_multiple_windows_per_borough(self, spark: SparkSession) -> None:
        # Two 5-min windows in Manhattan; expect two rows.
        rows = [
            _matched_row("e1", "A", "d1", _T0, 40.7831, -73.9712),
            _matched_row("e2", "B", "d2", _T0 + dt.timedelta(minutes=6), 40.7831, -73.9712),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        gold_rows = shape_demand(df).collect()
        assert len(gold_rows) == 2
        # Both are Manhattan; different windows.
        boroughs = {r["borough"] for r in gold_rows}
        assert boroughs == {"Manhattan"}

    def test_windows_do_not_span_boroughs(self, spark: SparkSession) -> None:
        # Same window, three different boroughs → three rows.
        rows = [
            _matched_row("m", "A", "d1", _T0, 40.7831, -73.9712),
            _matched_row("b", "B", "d2", _T0, 40.6782, -73.9442),
            _matched_row("q", "C", "d3", _T0, 40.7282, -73.7949),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        gold_rows = shape_demand(df).collect()
        assert len(gold_rows) == 3
        by_borough = {r["borough"]: r["distinct_trips"] for r in gold_rows}
        assert by_borough == {"Manhattan": 1, "Brooklyn": 1, "Queens": 1}
