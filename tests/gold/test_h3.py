"""Spark-mode tests for Phase 6 H3 helper + H3 marts.

Marked ``spark`` so they run only on demand. Uses a shared
module-scoped SparkSession, driving the pure shape functions
(``shape_active_trips_h3``, ``shape_demand_h3``, ``h3_index_of_expr``)
against handcrafted DataFrames. The streaming plumbing is exercised
by the end-to-end run documented in ``docs/phase-06-h3-hex-zones.md``.
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

import h3
import pytest

from rides_telemetry.geo.spark import h3_index_of_expr
from rides_telemetry.gold.active_trips_h3 import shape_active_trips_h3
from rides_telemetry.gold.demand_h3 import shape_demand_h3
from rides_telemetry.gold.schemas import (
    GOLD_ACTIVE_TRIPS_H3_SCHEMA,
    GOLD_DEMAND_H3_SCHEMA,
    H3_RESOLUTION,
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
            app_name="rides-gold-h3-tests",
            log_level="ERROR",
            shuffle_partitions=2,
        )
    )


# Naive datetimes — same reasoning as the phase 5 spark tests.
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
# h3_index_of_expr — the pandas_udf itself.
# -------------------------------------------------------------------------


class TestH3IndexOfExpr:
    """Cross-check the pandas_udf against the pure-Python h3 library.

    If they disagree, either the UDF wiring is broken or we're
    building against a different h3 version than expected.
    """

    def test_times_sq_matches_python_h3(self, spark: SparkSession) -> None:
        lat, lng = 40.7580, -73.9855  # Times Square
        expected = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        df = spark.createDataFrame([(lat, lng)], schema="lat double, lng double")
        got = df.withColumn("h3", h3_index_of_expr("lat", "lng", H3_RESOLUTION)).collect()[0]["h3"]
        assert got == expected

    def test_jfk_matches_python_h3(self, spark: SparkSession) -> None:
        lat, lng = 40.6413, -73.7781  # JFK
        expected = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        df = spark.createDataFrame([(lat, lng)], schema="lat double, lng double")
        got = df.withColumn("h3", h3_index_of_expr("lat", "lng", H3_RESOLUTION)).collect()[0]["h3"]
        assert got == expected

    def test_null_lat_or_lng_propagates_null(self, spark: SparkSession) -> None:
        rows = [(None, -73.98), (40.75, None), (None, None)]
        df = spark.createDataFrame(rows, schema="lat double, lng double")
        got = df.withColumn("h3", h3_index_of_expr("lat", "lng", H3_RESOLUTION)).collect()
        assert all(r["h3"] is None for r in got), f"expected all NULL, got {got}"

    def test_nearby_pings_share_a_cell(self, spark: SparkSession) -> None:
        # Two points ~10 m apart at res 8 (461 m edges) MUST fall in
        # the same cell. This is the property that makes hex marts
        # useful: nearby pings aggregate together.
        rows = [(40.7580, -73.9855), (40.7581, -73.9856)]
        df = spark.createDataFrame(rows, schema="lat double, lng double")
        got = df.withColumn("h3", h3_index_of_expr("lat", "lng", H3_RESOLUTION)).collect()
        assert got[0]["h3"] == got[1]["h3"], (
            "nearby pings should share a res-8 cell — spot-check first"
        )

    def test_far_pings_are_in_different_cells(self, spark: SparkSession) -> None:
        # Times Sq and JFK are ~24 km apart; res-8 cells are ~0.5 km
        # edge, so no way they share a cell.
        rows = [(40.7580, -73.9855), (40.6413, -73.7781)]
        df = spark.createDataFrame(rows, schema="lat double, lng double")
        got = df.withColumn("h3", h3_index_of_expr("lat", "lng", H3_RESOLUTION)).collect()
        assert got[0]["h3"] != got[1]["h3"]

    def test_different_resolutions_produce_different_ids(self, spark: SparkSession) -> None:
        # A point's res-7 cell contains its res-8 cell — the two
        # IDs must differ.
        lat, lng = 40.7580, -73.9855
        df = spark.createDataFrame([(lat, lng)], schema="lat double, lng double")
        got = (
            df.withColumn("h3_r7", h3_index_of_expr("lat", "lng", 7))
            .withColumn("h3_r8", h3_index_of_expr("lat", "lng", 8))
            .collect()[0]
        )
        assert got["h3_r7"] != got["h3_r8"]


# -------------------------------------------------------------------------
# gold_active_trips_h3
# -------------------------------------------------------------------------


class TestActiveTripsH3Mart:
    def test_output_schema_matches_spec(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_matched_row("e1", "A", "d1", _T0, 40.75, -73.98)],
            schema=SILVER_MATCHED_SCHEMA,
        )
        gold = shape_active_trips_h3(df)
        expected = [f.name for f in GOLD_ACTIVE_TRIPS_H3_SCHEMA.fields]
        assert gold.schema.fieldNames() == expected

    def test_nearby_pings_aggregate_in_same_hex(self, spark: SparkSession) -> None:
        # Three trips within ~50 m of each other — must land in one
        # res-8 hex, so ONE gold row with active_trips=3.
        rows = [
            _matched_row("e1", "A", "d1", _T0, 40.7580, -73.9855),
            _matched_row("e2", "B", "d2", _T0 + dt.timedelta(seconds=10), 40.7581, -73.9856),
            _matched_row("e3", "C", "d3", _T0 + dt.timedelta(seconds=20), 40.7580, -73.9854),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        gold = shape_active_trips_h3(df).collect()
        assert len(gold) == 1
        r = gold[0].asDict()
        assert r["active_trips"] == 3
        assert r["borough"] == "Manhattan"

    def test_far_pings_produce_multiple_rows(self, spark: SparkSession) -> None:
        # Times Sq trip + JFK trip → two distinct hexes → two rows.
        rows = [
            _matched_row("m", "A", "d1", _T0, 40.7580, -73.9855),
            _matched_row("j", "B", "d2", _T0, 40.6413, -73.7781),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        gold = shape_active_trips_h3(df).collect()
        assert len(gold) == 2
        hexes = {r["h3_r8"] for r in gold}
        assert len(hexes) == 2

    def test_completed_trips_excluded(self, spark: SparkSession) -> None:
        rows = [
            _matched_row("e1", "A", "d1", _T0, 40.7580, -73.9855, trip_status="IN_PROGRESS"),
            _matched_row("e2", "B", "d2", _T0, 40.7580, -73.9855, trip_status="COMPLETED"),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        gold = shape_active_trips_h3(df).collect()
        assert len(gold) == 1
        assert gold[0]["active_trips"] == 1


# -------------------------------------------------------------------------
# gold_demand_by_h3_5min
# -------------------------------------------------------------------------


class TestDemandH3Mart:
    def test_output_schema_matches_spec(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_matched_row("e1", "A", "d1", _T0, 40.75, -73.98)],
            schema=SILVER_MATCHED_SCHEMA,
        )
        gold = shape_demand_h3(df)
        expected = [f.name for f in GOLD_DEMAND_H3_SCHEMA.fields]
        assert gold.schema.fieldNames() == expected

    def test_per_hex_distinct_counts(self, spark: SparkSession) -> None:
        # One hex, 4 pings, 2 trips, 2 drivers.
        rows = [
            _matched_row("e1", "A", "d1", _T0, 40.7580, -73.9855),
            _matched_row("e2", "A", "d1", _T0 + dt.timedelta(seconds=10), 40.7580, -73.9855),
            _matched_row("e3", "B", "d2", _T0 + dt.timedelta(seconds=20), 40.7580, -73.9855),
            _matched_row("e4", "B", "d2", _T0 + dt.timedelta(seconds=30), 40.7580, -73.9855),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        gold = shape_demand_h3(df).collect()
        assert len(gold) == 1
        r = gold[0].asDict()
        assert r["ping_count"] == 4
        assert r["distinct_trips"] == 2
        assert r["distinct_drivers"] == 2

    def test_h3_rollup_matches_borough_totals(self, spark: SparkSession) -> None:
        """H3 sums to borough totals *when each trip stays in one hex*.

        Every res-8 hex sits entirely inside one borough, so borough
        attribution flows correctly through the H3 grouping. In this
        test each trip has exactly one ping (hence one hex), so a
        naive rollup ``sum(distinct_trips) by borough`` matches the
        borough mart. In real data a trip that crosses hex boundaries
        gets counted once per hex, so H3-rollup over-counts vs the
        borough mart — see ``docs/phase-06-h3-hex-zones.md`` for the
        composability limitation of distinct-count aggregations.
        """
        # 2 trips in Manhattan hex A, 1 trip in Manhattan hex B,
        # 1 trip in Brooklyn hex C. Manhattan total should be 3.
        rows = [
            _matched_row("m1a", "T1", "d1", _T0, 40.7580, -73.9855),  # hex A
            _matched_row("m2a", "T2", "d2", _T0, 40.7580, -73.9855),  # hex A
            _matched_row("m1b", "T3", "d3", _T0, 40.7900, -73.9600),  # hex B (still Manhattan)
            _matched_row("b1c", "T4", "d4", _T0, 40.6782, -73.9442),  # hex C (Brooklyn)
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        gold = shape_demand_h3(df).collect()

        # Group by borough manually
        by_borough: dict[str | None, int] = {}
        for r in gold:
            by_borough[r["borough"]] = by_borough.get(r["borough"], 0) + r["distinct_trips"]
        assert by_borough.get("Manhattan") == 3
        assert by_borough.get("Brooklyn") == 1
