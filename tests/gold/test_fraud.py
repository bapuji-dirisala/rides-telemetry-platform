"""Spark-mode tests for Phase 5b fraud marts + Haversine helper.

Marked ``spark`` so they run only on demand. Uses a module-scoped
SparkSession to keep JVM startup cost bounded, and drives the pure
shape functions (``shape_fraud_teleport``, ``shape_fraud_dual_trip``,
``haversine_km_expr``) against handcrafted DataFrames. The streaming
plumbing (``foreachBatch`` for teleport, ``writeStream`` for
dual-trip) is exercised by the end-to-end local run documented in
``docs/phase-05b-fraud-marts.md``.
"""

from __future__ import annotations

import datetime as dt
import math
from typing import TYPE_CHECKING

import pytest

from rides_telemetry.geo.spark import haversine_km_expr
from rides_telemetry.gold.fraud_dual_trip import shape_fraud_dual_trip
from rides_telemetry.gold.fraud_teleport import shape_fraud_teleport
from rides_telemetry.gold.schemas import (
    GOLD_FRAUD_DUAL_TRIP_SCHEMA,
    GOLD_FRAUD_TELEPORT_SCHEMA,
    MAX_PLAUSIBLE_TELEPORT_KMH,
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
            app_name="rides-gold-fraud-tests",
            log_level="ERROR",
            shuffle_partitions=2,
        )
    )


# Naive datetimes — sidestep JVM-timezone round-trip (same reason as
# the phase-5a spark tests). See docs/phase-04-silver-streaming.md.
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
# Haversine distance
# -------------------------------------------------------------------------


class TestHaversineKmExpr:
    def test_zero_distance_for_same_point(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [(40.75, -73.98, 40.75, -73.98)],
            schema="lat1 double, lng1 double, lat2 double, lng2 double",
        )
        result = df.withColumn("d", haversine_km_expr("lat1", "lng1", "lat2", "lng2")).collect()[0][
            "d"
        ]
        assert result == pytest.approx(0.0, abs=1e-9)

    def test_matches_python_reference_within_1m(self, spark: SparkSession) -> None:
        # Times Square → JFK Airport, roughly ~24 km great-circle.
        pts = [(40.7580, -73.9855, 40.6413, -73.7781)]
        df = spark.createDataFrame(pts, schema="lat1 double, lng1 double, lat2 double, lng2 double")
        got = df.withColumn("d", haversine_km_expr("lat1", "lng1", "lat2", "lng2")).collect()[0][
            "d"
        ]

        # Reference computation in pure Python
        def _hav(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
            r = 6371.0088
            phi1, phi2 = math.radians(lat1), math.radians(lat2)
            d_phi = math.radians(lat2 - lat1)
            d_l = math.radians(lng2 - lng1)
            a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_l / 2) ** 2
            return 2 * r * math.asin(math.sqrt(min(a, 1.0)))

        expected = _hav(*pts[0])
        # 1 metre tolerance = 0.001 km. Spark-double math should be
        # much tighter than that; the assertion just guards against
        # dumb regressions.
        assert got == pytest.approx(expected, abs=0.001)

    def test_symmetric(self, spark: SparkSession) -> None:
        # d(A, B) == d(B, A) — a basic invariant.
        pts_ab = [(40.75, -73.98, 40.65, -73.94)]
        pts_ba = [(40.65, -73.94, 40.75, -73.98)]
        schema = "lat1 double, lng1 double, lat2 double, lng2 double"
        ab = (
            spark.createDataFrame(pts_ab, schema=schema)
            .withColumn("d", haversine_km_expr("lat1", "lng1", "lat2", "lng2"))
            .collect()[0]["d"]
        )
        ba = (
            spark.createDataFrame(pts_ba, schema=schema)
            .withColumn("d", haversine_km_expr("lat1", "lng1", "lat2", "lng2"))
            .collect()[0]["d"]
        )
        assert ab == pytest.approx(ba, abs=1e-9)


# -------------------------------------------------------------------------
# gold_fraud_teleport
# -------------------------------------------------------------------------


class TestFraudTeleport:
    def test_output_schema_matches_spec(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_matched_row("e1", "A", "d1", _T0, 40.75, -73.98)],
            schema=SILVER_MATCHED_SCHEMA,
        )
        result = shape_fraud_teleport(df)
        expected = [f.name for f in GOLD_FRAUD_TELEPORT_SCHEMA.fields]
        assert result.schema.fieldNames() == expected

    def test_single_ping_per_driver_never_flags(self, spark: SparkSession) -> None:
        # No previous ping to compare against — mart must be empty.
        rows = [_matched_row(f"e{i}", "A", f"d{i}", _T0, 40.75, -73.98) for i in range(3)]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        assert shape_fraud_teleport(df).count() == 0

    def test_slow_driver_is_not_flagged(self, spark: SparkSession) -> None:
        # Same driver, ~1 km apart over 60 s = 60 km/h. Well below
        # threshold — mart must be empty.
        rows = [
            _matched_row("e1", "A", "d1", _T0, 40.7500, -73.9800),
            _matched_row("e2", "A", "d1", _T0 + dt.timedelta(seconds=60), 40.7590, -73.9800),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        assert shape_fraud_teleport(df).count() == 0

    def test_teleport_ping_is_flagged(self, spark: SparkSession) -> None:
        # Same driver, ~24 km apart (Times Sq → JFK) over 60 s
        # = ~1440 km/h. Way above the 200 km/h threshold.
        rows = [
            _matched_row("e1", "A", "d1", _T0, 40.7580, -73.9855),  # Times Sq
            _matched_row("e2", "A", "d1", _T0 + dt.timedelta(seconds=60), 40.6413, -73.7781),  # JFK
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        result = shape_fraud_teleport(df).collect()
        assert len(result) == 1
        r = result[0].asDict()
        assert r["event_id"] == "e2"
        assert r["prev_event_id"] == "e1"
        assert r["implied_speed_kmh"] > MAX_PLAUSIBLE_TELEPORT_KMH
        # Sanity: distance is real (Times Sq → JFK is ~24 km straight).
        assert 20 < r["distance_km"] < 30

    def test_stationary_jitter_is_not_flagged(self, spark: SparkSession) -> None:
        # Two pings ~100 m apart over 5 s = ~72 km/h implied.
        # Implied speed is fine, but the guardrail on distance_km
        # (>= 0.5 km) protects us if a longer time delta made the
        # implied speed absurd from jitter alone.
        # Here we set a MUCH tighter drift: 5 m over 30 s = ~0.6 km/h
        # implied — not flagged for either reason.
        rows = [
            _matched_row("e1", "A", "d1", _T0, 40.7500, -73.9800),
            _matched_row("e2", "A", "d1", _T0 + dt.timedelta(seconds=30), 40.75005, -73.98005),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        assert shape_fraud_teleport(df).count() == 0

    def test_lag_partitioned_by_driver(self, spark: SparkSession) -> None:
        # Interleaved pings from TWO drivers, only driver A teleports.
        # Driver B's slow pings must not be paired against driver A's,
        # and vice versa.
        rows = [
            _matched_row("a1", "A", "d1", _T0, 40.7580, -73.9855),
            _matched_row("b1", "B", "d2", _T0 + dt.timedelta(seconds=10), 40.65, -73.94),
            _matched_row(
                "a2", "A", "d1", _T0 + dt.timedelta(seconds=60), 40.6413, -73.7781
            ),  # teleport
            _matched_row("b2", "B", "d2", _T0 + dt.timedelta(seconds=70), 40.651, -73.941),  # slow
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        result = shape_fraud_teleport(df).collect()
        assert len(result) == 1
        r = result[0].asDict()
        assert r["driver_id"] == "d1"
        assert r["event_id"] == "a2"
        assert r["prev_event_id"] == "a1"


# -------------------------------------------------------------------------
# gold_fraud_dual_trip
# -------------------------------------------------------------------------


class TestFraudDualTrip:
    def test_output_schema_matches_spec(self, spark: SparkSession) -> None:
        # Build a scenario that WILL emit a row so we exercise the
        # full projection (a mart with 0 rows still has the right
        # schema thanks to Spark, but the actual write path only
        # matters when there ARE rows).
        rows = [
            _matched_row("e1", "T1", "d1", _T0, 40.75, -73.98),
            _matched_row("e2", "T2", "d1", _T0 + dt.timedelta(minutes=1), 40.75, -73.98),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        result = shape_fraud_dual_trip(df)
        expected = [f.name for f in GOLD_FRAUD_DUAL_TRIP_SCHEMA.fields]
        assert result.schema.fieldNames() == expected

    def test_single_trip_per_driver_not_flagged(self, spark: SparkSession) -> None:
        rows = [
            _matched_row("e1", "T1", "d1", _T0, 40.75, -73.98),
            _matched_row("e2", "T1", "d1", _T0 + dt.timedelta(seconds=30), 40.75, -73.98),
            _matched_row("e3", "T1", "d1", _T0 + dt.timedelta(minutes=1), 40.75, -73.98),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        assert shape_fraud_dual_trip(df).count() == 0

    def test_two_trips_same_driver_same_window_is_flagged(self, spark: SparkSession) -> None:
        rows = [
            _matched_row("e1", "T1", "d1", _T0, 40.75, -73.98),
            _matched_row("e2", "T2", "d1", _T0 + dt.timedelta(minutes=1), 40.75, -73.98),
            _matched_row("e3", "T2", "d1", _T0 + dt.timedelta(minutes=2), 40.75, -73.98),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        result = shape_fraud_dual_trip(df).collect()
        assert len(result) == 1
        r = result[0].asDict()
        assert r["driver_id"] == "d1"
        assert r["concurrent_trip_count"] == 2
        assert sorted(r["sample_trip_ids"]) == ["T1", "T2"]

    def test_two_trips_same_driver_different_windows_not_flagged(self, spark: SparkSession) -> None:
        # 6 minutes apart — falls into two separate 5-min windows.
        rows = [
            _matched_row("e1", "T1", "d1", _T0, 40.75, -73.98),
            _matched_row("e2", "T2", "d1", _T0 + dt.timedelta(minutes=6), 40.75, -73.98),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        assert shape_fraud_dual_trip(df).count() == 0

    def test_two_trips_different_drivers_not_flagged(self, spark: SparkSession) -> None:
        rows = [
            _matched_row("e1", "T1", "d1", _T0, 40.75, -73.98),
            _matched_row("e2", "T2", "d2", _T0 + dt.timedelta(minutes=1), 40.75, -73.98),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        # Each driver has 1 trip → nothing to flag.
        assert shape_fraud_dual_trip(df).count() == 0

    def test_sample_trip_ids_capped_at_five(self, spark: SparkSession) -> None:
        # A driver on 7 distinct trips in one window — sample_trip_ids
        # must contain only 5 (sorted lexically → first 5).
        rows = [_matched_row(f"e{i}", f"T{i:02d}", "d1", _T0, 40.75, -73.98) for i in range(7)]
        df = spark.createDataFrame(rows, schema=SILVER_MATCHED_SCHEMA)
        result = shape_fraud_dual_trip(df).collect()
        assert len(result) == 1
        r = result[0].asDict()
        assert r["concurrent_trip_count"] == 7
        assert len(r["sample_trip_ids"]) == 5
        # Deterministic ordering after ``sort_array`` — the first 5
        # trip IDs alphabetically.
        assert r["sample_trip_ids"] == ["T00", "T01", "T02", "T03", "T04"]
