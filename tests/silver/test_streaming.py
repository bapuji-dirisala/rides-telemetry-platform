"""Spark-mode tests for silver shaping logic.

Marked ``spark`` so it's skipped by default. Uses the same one-per-
module SparkSession as the bronze tests to keep startup cost bounded.

We deliberately do not exercise the streaming machinery here — we
call the pure-DataFrame shaping helpers (``shape_silver_gps`` and
``shape_trip_facts_batch``) against synthetic batch DataFrames. That
gives us end-to-end coverage of the transformation logic without
depending on Kafka, Delta, or Spark's streaming state store.
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

import pytest

from rides_telemetry.bronze.schemas import BRONZE_GPS_SCHEMA, BRONZE_TRIPS_SCHEMA
from rides_telemetry.silver.gps import shape_silver_gps
from rides_telemetry.silver.matched import shape_matched
from rides_telemetry.silver.schemas import (
    SILVER_GPS_SCHEMA,
    SILVER_MATCHED_SCHEMA,
    SILVER_TRIPS_SCHEMA,
)
from rides_telemetry.silver.trips import shape_trip_facts_batch
from rides_telemetry.spark import SparkSessionConfig, build_spark_session

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return build_spark_session(
        SparkSessionConfig(
            app_name="rides-silver-tests",
            log_level="ERROR",
            shuffle_partitions=2,
        )
    )


# TimestampType round-trips through the JVM's local timezone, so
# tz-aware Python datetimes come back naive-in-local-time. Using naive
# datetimes on both sides sidesteps the mismatch and keeps assertions
# equality-based rather than epoch-based.
_T0 = dt.datetime(2024, 1, 15, 8, 0)


# -------------------------------------------------------------------------
# Fixtures — small handcrafted batches shaped like bronze reads.
# -------------------------------------------------------------------------


def _bronze_gps_row(
    event_id: str,
    trip_id: str = "trip-A",
    driver_id: str = "driver-1",
    lat: float = 40.75,
    lng: float = -73.98,
    speed_kmh: float | None = 25.0,
    accuracy_m: float | None = 5.0,
    event_ts: dt.datetime = _T0,
) -> dict:
    """Row shape matching ``BRONZE_GPS_SCHEMA`` (payload + envelope)."""
    return {
        "event_id": event_id,
        "event_ts": event_ts,
        "event_type": "gps_ping",
        "trip_id": trip_id,
        "driver_id": driver_id,
        "lat": lat,
        "lng": lng,
        "speed_kmh": speed_kmh,
        "heading_deg": 90.0,
        "accuracy_m": accuracy_m,
        "kafka_topic": "rides.gps.v1",
        "kafka_partition": 0,
        "kafka_offset": 0,
        "kafka_timestamp": event_ts,
        "kafka_key": trip_id,
        "ingest_ts": event_ts,
        "raw_json": "{}",
    }


def _bronze_lifecycle_row(
    event_type: str,
    trip_id: str = "trip-A",
    rider_id: str = "rider-1",
    driver_id: str | None = None,
    event_ts: dt.datetime = _T0,
    **overrides,
) -> dict:
    """Row shape matching ``BRONZE_TRIPS_SCHEMA``. All optional cols default to None."""
    base = {
        "event_id": f"{event_type}-{trip_id}",
        "event_ts": event_ts,
        "event_type": event_type,
        "trip_id": trip_id,
        "rider_id": rider_id,
        "driver_id": driver_id,
        "pickup_lat": 40.75,
        "pickup_lng": -73.98,
        "dropoff_lat": 40.68,
        "dropoff_lng": -73.94,
        "trip_distance_km": None,
        "fare_amount_usd": None,
        "cancelled_by": None,
        "kafka_topic": "rides.trips.v1",
        "kafka_partition": 0,
        "kafka_offset": 0,
        "kafka_timestamp": event_ts,
        "kafka_key": trip_id,
        "ingest_ts": event_ts,
        "raw_json": "{}",
    }
    base.update(overrides)
    return base


# -------------------------------------------------------------------------
# Silver GPS shaping
# -------------------------------------------------------------------------


class TestSilverGpsShaping:
    def test_output_schema_matches_silver_spec(self, spark: SparkSession) -> None:
        # One valid row → output schema should exactly match SILVER_GPS_SCHEMA
        # (field names and order; nullability isn't preserved through select
        # in all Spark versions, so we compare names only).
        df = spark.createDataFrame([_bronze_gps_row("e1")], schema=BRONZE_GPS_SCHEMA)
        silver = shape_silver_gps(df)

        expected = [f.name for f in SILVER_GPS_SCHEMA.fields]
        actual = silver.schema.fieldNames()
        assert actual == expected

    def test_dedup_by_event_id(self, spark: SparkSession) -> None:
        # Two rows with the same event_id → one row out.
        df = spark.createDataFrame(
            [
                _bronze_gps_row("dup-id", speed_kmh=20.0),
                _bronze_gps_row("dup-id", speed_kmh=25.0),
                _bronze_gps_row("other-id", speed_kmh=30.0),
            ],
            schema=BRONZE_GPS_SCHEMA,
        )
        rows = shape_silver_gps(df).collect()
        assert len(rows) == 2
        assert {r["event_id"] for r in rows} == {"dup-id", "other-id"}

    def test_drops_pings_outside_nyc(self, spark: SparkSession) -> None:
        # A ping in San Francisco should be filtered out.
        df = spark.createDataFrame(
            [
                _bronze_gps_row("nyc", lat=40.75, lng=-73.98),
                _bronze_gps_row("sf", lat=37.77, lng=-122.42),
            ],
            schema=BRONZE_GPS_SCHEMA,
        )
        rows = shape_silver_gps(df).collect()
        assert {r["event_id"] for r in rows} == {"nyc"}

    def test_drops_implausible_speed(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                _bronze_gps_row("normal", speed_kmh=45.0),
                _bronze_gps_row("teleport", speed_kmh=900.0),
                _bronze_gps_row("negative", speed_kmh=-5.0),
            ],
            schema=BRONZE_GPS_SCHEMA,
        )
        rows = shape_silver_gps(df).collect()
        assert {r["event_id"] for r in rows} == {"normal"}

    def test_keeps_pings_with_missing_speed(self, spark: SparkSession) -> None:
        # Missing speed is not an error — some devices just don't report.
        df = spark.createDataFrame(
            [
                _bronze_gps_row("with-speed", speed_kmh=30.0),
                _bronze_gps_row("no-speed", speed_kmh=None),
            ],
            schema=BRONZE_GPS_SCHEMA,
        )
        rows = shape_silver_gps(df).collect()
        assert {r["event_id"] for r in rows} == {"with-speed", "no-speed"}

    def test_drops_low_accuracy_pings(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                _bronze_gps_row("good", accuracy_m=5.0),
                _bronze_gps_row("terrible", accuracy_m=5000.0),
            ],
            schema=BRONZE_GPS_SCHEMA,
        )
        rows = shape_silver_gps(df).collect()
        assert {r["event_id"] for r in rows} == {"good"}


# -------------------------------------------------------------------------
# Silver trip facts shaping (batch aggregation, pre-MERGE)
# -------------------------------------------------------------------------


class TestTripFactsShaping:
    def test_folds_full_lifecycle_into_one_row(self, spark: SparkSession) -> None:
        events = [
            _bronze_lifecycle_row("trip_requested", event_ts=_T0),
            _bronze_lifecycle_row(
                "trip_accepted",
                driver_id="driver-1",
                event_ts=_T0 + dt.timedelta(seconds=30),
            ),
            _bronze_lifecycle_row(
                "trip_started",
                driver_id="driver-1",
                event_ts=_T0 + dt.timedelta(minutes=2),
            ),
            _bronze_lifecycle_row(
                "trip_ended",
                driver_id="driver-1",
                event_ts=_T0 + dt.timedelta(minutes=15),
                trip_distance_km=6.5,
                fare_amount_usd=22.0,
            ),
        ]
        df = spark.createDataFrame(events, schema=BRONZE_TRIPS_SCHEMA)
        rows = shape_trip_facts_batch(df).collect()

        assert len(rows) == 1
        r = rows[0].asDict()
        assert r["trip_id"] == "trip-A"
        assert r["driver_id"] == "driver-1"
        assert r["requested_ts"] == _T0
        assert r["accepted_ts"] == _T0 + dt.timedelta(seconds=30)
        assert r["started_ts"] == _T0 + dt.timedelta(minutes=2)
        assert r["ended_ts"] == _T0 + dt.timedelta(minutes=15)
        assert r["cancelled_ts"] is None
        assert r["trip_distance_km"] == 6.5
        assert r["fare_amount_usd"] == 22.0
        assert r["status"] == "COMPLETED"
        assert r["first_event_ts"] == _T0
        assert r["last_event_ts"] == _T0 + dt.timedelta(minutes=15)

    def test_status_pending_when_only_requested(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_bronze_lifecycle_row("trip_requested")], schema=BRONZE_TRIPS_SCHEMA
        )
        r = shape_trip_facts_batch(df).collect()[0].asDict()
        assert r["status"] == "PENDING"
        assert r["accepted_ts"] is None

    def test_status_accepted_when_up_to_accepted(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                _bronze_lifecycle_row("trip_requested"),
                _bronze_lifecycle_row("trip_accepted", driver_id="d1"),
            ],
            schema=BRONZE_TRIPS_SCHEMA,
        )
        r = shape_trip_facts_batch(df).collect()[0].asDict()
        assert r["status"] == "ACCEPTED"
        assert r["driver_id"] == "d1"

    def test_status_in_progress_when_started(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                _bronze_lifecycle_row("trip_requested"),
                _bronze_lifecycle_row("trip_accepted", driver_id="d1"),
                _bronze_lifecycle_row("trip_started", driver_id="d1"),
            ],
            schema=BRONZE_TRIPS_SCHEMA,
        )
        r = shape_trip_facts_batch(df).collect()[0].asDict()
        assert r["status"] == "IN_PROGRESS"
        assert r["ended_ts"] is None

    def test_status_cancelled_beats_started(self, spark: SparkSession) -> None:
        # Out-of-order arrival: trip_started AND trip_cancelled both present.
        # CANCELLED is terminal, so it should win.
        df = spark.createDataFrame(
            [
                _bronze_lifecycle_row("trip_requested"),
                _bronze_lifecycle_row(
                    "trip_started",
                    driver_id="d1",
                    event_ts=_T0 + dt.timedelta(minutes=2),
                ),
                _bronze_lifecycle_row(
                    "trip_cancelled",
                    event_ts=_T0 + dt.timedelta(minutes=3),
                    cancelled_by="rider",
                ),
            ],
            schema=BRONZE_TRIPS_SCHEMA,
        )
        r = shape_trip_facts_batch(df).collect()[0].asDict()
        assert r["status"] == "CANCELLED"
        assert r["cancelled_by"] == "rider"

    def test_multiple_trips_produce_multiple_rows(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                _bronze_lifecycle_row("trip_requested", trip_id="A", rider_id="rA"),
                _bronze_lifecycle_row("trip_requested", trip_id="B", rider_id="rB"),
                _bronze_lifecycle_row(
                    "trip_accepted",
                    trip_id="A",
                    rider_id="rA",
                    driver_id="dA",
                ),
            ],
            schema=BRONZE_TRIPS_SCHEMA,
        )
        rows = {r["trip_id"]: r.asDict() for r in shape_trip_facts_batch(df).collect()}
        assert set(rows) == {"A", "B"}
        assert rows["A"]["status"] == "ACCEPTED"
        assert rows["B"]["status"] == "PENDING"
        assert rows["A"]["driver_id"] == "dA"
        assert rows["B"]["driver_id"] is None


# -------------------------------------------------------------------------
# Silver matched (stream-stream join)
#
# We exercise ``shape_matched`` against static DataFrames. In batch
# mode watermarks are no-ops and the streaming state store isn't used,
# so this covers the join predicate, filter, and projection — the
# actual streaming semantics (state eviction, incremental emit) are
# Spark's responsibility and are exercised by the end-to-end run
# documented in the phase doc.
# -------------------------------------------------------------------------


def _silver_gps_row(
    event_id: str,
    trip_id: str,
    driver_id: str,
    event_ts: dt.datetime,
    lat: float = 40.75,
    lng: float = -73.98,
    speed_kmh: float | None = 25.0,
) -> dict:
    return {
        "event_id": event_id,
        "event_ts": event_ts,
        "trip_id": trip_id,
        "driver_id": driver_id,
        "lat": lat,
        "lng": lng,
        "speed_kmh": speed_kmh,
        "heading_deg": 90.0,
        "accuracy_m": 5.0,
        "ingest_ts": event_ts,
        "silver_processed_ts": event_ts,
    }


def _silver_trip_row(
    trip_id: str,
    rider_id: str,
    driver_id: str | None,
    status: str,
    started_ts: dt.datetime | None,
    ended_ts: dt.datetime | None = None,
    cancelled_ts: dt.datetime | None = None,
    requested_ts: dt.datetime | None = None,
    last_event_ts: dt.datetime | None = None,
) -> dict:
    return {
        "trip_id": trip_id,
        "rider_id": rider_id,
        "driver_id": driver_id,
        "requested_ts": requested_ts or started_ts,
        "accepted_ts": started_ts,
        "started_ts": started_ts,
        "ended_ts": ended_ts,
        "cancelled_ts": cancelled_ts,
        "pickup_lat": 40.75,
        "pickup_lng": -73.98,
        "dropoff_lat": 40.68,
        "dropoff_lng": -73.94,
        "trip_distance_km": None,
        "fare_amount_usd": None,
        "cancelled_by": None,
        "status": status,
        "first_event_ts": requested_ts or started_ts,
        "last_event_ts": last_event_ts or ended_ts or started_ts,
        "silver_processed_ts": started_ts,
    }


class TestMatchedJoin:
    def test_output_schema_matches_matched_spec(self, spark: SparkSession) -> None:
        gps = spark.createDataFrame(
            [_silver_gps_row("e1", "A", "d1", _T0 + dt.timedelta(minutes=1))],
            schema=SILVER_GPS_SCHEMA,
        )
        trips = spark.createDataFrame(
            [
                _silver_trip_row(
                    "A",
                    "r1",
                    "d1",
                    "COMPLETED",
                    started_ts=_T0,
                    ended_ts=_T0 + dt.timedelta(minutes=15),
                )
            ],
            schema=SILVER_TRIPS_SCHEMA,
        )
        matched = shape_matched(gps, trips)
        expected = [f.name for f in SILVER_MATCHED_SCHEMA.fields]
        assert matched.schema.fieldNames() == expected

    def test_ping_inside_window_matches(self, spark: SparkSession) -> None:
        gps = spark.createDataFrame(
            [
                _silver_gps_row("mid-trip", "A", "d1", _T0 + dt.timedelta(minutes=5)),
            ],
            schema=SILVER_GPS_SCHEMA,
        )
        trips = spark.createDataFrame(
            [
                _silver_trip_row(
                    "A",
                    "r1",
                    "d1",
                    "COMPLETED",
                    started_ts=_T0,
                    ended_ts=_T0 + dt.timedelta(minutes=15),
                )
            ],
            schema=SILVER_TRIPS_SCHEMA,
        )
        rows = shape_matched(gps, trips).collect()
        assert len(rows) == 1
        r = rows[0].asDict()
        assert r["event_id"] == "mid-trip"
        assert r["rider_id"] == "r1"
        assert r["trip_status"] == "COMPLETED"
        assert r["pickup_lat"] == 40.75

    def test_ping_before_start_is_dropped(self, spark: SparkSession) -> None:
        # 5 minutes before started_ts is well outside the 30s start slack.
        gps = spark.createDataFrame(
            [_silver_gps_row("before", "A", "d1", _T0 - dt.timedelta(minutes=5))],
            schema=SILVER_GPS_SCHEMA,
        )
        trips = spark.createDataFrame(
            [
                _silver_trip_row(
                    "A",
                    "r1",
                    "d1",
                    "COMPLETED",
                    started_ts=_T0,
                    ended_ts=_T0 + dt.timedelta(minutes=15),
                )
            ],
            schema=SILVER_TRIPS_SCHEMA,
        )
        assert shape_matched(gps, trips).count() == 0

    def test_ping_within_start_slack_matches(self, spark: SparkSession) -> None:
        # 10s before started_ts is inside the 30s start slack.
        gps = spark.createDataFrame(
            [_silver_gps_row("edge", "A", "d1", _T0 - dt.timedelta(seconds=10))],
            schema=SILVER_GPS_SCHEMA,
        )
        trips = spark.createDataFrame(
            [
                _silver_trip_row(
                    "A",
                    "r1",
                    "d1",
                    "COMPLETED",
                    started_ts=_T0,
                    ended_ts=_T0 + dt.timedelta(minutes=15),
                )
            ],
            schema=SILVER_TRIPS_SCHEMA,
        )
        assert shape_matched(gps, trips).count() == 1

    def test_ping_within_end_slack_matches(self, spark: SparkSession) -> None:
        # 3 minutes after ended_ts is inside the 5-minute end slack.
        gps = spark.createDataFrame(
            [
                _silver_gps_row(
                    "late",
                    "A",
                    "d1",
                    _T0 + dt.timedelta(minutes=18),  # ended at min 15 + 3min slack
                ),
            ],
            schema=SILVER_GPS_SCHEMA,
        )
        trips = spark.createDataFrame(
            [
                _silver_trip_row(
                    "A",
                    "r1",
                    "d1",
                    "COMPLETED",
                    started_ts=_T0,
                    ended_ts=_T0 + dt.timedelta(minutes=15),
                )
            ],
            schema=SILVER_TRIPS_SCHEMA,
        )
        assert shape_matched(gps, trips).count() == 1

    def test_ping_well_after_end_is_dropped(self, spark: SparkSession) -> None:
        # 30 minutes after ended_ts is well outside the 5-minute slack.
        gps = spark.createDataFrame(
            [_silver_gps_row("far-after", "A", "d1", _T0 + dt.timedelta(minutes=45))],
            schema=SILVER_GPS_SCHEMA,
        )
        trips = spark.createDataFrame(
            [
                _silver_trip_row(
                    "A",
                    "r1",
                    "d1",
                    "COMPLETED",
                    started_ts=_T0,
                    ended_ts=_T0 + dt.timedelta(minutes=15),
                )
            ],
            schema=SILVER_TRIPS_SCHEMA,
        )
        assert shape_matched(gps, trips).count() == 0

    def test_cancelled_trip_is_excluded(self, spark: SparkSession) -> None:
        # A ping arriving inside what would have been the trip window,
        # but the trip was cancelled — must NOT appear in matched.
        gps = spark.createDataFrame(
            [_silver_gps_row("orphan", "A", "d1", _T0 + dt.timedelta(minutes=1))],
            schema=SILVER_GPS_SCHEMA,
        )
        trips = spark.createDataFrame(
            [
                _silver_trip_row(
                    "A",
                    "r1",
                    "d1",
                    "CANCELLED",
                    started_ts=_T0,
                    cancelled_ts=_T0 + dt.timedelta(minutes=2),
                    last_event_ts=_T0 + dt.timedelta(minutes=2),
                )
            ],
            schema=SILVER_TRIPS_SCHEMA,
        )
        assert shape_matched(gps, trips).count() == 0

    def test_in_progress_trip_uses_last_event_ts_as_end(self, spark: SparkSession) -> None:
        # Trip is still IN_PROGRESS (no ended_ts). The join predicate
        # falls back to COALESCE(ended_ts, last_event_ts) + slack.
        gps = spark.createDataFrame(
            [_silver_gps_row("live", "A", "d1", _T0 + dt.timedelta(minutes=4))],
            schema=SILVER_GPS_SCHEMA,
        )
        trips = spark.createDataFrame(
            [
                _silver_trip_row(
                    "A",
                    "r1",
                    "d1",
                    "IN_PROGRESS",
                    started_ts=_T0,
                    ended_ts=None,
                    last_event_ts=_T0 + dt.timedelta(minutes=3),
                )
            ],
            schema=SILVER_TRIPS_SCHEMA,
        )
        # Ping at min 4 is within last_event_ts (min 3) + 5min slack.
        assert shape_matched(gps, trips).count() == 1

    def test_wrong_trip_id_is_dropped(self, spark: SparkSession) -> None:
        # Ping's trip_id doesn't match any trip — inner join drops it.
        gps = spark.createDataFrame(
            [_silver_gps_row("orphan", "MISSING", "d1", _T0 + dt.timedelta(minutes=1))],
            schema=SILVER_GPS_SCHEMA,
        )
        trips = spark.createDataFrame(
            [
                _silver_trip_row(
                    "A",
                    "r1",
                    "d1",
                    "COMPLETED",
                    started_ts=_T0,
                    ended_ts=_T0 + dt.timedelta(minutes=15),
                )
            ],
            schema=SILVER_TRIPS_SCHEMA,
        )
        assert shape_matched(gps, trips).count() == 0
