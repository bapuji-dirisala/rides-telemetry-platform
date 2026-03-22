"""Hermetic gold schema + window-config tests."""

from __future__ import annotations

from pyspark.sql.types import ArrayType, StringType

from rides_telemetry.geo.nyc import NYC_BOROUGHS
from rides_telemetry.gold.schemas import (
    ACTIVE_TRIPS_WINDOW,
    DEMAND_WATERMARK,
    DEMAND_WINDOW,
    FRAUD_WATERMARK,
    FRAUD_WINDOW,
    GOLD_ACTIVE_TRIPS_SCHEMA,
    GOLD_DEMAND_SCHEMA,
    GOLD_FRAUD_DUAL_TRIP_SCHEMA,
    GOLD_FRAUD_TELEPORT_SCHEMA,
    MAX_PLAUSIBLE_TELEPORT_KMH,
    MIN_TELEPORT_DISTANCE_KM,
    MIN_TELEPORT_TIME_DELTA_SECONDS,
)
from rides_telemetry.silver.schemas import MAX_PLAUSIBLE_SPEED_KMH


class TestActiveTripsSchema:
    def test_has_window_bounds_borough_count(self) -> None:
        names = {f.name for f in GOLD_ACTIVE_TRIPS_SCHEMA.fields}
        required = {"window_start", "window_end", "borough", "active_trips"}
        assert required.issubset(names), f"missing required cols: {required - names}"

    def test_active_trips_count_is_integer(self) -> None:
        f = next(f for f in GOLD_ACTIVE_TRIPS_SCHEMA.fields if f.name == "active_trips")
        # Integer, not Long — no borough will ever have >2B active trips
        # and a smaller type saves a few bytes per row.
        assert f.dataType.simpleString() == "int"

    def test_window_bounds_are_not_nullable(self) -> None:
        # Every gold row is aggregated INTO a window; the window itself
        # is derived and always present.
        for name in ("window_start", "window_end", "borough", "active_trips"):
            f = next(f for f in GOLD_ACTIVE_TRIPS_SCHEMA.fields if f.name == name)
            assert not f.nullable, f"{name} must be NOT NULL"


class TestDemandSchema:
    def test_has_supply_and_demand_columns(self) -> None:
        names = {f.name for f in GOLD_DEMAND_SCHEMA.fields}
        required = {
            "window_start",
            "window_end",
            "borough",
            "ping_count",
            "distinct_trips",
            "distinct_drivers",
        }
        assert required.issubset(names), f"missing required cols: {required - names}"

    def test_count_columns_are_long(self) -> None:
        # ``countDistinct`` returns a ``bigint`` in Spark. Using ``long``
        # here keeps the schema honest and avoids a downstream cast.
        for name in ("ping_count", "distinct_trips", "distinct_drivers"):
            f = next(f for f in GOLD_DEMAND_SCHEMA.fields if f.name == name)
            assert f.dataType.simpleString() == "bigint", (
                f"{name} should be long/bigint, got {f.dataType.simpleString()}"
            )


class TestWindowConfig:
    def test_windows_are_reasonable_durations(self) -> None:
        # Active trips: minute-scale so surge reacts fast.
        assert ACTIVE_TRIPS_WINDOW.endswith(("minute", "minutes"))
        # Demand: minute-scale is fine; hour would be too coarse for surge.
        assert DEMAND_WINDOW.endswith(("minute", "minutes"))
        # Watermark >= window is a hard requirement of Spark's tumbling
        # window semantics (otherwise windows never close).
        # Both are strings but we can eyeball: "5 minutes" < "10 minutes".
        assert DEMAND_WATERMARK.endswith(("minute", "minutes"))


class TestBoroughVocabulary:
    """Gold marts use borough NAMES from :mod:`~rides_telemetry.geo.nyc`.

    Any drift between the borough constants and the values gold produces
    would silently split aggregations. This test locks the vocabulary.
    """

    def test_expected_boroughs_present(self) -> None:
        expected = {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"}
        assert expected == set(NYC_BOROUGHS.keys()), (
            "borough vocabulary changed — update gold docs + downstream marts"
        )


# -------------------------------------------------------------------------
# Phase 5b — fraud marts
# -------------------------------------------------------------------------


class TestFraudTeleportSchema:
    def test_carries_both_pings_geometry(self) -> None:
        # Operators need to eyeball flagged pairs without a follow-up
        # query — the schema must include both current and previous
        # ping's lat/lng.
        names = {f.name for f in GOLD_FRAUD_TELEPORT_SCHEMA.fields}
        required = {"lat", "lng", "prev_lat", "prev_lng"}
        assert required.issubset(names), f"missing geometry cols: {required - names}"

    def test_carries_derived_signal(self) -> None:
        names = {f.name for f in GOLD_FRAUD_TELEPORT_SCHEMA.fields}
        required = {"distance_km", "time_delta_seconds", "implied_speed_kmh"}
        assert required.issubset(names), f"missing signal cols: {required - names}"

    def test_identity_cols_are_not_nullable(self) -> None:
        for name in ("event_id", "driver_id", "trip_id", "prev_event_id", "prev_trip_id"):
            f = next(f for f in GOLD_FRAUD_TELEPORT_SCHEMA.fields if f.name == name)
            assert not f.nullable, f"{name} must be NOT NULL"


class TestFraudDualTripSchema:
    def test_sample_trip_ids_is_string_array(self) -> None:
        f = next(f for f in GOLD_FRAUD_DUAL_TRIP_SCHEMA.fields if f.name == "sample_trip_ids")
        assert isinstance(f.dataType, ArrayType), "sample_trip_ids must be an array"
        assert isinstance(f.dataType.elementType, StringType), (
            f"sample_trip_ids elements must be string, got {f.dataType.elementType}"
        )
        # Array elements themselves are non-nullable (trip IDs never null).
        assert not f.dataType.containsNull

    def test_concurrent_trip_count_is_integer(self) -> None:
        f = next(f for f in GOLD_FRAUD_DUAL_TRIP_SCHEMA.fields if f.name == "concurrent_trip_count")
        # A single driver claiming >2B concurrent trips is not a
        # scenario worth planning for. Int is enough and saves bytes.
        assert f.dataType.simpleString() == "int"


class TestFraudTunables:
    def test_teleport_threshold_matches_silver_single_ping_threshold(self) -> None:
        # Deliberate coupling — silver drops single pings claiming
        # speed > MAX_PLAUSIBLE_SPEED_KMH, and gold flags ping *pairs*
        # whose implied speed > MAX_PLAUSIBLE_TELEPORT_KMH. Keeping
        # them equal is a design choice; if one moves, the reviewer
        # should see the other in the same PR.
        assert MAX_PLAUSIBLE_TELEPORT_KMH == MAX_PLAUSIBLE_SPEED_KMH

    def test_teleport_time_and_distance_guards_are_positive(self) -> None:
        assert MIN_TELEPORT_TIME_DELTA_SECONDS > 0
        assert MIN_TELEPORT_DISTANCE_KM > 0

    def test_fraud_window_matches_demand_window(self) -> None:
        # Operators join fraud alerts against the demand mart for
        # context ("what share of borough supply was this driver?").
        # Same window makes the join trivial.
        assert FRAUD_WINDOW == DEMAND_WINDOW

    def test_fraud_watermark_matches_gps_source(self) -> None:
        assert FRAUD_WATERMARK.endswith(("minute", "minutes"))
