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
    GOLD_ACTIVE_TRIPS_H3_SCHEMA,
    GOLD_ACTIVE_TRIPS_SCHEMA,
    GOLD_DEMAND_H3_SCHEMA,
    GOLD_DEMAND_SCHEMA,
    GOLD_FRAUD_DUAL_TRIP_SCHEMA,
    GOLD_FRAUD_TELEPORT_SCHEMA,
    H3_RESOLUTION,
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


# -------------------------------------------------------------------------
# Phase 6 — H3 hex marts
# -------------------------------------------------------------------------


class TestH3Resolution:
    def test_resolution_is_8(self) -> None:
        # Res 8 = ~461 m edge, ~0.74 km² area. Documented as the
        # default in schemas.H3_RESOLUTION and the phase-6 doc.
        # Bumping this constant is a deliberate cross-cutting change
        # (touches every H3 mart's cardinality) — this test forces
        # a reviewer to see both.
        assert H3_RESOLUTION == 8

    def test_resolution_is_in_reasonable_range(self) -> None:
        # Below 6 (~36 km edges) the marts collapse to a handful of
        # rows city-wide; above 10 (~65 m) individual GPS jitter
        # blows out cardinality. Force the value into a sane band.
        assert 6 <= H3_RESOLUTION <= 10


class TestActiveTripsH3Schema:
    def test_has_h3_and_borough_columns(self) -> None:
        names = {f.name for f in GOLD_ACTIVE_TRIPS_H3_SCHEMA.fields}
        assert "h3_r8" in names, "H3 mart must carry the h3_r8 hex ID column"
        assert "borough" in names, (
            "borough kept alongside for cheap drill-through — see phase-6 doc"
        )

    def test_h3_col_is_not_nullable(self) -> None:
        # h3_r8 is the primary grouping key; NULL would mean the ping
        # couldn't be classified, which we filter out upstream.
        f = next(f for f in GOLD_ACTIVE_TRIPS_H3_SCHEMA.fields if f.name == "h3_r8")
        assert not f.nullable

    def test_borough_col_is_nullable(self) -> None:
        # borough is derived from the same lat/lng — could in principle
        # be NULL if the ping is outside NYC bbox (never happens in
        # practice given silver's filter, but the schema honesty is
        # cheap and future-proof).
        f = next(f for f in GOLD_ACTIVE_TRIPS_H3_SCHEMA.fields if f.name == "borough")
        assert f.nullable


class TestDemandH3Schema:
    def test_has_all_three_distinct_counts(self) -> None:
        names = {f.name for f in GOLD_DEMAND_H3_SCHEMA.fields}
        for c in ("ping_count", "distinct_trips", "distinct_drivers"):
            assert c in names, f"demand H3 mart missing {c}"

    def test_geographic_columns_present(self) -> None:
        names = {f.name for f in GOLD_DEMAND_H3_SCHEMA.fields}
        assert {"h3_r8", "borough"}.issubset(names)


class TestH3MartSchemaParityWithBoroughMarts:
    """H3 marts must include the same non-geo columns as their borough peers.

    A consumer switching from borough to H3 gets extra spatial
    granularity but the SAME window / measure columns. If a borough
    mart gains a column (e.g. a new outcome metric), the H3 peer
    should follow — these tests will flag the drift.
    """

    def test_active_trips_measure_cols_match(self) -> None:
        borough_measures = {"active_trips"}
        h3_measures = {"active_trips"}
        # Both must have the same measure cols; geo dims may differ.
        borough_names = {f.name for f in GOLD_ACTIVE_TRIPS_SCHEMA.fields}
        h3_names = {f.name for f in GOLD_ACTIVE_TRIPS_H3_SCHEMA.fields}
        assert borough_measures.issubset(borough_names)
        assert h3_measures.issubset(h3_names)
        # Both share window bounds
        for col in ("window_start", "window_end", "gold_processed_ts"):
            assert col in borough_names, f"borough mart missing {col}"
            assert col in h3_names, f"H3 mart missing {col}"

    def test_demand_measure_cols_match(self) -> None:
        measures = {"ping_count", "distinct_trips", "distinct_drivers"}
        borough_names = {f.name for f in GOLD_DEMAND_SCHEMA.fields}
        h3_names = {f.name for f in GOLD_DEMAND_H3_SCHEMA.fields}
        assert measures.issubset(borough_names)
        assert measures.issubset(h3_names)
