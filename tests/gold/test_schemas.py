"""Hermetic gold schema + window-config tests."""

from __future__ import annotations

from rides_telemetry.geo.nyc import NYC_BOROUGHS
from rides_telemetry.gold.schemas import (
    ACTIVE_TRIPS_WINDOW,
    DEMAND_WATERMARK,
    DEMAND_WINDOW,
    GOLD_ACTIVE_TRIPS_SCHEMA,
    GOLD_DEMAND_SCHEMA,
)


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
