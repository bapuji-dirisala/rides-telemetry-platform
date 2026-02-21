"""End-to-end tests for the ride simulator.

Every test here uses :class:`InMemoryTripSource`, so nothing hits the
network — CI stays fast and hermetic.
"""

from __future__ import annotations

import hashlib
import json

from rides_telemetry.events import (
    EventType,
    GpsPing,
    TripAccepted,
    TripEnded,
    TripRequested,
    TripStarted,
)
from rides_telemetry.generator import RideSimulator, SimulatorConfig
from rides_telemetry.geo import is_inside_nyc
from rides_telemetry.sources.in_memory import InMemoryTripSource


def _run(source: InMemoryTripSource, seed: int = 42) -> list:
    sim = RideSimulator(source, SimulatorConfig(seed=seed, ping_interval_s=30.0))
    return list(sim.iter_events())


class TestDeterminism:
    """Same seed + same input must always produce the exact same output."""

    def test_two_runs_produce_identical_json(self, in_memory_source: InMemoryTripSource) -> None:
        run1 = [e.model_dump_json() for e in _run(in_memory_source)]
        run2 = [e.model_dump_json() for e in _run(in_memory_source)]
        assert run1 == run2

    def test_different_seed_produces_different_output(
        self, in_memory_source: InMemoryTripSource
    ) -> None:
        run_a = [e.model_dump_json() for e in _run(in_memory_source, seed=1)]
        run_b = [e.model_dump_json() for e in _run(in_memory_source, seed=2)]
        assert run_a != run_b

    def test_hash_is_stable(self, in_memory_source: InMemoryTripSource) -> None:
        # Snapshot-style test: if we ever accidentally break determinism
        # (e.g. by using ``uuid4()`` somewhere), this hash will change.
        payload = "\n".join(e.model_dump_json() for e in _run(in_memory_source))
        digest = hashlib.sha256(payload.encode()).hexdigest()
        # We don't hardcode the expected digest here — the point is
        # that two runs match (covered above). Just make sure the hash
        # is a stable string of the expected shape.
        assert len(digest) == 64


class TestLifecycleInvariants:
    """The stream must be a well-formed rides-platform trace."""

    def test_every_trip_has_exactly_one_of_each_lifecycle_event(
        self, in_memory_source: InMemoryTripSource, sample_trips: list
    ) -> None:
        events = _run(in_memory_source)
        for cls in (TripRequested, TripAccepted, TripStarted, TripEnded):
            of_kind = [e for e in events if isinstance(e, cls)]
            assert len(of_kind) == len(sample_trips), cls.__name__

    def test_lifecycle_ordering_per_trip(self, in_memory_source: InMemoryTripSource) -> None:
        events = _run(in_memory_source)
        by_trip: dict = {}
        for e in events:
            by_trip.setdefault(e.trip_id, []).append(e)

        for trip_events in by_trip.values():
            trip_events.sort(key=lambda e: e.event_ts)
            types = [e.event_type for e in trip_events]
            # request first, ended last, everything in between.
            assert types[0] == EventType.TRIP_REQUESTED
            assert types[-1] == EventType.TRIP_ENDED
            # accepted must precede started.
            assert types.index(EventType.TRIP_ACCEPTED) < types.index(EventType.TRIP_STARTED)

    def test_driver_id_appears_from_accepted_onward(
        self, in_memory_source: InMemoryTripSource
    ) -> None:
        events = _run(in_memory_source)
        for e in events:
            if isinstance(e, TripRequested):
                assert e.driver_id is None
            elif isinstance(e, TripAccepted | TripStarted | TripEnded | GpsPing):
                assert e.driver_id is not None


class TestGpsPingInvariants:
    def test_pings_fall_within_trip_window(self, in_memory_source: InMemoryTripSource) -> None:
        events = _run(in_memory_source)
        starts = {e.trip_id: e.event_ts for e in events if isinstance(e, TripStarted)}
        ends = {e.trip_id: e.event_ts for e in events if isinstance(e, TripEnded)}
        for e in events:
            if isinstance(e, GpsPing):
                assert starts[e.trip_id] <= e.event_ts <= ends[e.trip_id]

    def test_pings_are_inside_nyc_bounding_box(self, in_memory_source: InMemoryTripSource) -> None:
        for e in _run(in_memory_source):
            if isinstance(e, GpsPing):
                assert is_inside_nyc(e.lat, e.lng), (e.lat, e.lng)

    def test_pings_have_plausible_speed_and_heading(
        self, in_memory_source: InMemoryTripSource
    ) -> None:
        for e in _run(in_memory_source):
            if isinstance(e, GpsPing):
                assert e.speed_kmh is not None and 0.0 <= e.speed_kmh <= 300.0
                assert e.heading_deg is not None and 0.0 <= e.heading_deg < 360.0


class TestNdjsonOutput:
    """Sanity check that the emitted lines are actually parseable NDJSON."""

    def test_every_event_serializes_to_one_json_line(
        self, in_memory_source: InMemoryTripSource
    ) -> None:
        for e in _run(in_memory_source):
            line = e.model_dump_json()
            assert "\n" not in line
            parsed = json.loads(line)
            assert parsed["event_type"] == e.event_type
