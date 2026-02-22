"""In-memory trip source, used by tests to avoid network I/O."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from rides_telemetry.sources.base import RawTrip


class InMemoryTripSource:
    """Yields a fixed sequence of pre-built :class:`RawTrip` records.

    Used in unit tests so we can assert on deterministic event streams
    without downloading a 50-MB parquet file in CI.
    """

    def __init__(self, trips: Sequence[RawTrip]) -> None:
        self._trips = tuple(trips)

    def iter_trips(self, limit: int | None = None) -> Iterable[RawTrip]:
        for i, trip in enumerate(self._trips):
            if limit is not None and i >= limit:
                return
            yield trip
