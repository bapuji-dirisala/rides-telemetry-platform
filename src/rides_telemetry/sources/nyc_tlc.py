"""NYC TLC High Volume For-Hire (Uber / Lyft) trip source.

Reads real ride-hailing trips from the NYC Taxi & Limousine Commission's
public parquet feed (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

Why this dataset:

- **Free, no API key, no auth.** Anyone can reproduce phase 1 output.
- **Real ride-hailing traffic.** HVFHS = "High Volume For-Hire Services"
  which is Uber, Lyft, Via, and Juno. That's a much better proxy for a
  ride-hailing platform than yellow taxis.
- **Rich lifecycle timing.** Each record has ``request_datetime``,
  ``on_scene_datetime``, ``pickup_datetime``, and ``dropoff_datetime``,
  so we can derive real ``TripRequested`` → ``TripAccepted`` →
  ``TripStarted`` → ``TripEnded`` timings instead of guessing offsets.

Coordinates: post-2016 TLC data is anonymized to taxi-zone IDs, not raw
lat/lng. Phase 1 resolves each zone to its borough and jitters a point
inside the borough. Phase 6 will swap this for proper zone centroids
and H3 hex assignment.

The parquet is ~500 MB uncompressed / ~50-80 MB on disk. We stream it
via pyarrow and only load the columns we need, so memory stays flat.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pyarrow.parquet as pq

from rides_telemetry.geo import NYC_BOROUGHS, Borough, jitter_point
from rides_telemetry.sources.base import RawTrip

_log = logging.getLogger(__name__)

# TLC serves parquet files off CloudFront. URL scheme is stable and
# documented on the TLC trip-record-data page.
_TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
_TLC_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

_MILES_TO_KM = 1.609344

# We only pull the columns we actually use — pyarrow can push this down
# to the parquet reader and skip the rest, which is a huge speedup on
# a ~30-column HVFHS file.
_HVFHS_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "base_passenger_fare",
]


@dataclass(frozen=True, slots=True)
class _ZoneLookup:
    """Maps a TLC LocationID to its borough (via a name lookup)."""

    id_to_borough: dict[int, Borough]

    @classmethod
    def from_csv(cls, path: Path) -> _ZoneLookup:
        by_id: dict[int, Borough] = {}
        with path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                loc_id = int(row["LocationID"])
                borough_name = row["Borough"].strip()
                borough = NYC_BOROUGHS.get(borough_name)
                if borough is None:
                    # "Unknown" and any future new borough names get skipped.
                    # We prefer dropping unmappable rows over inventing geometry.
                    continue
                by_id[loc_id] = borough
        return cls(id_to_borough=by_id)


class NycTlcTripSource:
    """Streams real HVFHS trips out of a monthly TLC parquet file.

    On first use, downloads the parquet + zone-lookup CSV into
    ``cache_dir`` and reuses them on subsequent runs.
    """

    def __init__(
        self,
        month: str,
        cache_dir: Path | str = "data/nyc_tlc",
        seed: int = 0,
        http_client: httpx.Client | None = None,
    ) -> None:
        """
        Parameters
        ----------
        month:
            Month to load, formatted ``"YYYY-MM"`` (e.g. ``"2024-01"``).
            TLC publishes monthly files with a ~2 month lag, so
            ``"2024-01"`` is the safest choice for a stable reference.
        cache_dir:
            Directory to store downloaded parquet + CSV files.
            Defaults to ``./data/nyc_tlc`` which is gitignored.
        seed:
            Seed for the coordinate-jitter RNG so a given (month, seed)
            pair always produces identical lat/lng values.
        http_client:
            Injectable ``httpx.Client`` for tests. If ``None`` a fresh
            client is created on demand.
        """
        self._validate_month(month)
        self.month = month
        self.cache_dir = Path(cache_dir)
        self.seed = seed
        self._http_client = http_client

        # Seeded RNGs, one per lat/lng coordinate, so that a fixed seed
        # yields the exact same coordinates for a given month.
        import random

        self._pickup_rng = random.Random(seed)
        self._dropoff_rng = random.Random(seed ^ 0xDEADBEEF)

    # ---- public ----------------------------------------------------------

    def iter_trips(self, limit: int | None = None) -> Iterator[RawTrip]:
        """Yield up to ``limit`` :class:`RawTrip` records in pickup-time order."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = self._ensure_parquet()
        lookup = _ZoneLookup.from_csv(self._ensure_zone_lookup())

        for i, trip in enumerate(self._iter_parquet(parquet_path, lookup)):
            if limit is not None and i >= limit:
                return
            yield trip

    # ---- internals -------------------------------------------------------

    @staticmethod
    def _validate_month(month: str) -> None:
        try:
            datetime.strptime(month, "%Y-%m")
        except ValueError as exc:
            raise ValueError(f"month must be formatted 'YYYY-MM', got {month!r}") from exc

    def _ensure_parquet(self) -> Path:
        target = self.cache_dir / f"fhvhv_tripdata_{self.month}.parquet"
        if target.exists() and target.stat().st_size > 0:
            _log.info("using cached parquet %s", target)
            return target
        url = f"{_TLC_BASE}/fhvhv_tripdata_{self.month}.parquet"
        self._download(url, target)
        return target

    def _ensure_zone_lookup(self) -> Path:
        target = self.cache_dir / "taxi_zone_lookup.csv"
        if target.exists() and target.stat().st_size > 0:
            return target
        self._download(_TLC_LOOKUP_URL, target)
        return target

    def _download(self, url: str, target: Path) -> None:
        _log.info("downloading %s -> %s", url, target)
        client = self._http_client or httpx.Client(timeout=60.0, follow_redirects=True)
        try:
            with client.stream("GET", url) as resp:
                resp.raise_for_status()
                # Write to a temp file first so a partial download can't
                # masquerade as a good cache hit on the next run.
                tmp = target.with_suffix(target.suffix + ".part")
                with tmp.open("wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
                tmp.replace(target)
        finally:
            if self._http_client is None:
                client.close()

    def _iter_parquet(self, path: Path, lookup: _ZoneLookup) -> Iterable[RawTrip]:
        parquet_file = pq.ParquetFile(path)
        for batch in parquet_file.iter_batches(batch_size=10_000, columns=_HVFHS_COLUMNS):
            # ``to_pylist`` gives us a list[dict] which is friendlier to
            # iterate than pyarrow's column-oriented view for this
            # relatively small row-at-a-time workload.
            for row in batch.to_pylist():
                trip = self._row_to_trip(row, lookup)
                if trip is not None:
                    yield trip

    def _row_to_trip(self, row: dict[str, object], lookup: _ZoneLookup) -> RawTrip | None:
        pu_id = row.get("PULocationID")
        do_id = row.get("DOLocationID")
        if not isinstance(pu_id, int) or not isinstance(do_id, int):
            return None
        pu_borough = lookup.id_to_borough.get(pu_id)
        do_borough = lookup.id_to_borough.get(do_id)
        if pu_borough is None or do_borough is None:
            return None

        pickup_ts = row.get("pickup_datetime")
        dropoff_ts = row.get("dropoff_datetime")
        if not isinstance(pickup_ts, datetime) or not isinstance(dropoff_ts, datetime):
            return None
        # TLC parquets store timestamps as naïve local time (America/New_York
        # in practice, but the file itself is tz-naïve). Attach UTC so every
        # event flowing through the pipeline is unambiguously tz-aware, in
        # line with the promise made by ``EventBase.event_ts``.
        if pickup_ts.tzinfo is None:
            pickup_ts = pickup_ts.replace(tzinfo=UTC)
        if dropoff_ts.tzinfo is None:
            dropoff_ts = dropoff_ts.replace(tzinfo=UTC)
        if dropoff_ts <= pickup_ts:
            return None
        # Guard against absurd durations (>4h) — likely data-quality issues.
        if (dropoff_ts - pickup_ts).total_seconds() > 4 * 3600:
            return None

        trip_miles = row.get("trip_miles") or 0.0
        fare = row.get("base_passenger_fare") or 0.0
        # pyarrow may hand us Decimal/float; coerce.
        trip_km = float(trip_miles) * _MILES_TO_KM  # type: ignore[arg-type]
        fare_usd = max(0.0, float(fare))  # type: ignore[arg-type]

        pickup_lat, pickup_lng = jitter_point(pu_borough, self._pickup_rng)
        dropoff_lat, dropoff_lng = jitter_point(do_borough, self._dropoff_rng)

        return RawTrip(
            pickup_ts=pickup_ts,
            dropoff_ts=dropoff_ts,
            pickup_lat=pickup_lat,
            pickup_lng=pickup_lng,
            dropoff_lat=dropoff_lat,
            dropoff_lng=dropoff_lng,
            trip_distance_km=trip_km,
            fare_amount_usd=fare_usd,
        )
