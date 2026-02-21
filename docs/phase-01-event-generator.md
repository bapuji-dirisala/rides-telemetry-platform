# Phase 1 — Synthetic ride event generator

## Goal

Produce a deterministic, offline stream of rides-platform telemetry
events (`TripRequested`, `TripAccepted`, `TripStarted`, `GpsPing`,
`TripEnded`) that later phases can feed into Kafka, Spark, and Delta.

Wherever possible, use *real* data instead of purely synthetic
generation. The stream should look and feel like a real ride-hailing
platform's Kafka output, not a toy loop.

## What this phase delivers

- **Event schemas** (pydantic v2, `src/rides_telemetry/events/`):
  - Lifecycle: `TripRequested`, `TripAccepted`, `TripStarted`,
    `TripEnded`, `TripCancelled`
  - Telemetry: `GpsPing`
  - Every event is frozen, JSON-serializable, and carries a shared
    `EventBase` envelope (`event_id`, `event_ts`, `event_type`)
- **Trip sources** (`src/rides_telemetry/sources/`):
  - `NycTlcTripSource` — streams real Uber/Lyft trips from the NYC TLC
    HVFHS parquet feed (no API key, no auth, ~50 MB/month)
  - `InMemoryTripSource` — a hand-rolled fake used by tests so CI never
    touches the network
- **Geography helpers** (`src/rides_telemetry/geo/`):
  - Borough centroids + area-uniform jitter for sampling plausible
    coordinates inside each of NYC's five boroughs (+ EWR)
- **Ride simulator** (`src/rides_telemetry/generator/`):
  - Consumes any `TripSource`
  - Derives request / accept / start / end timestamps from the real
    pickup/dropoff times in the source
  - Emits GPS pings at a configurable cadence (default 1 Hz) with
    interpolated coordinates and jittered speed / heading / accuracy
  - Deterministic given a seed
- **CLI**: `python -m rides_telemetry.generator` → NDJSON on stdout
- **Tests** (offline, hermetic): schema roundtrips, determinism,
  lifecycle ordering, GPS invariants, NDJSON parseability

## Data source

**NYC TLC High Volume For-Hire (HVFHS) trip records** —
<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

- Free, public, no API key
- Real Uber, Lyft, Via, and Juno trips at ~20M rows/month
- Rich lifecycle timestamps: `request_datetime`, `on_scene_datetime`,
  `pickup_datetime`, `dropoff_datetime`
- Post-2016, coordinates are anonymized to TLC taxi-zone IDs — Phase 1
  resolves each zone to its borough and jitters a point inside; Phase 6
  will swap this for proper zone centroids + H3 hex assignment

The simulator only reads six columns from the parquet (pickup ts,
dropoff ts, PU/DO location IDs, trip miles, fare) so pyarrow can push
column pruning down to the reader and memory stays flat.

## Event schemas

| Event | Fields (beyond envelope) |
|-------|--------------------------|
| `TripRequested` | `trip_id`, `rider_id`, `driver_id`=None, pickup/dropoff lat/lng |
| `TripAccepted` | + `driver_id` populated |
| `TripStarted` | (same as accepted) |
| `TripEnded` | + `trip_distance_km`, `fare_amount_usd` |
| `TripCancelled` | + `cancelled_by` ∈ {rider, driver, system} |
| `GpsPing` | `trip_id`, `driver_id`, `lat`, `lng`, `speed_kmh?`, `heading_deg?`, `accuracy_m?` |

Envelope (`EventBase`): `event_id: UUID`, `event_ts: datetime` (UTC,
tz-aware), `event_type: EventType`.

## Design decisions

- **Real data over pure synth.** The pitch of this whole project is
  "production-shaped telemetry" — starting from a real public dataset
  gives the downstream phases realistic skew, hotspots, and diurnal
  patterns for free, and makes the demo credible.

- **Coarse geography now, fine geography later.** Borough-level
  centroids are honest and cheap. Proper taxi-zone centroids need
  shapefile handling (geopandas / shapely) which is expensive and
  properly belongs in Phase 6 where H3 hexes get introduced anyway.

- **Deterministic given a seed.** Every RNG derives from a master
  `Random(config.seed)`. This is a hard requirement — Phase 3's dedup
  logic and Phase 4's stream-stream join tests need to replay the
  same events across runs.

- **No wall-clock dependency.** Timestamps come from source data, not
  `datetime.now()`. Reproducible replay of historical months is a
  first-class capability.

- **Straight-line interpolation for GPS.** Adding real road-following
  paths would require OSRM or Valhalla, which is Phase 6-tier work.
  Straight lines with small coordinate jitter are enough to test
  partitioning, watermarks, and geospatial SQL — which is what Phases
  2 – 5 need from this generator.

- **Pydantic v2 over dataclasses.** Runtime validation catches bad
  data at the producer boundary (before it enters Kafka), JSON
  serialization is built-in, and pydantic models expose a JSON Schema
  that converts cleanly to Avro / Protobuf when a Schema Registry
  lands in a later phase.

- **`generator` extra, not core deps.** The rides package still has
  zero required dependencies at install time. Only opting into the
  `generator` extra pulls in pydantic, pyarrow, and httpx.

## How to run

```bash
# One-time setup (Phase 0 tooling + generator deps)
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,generator]"

# Peek at three trips as NDJSON
python -m rides_telemetry.generator --month 2024-01 --max-trips 3 --seed 42 -v

# Save 100 trips as a fixture (~few thousand events)
python -m rides_telemetry.generator --max-trips 100 > sample.ndjson

# Run the test suite (offline, ~1 s)
pytest -q
```

Expected first-run behaviour: the CLI downloads
`fhvhv_tripdata_2024-01.parquet` (~50 MB) and `taxi_zone_lookup.csv`
into `data/nyc_tlc/` (gitignored), then streams events on stdout.
Subsequent runs use the cache and start instantly.

## Gotchas

- **First-run download is ~50 MB.** Cached under `data/nyc_tlc/`
  which is gitignored. Delete the cache dir to force a re-download.
- **TLC publishes with a ~2-month lag.** Trying `--month 2026-08`
  from mid-2026 will 404. `2024-01` is the safe default reference.
- **Coordinate resolution is borough-level.** Two consecutive pings
  in the same trip may look like they teleport a few km apart — that
  matches TLC's own zone-level anonymization and is fine for
  everything Phases 2 – 5 do with the stream.

## References

- NYC TLC trip record data: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
- HVFHS data dictionary:
  <https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf>
- Pydantic v2 docs: <https://docs.pydantic.dev/latest/>
- Prior phase: [Phase 0 — Repo scaffolding](phase-00-scaffolding.md)
