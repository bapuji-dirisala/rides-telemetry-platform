# Phase 4a — Silver streaming (bronze → clean facts)

## Goal

Promote the raw, replay-safe bronze Delta tables into business-usable
silver tables. Silver is where downstream consumers (surge marts,
fraud detectors, matching engines) actually build their queries — so
it has to give them **deduped**, **quality-filtered**, and
**business-shaped** rows.

## What this phase delivers

- **`src/rides_telemetry/silver/`** — two symmetric streaming jobs
  reading their respective bronze Delta table and writing a silver
  Delta table:
  - `stream_gps_to_silver` — row-level: watermark + dedup by
    `event_id`, quality filter, project to business columns.
  - `stream_trips_to_silver` — aggregation: fold the up-to-five
    lifecycle events per trip into one fact row via
    `foreachBatch` + Delta `MERGE INTO` (idempotent upsert).
- **`schemas.py`** — hand-written `StructType` for each silver table
  plus the tunable quality thresholds
  (`MAX_PLAUSIBLE_SPEED_KMH`, `MAX_ACCEPTABLE_ACCURACY_M`,
  `GPS_WATERMARK`, `TRIP_WATERMARK`) in one place.
- **`quality.py`** — reusable Spark `Column` predicates
  (`has_valid_coordinates`, `has_plausible_speed`,
  `has_usable_accuracy`, `gps_ping_is_valid`) so filter logic can
  be tested and reasoned about independently of streaming plumbing.
- **`__main__.py`** — `python -m rides_telemetry.silver --stream {gps,trips}`
  CLI, mirrors the bronze CLI's shape (with `--once` for bounded runs).
- **Silver tables** (physical layout):
  ```
  lakehouse/warehouse/silver_rides_gps/          # per-ping, deduped
  lakehouse/warehouse/silver_trip_facts/         # one row per trip
  lakehouse/_checkpoints/silver_gps/             # streaming checkpoint
  lakehouse/_checkpoints/silver_trips/           # streaming checkpoint
  ```
- **Tests**:
  - `tests/silver/test_schemas.py` — hermetic (0.1 s). Schema
    consistency: silver GPS covers every business field from bronze
    GPS, drops the Kafka envelope, has a `silver_processed_ts`;
    silver trip facts have every lifecycle timestamp + `status`;
    quality thresholds sit in sensible ranges.
  - `tests/silver/test_streaming.py` — `spark`-marked (~6 s).
    End-to-end shape tests for both `shape_silver_gps` and
    `shape_trip_facts_batch` against handcrafted DataFrames.
    Covers dedup, quality filter, the status state machine, and
    the terminal-state-wins rule.

## Silver GPS pipeline

```text
bronze_rides_gps  →  withWatermark("event_ts", "10 minutes")
                  →  dropDuplicates(["event_id"])
                  →  filter(gps_ping_is_valid())        # NYC bbox +
                                                        # speed cap +
                                                        # accuracy cap
                  →  select(business cols + silver_processed_ts)
                  →  writeStream.format("delta").outputMode("append")
```

### Why the watermark is on `event_ts`, not `ingest_ts`

`event_ts` is the *business* time the ping actually happened. Replay
scenarios (bronze reprocessed from Kafka, checkpoint rebuilt) push
`ingest_ts` forward but keep `event_ts` correct — watermarking on
business time makes silver replay-safe.

### Why append-only (no MERGE)

Every GPS ping is a distinct observation. There's nothing to update.
Append + dedup is enough for exactly-once semantics at rest, and it's
much cheaper than MERGE.

## Silver trip-facts pipeline

The interesting one. Bronze has **one row per lifecycle event** (up
to five per trip: requested → accepted → started → ended, or
requested → cancelled). Silver has **one row per trip**, with each
event contributing its own set of columns.

```text
bronze_rides_lifecycle  →  readStream (Delta source)
                        →  foreachBatch(upsert):
                             1. groupBy(trip_id).agg(...)
                                fold this batch's events into partial
                                trip-fact rows using
                                `max(when(event_type == X, col))`
                             2. Delta MERGE INTO silver_trip_facts
                                ON trip_id
                                WHEN MATCHED UPDATE SET
                                   * COALESCE per field
                                   * recompute status from merged ts
                                WHEN NOT MATCHED INSERT VALUES ...
```

### Why `foreachBatch` + MERGE

Delta's `MERGE INTO` is the canonical way to do idempotent upserts.
Each micro-batch is one atomic MERGE — a restart re-processes the
same batch and produces the same result. This is the pattern the
Databricks docs recommend for streaming upserts and it composes
cleanly with checkpointing.

The alternative — a streaming `groupBy(...).agg(...)` with
`outputMode("update")` — works but requires accumulating state in
the stream's state store rather than in Delta itself. Delta is the
better place for that state: cheaper, more auditable, time-travelable.

### Status state machine

Derived once during MERGE, so downstream code never re-runs the fold:

| Rule | Status |
|---|---|
| `cancelled_ts` is not NULL | `CANCELLED` (terminal) |
| `ended_ts` is not NULL | `COMPLETED` (terminal) |
| `started_ts` is not NULL | `IN_PROGRESS` |
| `accepted_ts` is not NULL | `ACCEPTED` |
| only `requested_ts` set | `PENDING` |

Terminal states win. If a `trip_started` arrives *after* a
`trip_cancelled` (out-of-order Kafka), the merged status stays
`CANCELLED` because the CASE checks the cancelled/ended timestamps
first.

### COALESCE strategy in the MERGE

For every field we choose one of two "which side wins" rules:

| Field group | Strategy | Rationale |
|---|---|---|
| Lifecycle timestamps (`*_ts`) | `COALESCE(t, s)` — existing wins | First-write-wins; a duplicate event shouldn't drift the timestamp |
| Geography (`pickup_lat`, etc.) | `COALESCE(t, s)` — existing wins | Same reasoning; pickup is set once at request time |
| Trip outcome (`trip_distance_km`, `fare_amount_usd`) | `COALESCE(t, s)` — existing wins | Ended-event value is authoritative |
| `driver_id` | `COALESCE(s, t)` — source wins if non-null | Legitimate to fill in a previously-unknown driver on a late `trip_accepted` |
| `first_event_ts` / `last_event_ts` | `LEAST` / `GREATEST` | Extend the observed window to the full batch |
| `status` | recomputed from merged timestamps | Terminal states dominate no matter which side has them |

## Running it

```bash
# Prereqs: producer already ran (so bronze has data) and Redpanda is up.
docker compose up -d

# Drain everything currently in bronze GPS into silver_rides_gps
python -m rides_telemetry.silver --stream gps --once -v

# Same for trip facts
python -m rides_telemetry.silver --stream trips --once -v
```

Verify the silver tables:

```python
from deltalake import DeltaTable  # pip install deltalake

gps = DeltaTable("lakehouse/warehouse/silver_rides_gps").to_pandas()
print(gps.shape, gps.columns.tolist())
print(gps[["event_ts", "trip_id", "lat", "lng", "speed_kmh"]].head())

trips = DeltaTable("lakehouse/warehouse/silver_trip_facts").to_pandas()
print(trips.status.value_counts())
print(trips[trips.status == "COMPLETED"][["trip_id", "trip_distance_km", "fare_amount_usd"]].head())
```

## Tests

```bash
pytest tests/silver/                          # hermetic — 12 tests, 0.1 s
pytest -m spark tests/silver/                 # spark-marked — 12 tests, ~6 s
```

## What's deferred to Phase 4b

- **Stream-stream join** between `silver_rides_gps` and
  `silver_trip_facts` to validate that each ping falls within an
  active trip's `[started_ts, ended_ts]` window. This is the
  marquee streaming feature (both sides watermarked, bounded state)
  and it makes gold-layer surge/fraud/matching queries trivial.
- **`speed_kmh_smoothed`** — a rolling average over the last N pings
  per trip, useful for surge/fraud signals. Needs a session window
  or user-defined state, which pairs naturally with the join work.

## Gotchas

- **JVM local timezone bleeds into TimestampType round-trips.** When a
  Python tz-aware `datetime` goes through `createDataFrame` and comes
  back via `collect()`, it's returned as a naive datetime in the
  JVM's local timezone. Tests use naive datetimes on both sides to
  sidestep the mismatch; production data always carries UTC on the
  wire (pydantic enforces it) and downstream consumers should apply
  `.dt.tz_localize("UTC")` on read if they need aware objects.
- **`spark.createDataFrame([row_dict])` with all-`None` optional
  fields raises `CANNOT_DETERMINE_TYPE`.** In tests, always pass
  `schema=BRONZE_TRIPS_SCHEMA` (or the equivalent for GPS) so Spark
  uses the declared schema instead of trying to infer from Python
  types. Production paths never hit this — Delta already has the
  schema.
- **`DeltaTable.createIfNotExists`** is required before the first
  `foreachBatch` MERGE — otherwise Delta raises "table not found" on
  the initial upsert. The trip-facts job handles this in
  `_ensure_silver_trips_table` before starting the stream.
