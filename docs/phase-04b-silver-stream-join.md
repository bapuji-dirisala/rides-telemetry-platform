# Phase 4b — Silver stream-stream join (GPS ⋈ trip facts)

## Goal

Join the two silver streams from Phase 4a — `silver_rides_gps` and
`silver_trip_facts` — inside a bounded time window to produce a
third silver table: `silver_gps_matched`. Every row is a validated
GPS ping enriched with its trip's rider, driver, pickup/dropoff, and
status. Gold marts (surge, fraud, matching) build on this directly
instead of re-joining on every read.

This is the marquee streaming feature of the lakehouse: a real
**stream-stream inner join with watermarks and a time-range
predicate**, exactly the pattern the docs recommend for bounded state.

## What this phase delivers

- **`src/rides_telemetry/silver/matched.py`** —
  `stream_matched_to_silver` reads both silver Delta tables as
  streams, applies watermarks + a time-range join predicate, and
  writes to `silver_gps_matched` with checkpointed exactly-once
  semantics.
- **`schemas.py` additions**:
  - `SILVER_MATCHED_TABLE` and `SILVER_MATCHED_SCHEMA` — GPS
    columns plus trip context.
  - Join tunables: `MATCHED_JOIN_START_SLACK` (30 s),
    `MATCHED_JOIN_END_SLACK` (5 min), `MATCHED_TRIP_WATERMARK`
    (30 min). One place to tune the window.
- **CLI extension** — `python -m rides_telemetry.silver --stream matched`.
- **Tests**:
  - 4 hermetic schema tests (matched carries full GPS + trip
    context; `trip_started_ts` non-null; `event_id` is the row key).
  - 1 hermetic threshold test (join slacks are reasonable
    durations).
  - 9 `spark`-marked join tests covering: inside-window match,
    before-start drop, within-start-slack match, within-end-slack
    match, well-after-end drop, cancelled-trip exclusion,
    in-progress-trip fallback to `last_event_ts`, wrong-trip-id drop.

## How the join stays bounded

Structured Streaming allows stream-stream joins only when **both
sides carry a watermark AND the join predicate constrains the two
sides' timestamps to a bounded interval**. Both conditions hold:

```python
gps_w   = gps  .withWatermark("event_ts",       "10 minutes")
trips_w = trips.withWatermark("last_event_ts",  "30 minutes")

join_expr = F.expr("""
    g.trip_id = t.trip_id AND
    g.event_ts >= t.started_ts - interval 30 seconds AND
    g.event_ts <= COALESCE(t.ended_ts, t.last_event_ts) + interval 5 minutes
""")
```

Spark uses these to size the state store — old rows are evicted
once the watermark advances past their usable range. Without the
time constraint, state would grow unbounded and the JVM would OOM
within an hour of production traffic.

## `ignoreChanges` on the trip stream

`silver_trip_facts` is written with `MERGE INTO` (see phase 4a's
`trips.py`), which updates existing rows every time a new lifecycle
event for a trip arrives. Delta's streaming source treats an updated
file as "changes since checkpoint" and refuses to read it in
append-only streaming mode unless we opt in:

```python
.readStream.format("delta")
.option("ignoreChanges", "true")
.load(trips_path)
```

This means the join may see the same `trip_id` more than once as it
progresses through its state machine (a first version at `ACCEPTED`,
a later one at `COMPLETED`). That's fine — the join is deterministic
on `(trip_id, event_id)` and the time-range predicate makes the
output naturally unique per event.

## Trip filter: started, non-cancelled

Before the join, the trip stream is pre-filtered to trips that are
**started AND not cancelled**:

```python
trips.filter(
    F.col("started_ts").isNotNull()
    & F.col("cancelled_ts").isNull()
)
```

Rationale:

- **`started_ts IS NOT NULL`** — the join predicate references
  `started_ts`; there's nothing meaningful to match without it.
- **`cancelled_ts IS NULL`** — a ping received *after* a cancellation
  is almost certainly noise from a device that hadn't received the
  cancel signal yet. Carrying those into gold would pollute active-
  trip counts and surge signals.

This pre-filter also shrinks join state — trips excluded here never
enter Spark's buffered state on the trips side.

## Output: append-only

Every matched `(ping, trip)` row is a distinct observation. No
MERGE, no state store for dedup — just append. Downstream de-dup
happens on `event_id`, and `event_id` is already unique in silver
GPS (guaranteed by phase 4a's `dropDuplicates`).

## Running it

```bash
# Prereqs: bronze + silver GPS + silver trip facts already populated.
docker compose up -d

# Full silver chain in order:
python -m rides_telemetry.silver --stream gps     --once -v
python -m rides_telemetry.silver --stream trips   --once -v
python -m rides_telemetry.silver --stream matched --once -v
```

Verify the matched table:

```python
from deltalake import DeltaTable
m = DeltaTable("lakehouse/warehouse/silver_gps_matched").to_pandas()
print(m.shape, m.columns.tolist())
print(m.trip_status.value_counts())
print(m[["event_ts","trip_id","rider_id","driver_id","lat","lng","pickup_lat","pickup_lng","trip_status"]].head(3))
```

## Tests

```bash
pytest tests/silver/                          # hermetic — 17 tests, 0.1 s
pytest -m spark tests/silver/                 # spark-marked — 21 tests, ~9 s
```

## What's deferred to Phase 5 (gold)

- **`speed_kmh_smoothed`** — a rolling average of speed over the last
  N pings per trip. Belongs naturally in the surge / fraud marts
  where it's actually consumed, and computing it once at the mart
  level avoids paying the state cost in silver.
- **Active-trips-now snapshot** — a gold mart that counts trips in
  `IN_PROGRESS` per H3 zone. Feeds surge pricing.
- **Fraud signals** — e.g. driver reporting GPS pings from two
  active trips simultaneously.

## Gotchas

- **`shape_matched` uses `F.expr(...)`, not `F.col("g.event_ts") >= ...`**,
  because Spark's Column expressions can't reference the aliased
  DataFrame columns *and* SQL interval literals cleanly in one
  Python expression. The SQL string keeps it readable.
- **Batch mode drops the watermark's meaning.** In tests we call
  `shape_matched` against static DataFrames — the `withWatermark`
  calls are no-ops there. That's fine: the join predicate is what
  actually determines which rows match, and it works identically in
  batch and streaming mode.
- **Trip stream can emit each `trip_id` multiple times** (one per
  MERGE-updated version). The join's output may accordingly show the
  same `event_id` more than once if the trip's status changed
  between two windows in which the ping was observed. If that
  matters downstream, add `dropDuplicates(["event_id"])` with a
  watermark on the matched read.
