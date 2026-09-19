# Phase 5a — Gold marts (active trips + demand by borough)

## Goal

Aggregate the enriched `silver_gps_matched` stream from Phase 4b
into two business-ready gold marts, and lay down the shared
geospatial helper (borough attribution) that all gold marts will
reuse.

## What this phase delivers

- **`src/rides_telemetry/geo/spark.py`** — `borough_of_expr(lat_col,
  lng_col)` returns a Spark `Column` that classifies a coordinate
  to its nearest NYC borough centroid. Pure Spark SQL (no Python
  UDF overhead), pushed down into Catalyst.
- **`src/rides_telemetry/gold/`** — new package containing:
  - `schemas.py` — `GOLD_ACTIVE_TRIPS_SCHEMA`,
    `GOLD_DEMAND_SCHEMA`, and the tunable window sizes
    (`ACTIVE_TRIPS_WINDOW`, `DEMAND_WINDOW`, `DEMAND_WATERMARK`).
  - `active_trips.py` — `stream_active_trips_mart`: rolling
    per-borough per-minute count of `IN_PROGRESS` trips.
  - `demand.py` — `stream_demand_mart`: tumbling 5-min windows with
    distinct pings, trips, drivers per borough.
  - `__main__.py` — `python -m rides_telemetry.gold --mart {active_trips,demand_5min}`.
- **Gold tables** (physical layout):
  ```
  lakehouse/warehouse/gold_active_trips_now/           # 1-min per-borough snapshots
  lakehouse/warehouse/gold_demand_by_borough_5min/     # 5-min tumbling windows
  lakehouse/_checkpoints/gold_active_trips/            # streaming checkpoint
  lakehouse/_checkpoints/gold_demand/                  # streaming checkpoint
  ```
- **Tests**:
  - `tests/gold/test_schemas.py` — 7 hermetic tests (0.1 s): schema
    shape, count-column types (int vs long), window-config sanity,
    borough vocabulary lock.
  - `tests/gold/test_streaming.py` — 13 `spark`-marked tests (~8 s):
    borough attribution (Manhattan/Brooklyn/EWR centroids,
    outside-NYC → NULL, all 5 boroughs classified); mart shape
    (schema, distinct counting, cross-borough grouping,
    multi-window aggregation).

## Borough attribution

`borough_of_expr` builds a `CASE WHEN` chain that computes scaled
squared distance from a `(lat, lng)` to each of the six centroids
in `NYC_BOROUGHS` and returns the name of the nearest one. Distances
are in km² using the local-metric approximation (1° lat ≈ 111 km,
1° lng ≈ 84 km at NYC's latitude), which is accurate enough for
Voronoi-style nearest-neighbor classification within a single metro.

The output is `NULL` for coordinates outside `NYC_BBOX` — defensive
against a stray non-NYC row, though silver already filters those.

**Why not H3?** H3 hex zones are the "right" answer for production
geospatial. But they add a Java dependency (`uber-h3`) and pin us
to specific resolution choices before we've measured what
granularity gold consumers actually want. Phase 6 replaces this
with H3 once we have signal from surge / matching about the right
zone size — swapping is a one-line change because the interface
is designed to look the same:

```python
# Phase 5: borough (~5 km cells)
.withColumn("zone", borough_of_expr("lat", "lng"))

# Phase 6: H3 hex resolution 8 (~460 m edge)
.withColumn("zone", h3_index_of_expr("lat", "lng", resolution=8))
```

## Mart 1: `gold_active_trips_now`

```text
silver_gps_matched
    → withWatermark("event_ts", 10 min)
    → filter(trip_status == "IN_PROGRESS")
    → borough = borough_of_expr(lat, lng)
    → filter(borough IS NOT NULL)          # defensive
    → groupBy(window(event_ts, 1 min), borough)
    → agg(approx_count_distinct(trip_id) as active_trips)
    → writeStream.format("delta").outputMode("append")
```

**Why distinct-count `trip_id`, not `count(*)`?** Silver matched can
emit duplicate `event_id`s when the underlying trip fact row was
MERGE-updated between windows (see the 4b doc — the trip stream
reads with `ignoreChanges=true`). Distinct counting sidesteps that.

**Why the `approx` variant?** Spark Structured Streaming does not
support exact `countDistinct` inside a streaming aggregation
("Distinct aggregations are not supported on streaming
DataFrames/Datasets"). `approx_count_distinct` uses HyperLogLog
with a ~5% relative error by default — for cardinalities under ~100
(the realistic per-borough per-minute range) it's essentially
exact, and at higher cardinalities the error is well inside the
noise floor of a surge signal. The alternative — `foreachBatch`
with a `MERGE INTO` on partial windows — trades exact counts for
a much more complex writer and per-batch state, which isn't worth
it for a surge input.

**Why append + tumbling windows?** A row is emitted for a window
only once the watermark has passed its end — giving us exactly-once
gold rows with no MERGE needed and no partial-window rewrites. The
tradeoff is latency: a 1-min window with a 10-min watermark means
gold rows for minute X land ~X + 10 min. Fine for a demo; production
tightens the watermark once upstream latency is measured.

## Mart 2: `gold_demand_by_borough_5min`

```text
silver_gps_matched
    → withWatermark("event_ts", 10 min)
    → borough = borough_of_expr(lat, lng)
    → filter(borough IS NOT NULL)
    → groupBy(window(event_ts, 5 min), borough)
    → agg(
        approx_count_distinct(event_id) as ping_count,
        approx_count_distinct(trip_id)   as distinct_trips,
        approx_count_distinct(driver_id) as distinct_drivers,
      )
    → writeStream.format("delta").outputMode("append")
```

**Interpretation**:
- `distinct_trips` — how many trips reported at least one ping in
  this window (demand signal).
- `distinct_drivers` — how many drivers reported at least one ping
  (supply signal).
- Ratio `distinct_trips / distinct_drivers` — coarse surge input.
  Above 1 means more active riders than drivers can comfortably
  service; below 1 means surplus supply.

Phase 5b will add proper demand accounting (requested trips, not
just started+active) and match this against a surge multiplier
curve.

## Running it

```bash
# Prereqs: silver matched already populated (from phase 4b).
docker compose up -d  # only if you also want to re-run producer -> bronze -> silver

# Both marts, bounded (drain what's there and exit):
python -m rides_telemetry.gold --mart active_trips --once -v
python -m rides_telemetry.gold --mart demand_5min  --once -v
```

Verify the marts:

```python
from deltalake import DeltaTable

active = DeltaTable("lakehouse/warehouse/gold_active_trips_now").to_pandas()
print(active.groupby("borough").active_trips.sum())

demand = DeltaTable("lakehouse/warehouse/gold_demand_by_borough_5min").to_pandas()
print(demand[["window_start","borough","distinct_trips","distinct_drivers"]].head())

# Surge signal (trips-per-driver) per borough over time:
demand["surge_input"] = demand.distinct_trips / demand.distinct_drivers.clip(lower=1)
print(demand.groupby("borough").surge_input.describe())
```

## Tests

```bash
pytest tests/gold/                     # hermetic — 7 tests, 0.1 s
pytest -m spark tests/gold/            # spark-marked — 13 tests, ~8 s
```

## What's deferred to Phase 5b

- **Fraud signals** — a `gold_fraud_teleport` mart that flags GPS
  pings whose implied speed (Haversine distance / time between
  consecutive pings) exceeds a plausibility threshold, and a
  `gold_fraud_dual_trip` mart that flags drivers with pings from
  two `IN_PROGRESS` trips within the same window.

## What's deferred to Phase 6

- **H3 hex zones** replace nearest-centroid borough attribution
  (~5 km cells) with fine-grained H3 cells (resolution 8 ≈ 460 m
  edges). Same gold-mart interface, higher spatial precision.

## Batch replay vs live streaming — a subtle artifact

If you run the entire bronze → silver → gold pipeline on a fixed
historical NYC TLC batch (which is exactly what our
``docker compose`` demo does), **`gold_active_trips_now` will be
empty**. This isn't a bug — it's a stateful-pipeline artifact:

1. Bronze lifecycle has all 5 events for all 20 trips.
2. The silver-trips MERGE processes them in a **single batch**.
3. Silver_trip_facts v1 immediately holds every trip as
   ``status='COMPLETED'`` — there's no snapshot at which a trip
   was ever transiently ``IN_PROGRESS``.
4. Silver_gps_matched joins pings against these completed trip
   rows and inherits ``trip_status='COMPLETED'`` for every ping.
5. The active-trips filter (``trip_status = 'IN_PROGRESS'``) then
   matches zero rows.

To prove the mart shape is correct against realistic streaming
input, force ``trip_status = IN_PROGRESS`` on the matched table in
memory and re-run the shape function:

```python
from pyspark.sql import functions as F
from rides_telemetry.spark import build_spark_session, SparkSessionConfig
from rides_telemetry.gold.active_trips import shape_active_trips

spark = build_spark_session(SparkSessionConfig())
matched = spark.read.format("delta").load("lakehouse/warehouse/silver_gps_matched")
faked = matched.withColumn("trip_status", F.lit("IN_PROGRESS"))
shape_active_trips(faked).orderBy("window_start", "borough").show(20, False)
# → 215 rows across 4 boroughs, 1–5 concurrent trips per minute
```

In a live pipeline with the phase-2 producer running (trips
lifecycle events streamed with real inter-event delays), the mart
populates naturally because silver_trip_facts progresses through
its state machine over time.

## Gotchas

- **Structured Streaming forbids exact `countDistinct`.** Any
  aggregation using distinct counting must be either
  `approx_count_distinct` (what we do) or evaluated inside a
  `foreachBatch` handler (batch mode allows exact distinct). We
  chose HLL for simplicity — see the mart sections above.
- **`current_timestamp()` in the projection is evaluated at plan
  time, not row time.** This is a Spark quirk. It's fine for
  `gold_processed_ts` (roughly "when the batch ran") but if you
  ever need per-row wallclock, use `expr("current_timestamp()")`
  inside a UDF instead.
- **`shape_active_trips` uses `withWatermark` even in batch mode**
  (via unit tests). That's a no-op in batch — safe to include so
  the batch/streaming code paths stay identical.
