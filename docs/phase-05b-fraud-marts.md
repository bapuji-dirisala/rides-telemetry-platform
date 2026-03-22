# Phase 5b — Fraud marts (teleport + dual-trip driver)

## Goal

Ship the two fraud-signal gold marts that operators use to catch
GPS-spoofing and account-sharing patterns in the ride-hail data:

- **`gold_fraud_teleport`** — every GPS ping pair whose implied
  speed (Haversine / time) exceeds a plausibility threshold. Catches
  device swaps, spoofed coordinates, and other "impossible movement"
  patterns.
- **`gold_fraud_dual_trip`** — every `(driver_id, 5-min window)`
  where the driver reported from more than one distinct `trip_id`.
  Catches account sharing, multi-apping, and drivers who fail to
  end a trip before starting the next.

## What this phase delivers

- **`src/rides_telemetry/geo/spark.py`** — new
  `haversine_km_expr(lat1, lng1, lat2, lng2)` for pure-Spark
  great-circle distance. Reusable by any future geospatial mart.
- **`src/rides_telemetry/gold/schemas.py`** — extended with:
  - Fraud tunables: `MAX_PLAUSIBLE_TELEPORT_KMH`,
    `MIN_TELEPORT_TIME_DELTA_SECONDS`, `MIN_TELEPORT_DISTANCE_KM`,
    `FRAUD_WINDOW`, `FRAUD_WATERMARK`.
  - `GOLD_FRAUD_TELEPORT_SCHEMA` (per-ping-pair, unaggregated).
  - `GOLD_FRAUD_DUAL_TRIP_SCHEMA` (per driver-window, aggregated
    with a bounded `sample_trip_ids` array for operator context).
- **Two new streaming jobs**:
  - `fraud_teleport.py` — `stream_fraud_teleport_mart` (`foreachBatch` because `lag()` isn't allowed in streaming aggregations).
  - `fraud_dual_trip.py` — `stream_fraud_dual_trip_mart` (standard streaming aggregation with `approx_count_distinct` + `collect_set` + filter).
- **CLI**: `python -m rides_telemetry.gold --mart {fraud_teleport,fraud_dual_trip}` — same shape as Phase 5a.
- **Tests** (+31 total, now 126 in the suite):
  - `tests/gold/test_schemas.py` — +9 hermetic (fraud schema shape, tunable sanity, coupling checks with silver).
  - `tests/gold/test_fraud.py` — +15 `spark`-marked: Haversine correctness (zero distance, ~24 km Times Sq → JFK against a Python reference, symmetry), teleport detection (slow drivers not flagged, teleport ping flagged, stationary jitter not flagged, `lag()` correctly partitioned by `driver_id`), dual-trip detection (single trip not flagged, two trips same window flagged, two trips *different* windows not flagged, cross-driver not flagged, sample capped at 5 lex-first IDs).

## Mart 1: `gold_fraud_teleport`

### Pipeline

```text
silver_gps_matched
    → foreachBatch(batch_df, batch_id):
        window = partitionBy(driver_id).orderBy(event_ts)
        with_prev = select(*, lag(event_id/ts/trip_id/lat/lng) over window)
                    .filter(prev_event_id IS NOT NULL)
        with_signal = withColumn(
            distance_km = haversine_km_expr(prev_lat, prev_lng, lat, lng),
            time_delta_seconds = event_ts::double - prev_event_ts::double,
            implied_speed_kmh = (distance_km / time_delta_seconds) * 3600,
        )
        flagged = filter(
            implied_speed_kmh > 200
            AND time_delta_seconds >= 5
            AND distance_km >= 0.5
        )
        flagged.write.format("delta").mode("append")
```

### Why `foreachBatch`?

The core operation — `lag()` over `driver_id` ordered by
`event_ts` — is a **non-time-based window function** over the
stream. Spark Structured Streaming rejects those with:

```
Non-time-based windows are not supported on streaming DataFrames/Datasets
```

Batch mode allows them freely, so we drop the streaming source into
a `foreachBatch` handler and run the window + filter + write inside
that handler on the per-micro-batch DataFrame.

### Batch-boundary limitation

Each micro-batch computes `lag()` independently. If a driver's last
ping in batch N and their first ping in batch N+1 form a teleport
pair, that pair is **missed** — Spark won't compare them.

Two mitigations, in order of ambition:

1. **Cadence tuning.** With the default 30-s trigger and typical
   ping rates (~1 per 2–5 s), each driver contributes 6–15 pings
   per batch, so most teleports land inside a batch. For local
   `--once` runs, the entire source is drained into a single batch,
   so no boundary issues apply at all.
2. **Stateful processing** (deferred). Rewrite as
   `flatMapGroupsWithState` keyed on `driver_id`, carrying "last
   ping seen" across batches. Adds meaningful complexity —
   left for a later phase.

### Guardrails against false positives

| Guard | Threshold | Rationale |
| --- | --- | --- |
| Implied speed | > 200 km/h | Matches the single-ping threshold in silver (`MAX_PLAUSIBLE_SPEED_KMH`). NYC highway driving tops out well below this. |
| Time delta | ≥ 1 s | Realistic minimum device ping cadence. Sub-second gaps almost always mean a re-emitted GPS fix — the derived speed is meaningless. Deliberately *not* higher: shorter time deltas make a large displacement *more* suspicious, not less. |
| Distance | ≥ 0.5 km (500 m) | Primary jitter defense. "Driver at a red light, GPS drifts 100 m" isn't fraud even if the implied speed looks huge — 100 m is well under 500 m so the guard never triggers on jitter alone. |

### Output row (one per flagged pair)

Every flagged pair is emitted as its own row — no aggregation. The
schema keeps both pings' geometry (`lat/lng` and `prev_lat/prev_lng`)
so operators can eyeball the flagged pair on a map without a
follow-up query. Downstream rollups ("top offending drivers this
hour") are a cheap SQL aggregate on this mart.

## Mart 2: `gold_fraud_dual_trip`

### Pipeline

```text
silver_gps_matched
    → withWatermark("event_ts", 10 min)
    → groupBy(window(event_ts, 5 min), driver_id)
    → agg(
        approx_count_distinct(trip_id) as concurrent_trip_count,
        collect_set(trip_id)           as _all_trip_ids,
      )
    → filter(concurrent_trip_count > 1)
    → project(*, slice(sort_array(_all_trip_ids), 1, 5) as sample_trip_ids)
    → writeStream.format("delta").outputMode("append")
```

### `approx_count_distinct` — same story as Phase 5a

Spark Structured Streaming forbids exact `countDistinct` inside a
streaming aggregation. HLL's ~5% relative error is exact at the
tiny per-driver-per-window cardinalities we're targeting (1 in the
normal case, 2–3 in the fraud case). If a driver is legitimately
juggling >100 distinct trip IDs in 5 minutes, we have bigger
problems than an ~5% count error.

### `collect_set` state cost

`collect_set` is a `TypedImperativeAggregate`. Its per-group state
is proportional to the distinct trip IDs in that window, which is
single-digit even for fraud cases. We `slice(sort_array(...), 1, 5)`
in the projection so the output stays readable in a BI tool while
the raw set stays small in state.

### What it does not catch

- **Serial fraud** — driver ends trip 1, immediately claims to
  start trip 2 within seconds. That's a `fraud_teleport` signal
  (impossible movement between end-of-1 and start-of-2 pings).
- **GPS spoofing to a fixed location.** Falls out of scope for a
  pure trip-count aggregation. Phase 6 (H3 zones + baseline
  distributions) is where a "driver always pings from the same
  hex" signal lives.

## Running it

```bash
# Prereq: silver matched populated (from phase 4b).
python -m rides_telemetry.gold --mart fraud_teleport   --once -v
python -m rides_telemetry.gold --mart fraud_dual_trip  --once -v
```

Inspect the flagged pairs:

```python
from deltalake import DeltaTable

tp = DeltaTable("lakehouse/warehouse/gold_fraud_teleport").to_pandas()
print(tp[["driver_id","implied_speed_kmh","distance_km","time_delta_seconds"]]
      .sort_values("implied_speed_kmh", ascending=False).head(10))

dt = DeltaTable("lakehouse/warehouse/gold_fraud_dual_trip").to_pandas()
print(dt[["window_start","driver_id","concurrent_trip_count","sample_trip_ids"]])
```

### Expected output on the batch-replay dataset

The clean synthetic NYC TLC replay has neither impossible pings
(silver already dropped anything > 200 km/h) nor drivers on
multiple concurrent trips (each of our 20 trips has a unique
driver and no overlap). So **both marts are legitimately empty**
on the vanilla batch. The unit tests exercise the detection
paths against handcrafted inputs, and the phase doc includes an
"inject fraud" one-liner below.

## Injecting fraud for a live demo

```python
# Inject a teleport ping pair for driver d1 in silver_gps_matched,
# re-run the mart, and verify it fires.
from pyspark.sql import functions as F
from rides_telemetry.spark import build_spark_session, SparkSessionConfig
from rides_telemetry.gold.fraud_teleport import shape_fraud_teleport

spark = build_spark_session(SparkSessionConfig())
matched = spark.read.format("delta").load("lakehouse/warehouse/silver_gps_matched")

# Force half of driver d1's pings to teleport (jump 20° west)
faked = matched.withColumn(
    "lng",
    F.when(
        (F.col("driver_id") == "d1") & (F.col("event_ts").cast("long") % 2 == 0),
        F.col("lng") - 20.0,
    ).otherwise(F.col("lng")),
)
shape_fraud_teleport(faked).show(5, False)
```

## Tests

```bash
pytest tests/gold/                            # hermetic — 16 tests
pytest -m spark tests/gold/                   # spark-marked — 28 tests, ~15 s
```

## Gotchas

- **`foreachBatch` returns a `StreamingQuery`, but has no `.name`
  useful for JSON output** — the CLI's `last progress` log line
  works but the query name is inferred from the caller, not the
  usual auto-generated ID.
- **`sort_array` on `collect_set` output** — the projection sorts
  the trip IDs so the sample is deterministic. Without this, two
  runs of the same batch could emit different `sample_trip_ids`
  arrays (semantically equal, but not byte-equal), which trips
  up naive dedup downstream.
- **Timezone semantics** — same as everywhere else in the pipeline:
  the JVM sees `event_ts` as naive-UTC, so tests use naive
  `datetime`s to avoid the tz round-trip that bit us in Phase 4a.

## Where this leaves us

With Phase 5b done, the lakehouse has:

- **Two operational marts** (Phase 5a): active-trips-now,
  demand-by-borough.
- **Two fraud marts** (Phase 5b): teleport, dual-trip.

All four consume the same `silver_gps_matched` stream, so adding
a fifth mart is a matter of a schema block, a shape function,
and a CLI switch.

**Phase 6** replaces `borough_of_expr` with `h3_index_of_expr`
(H3 hex resolution 8 ≈ 460 m edges). Every mart's `groupBy` gets
finer geospatial granularity with a one-line change; the reusable
`haversine_km_expr` shipped here stays intact.
