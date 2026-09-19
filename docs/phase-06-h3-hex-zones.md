# Phase 6 — H3 hex zones

## Goal

Upgrade the Phase 5a operational marts from **coarse borough
attribution** (~5 km cells, 6 total) to **fine-grained H3 hex
zones** (~461 m edges, ~0.74 km² per cell, ~2000 populated cells
per hour in the demo data) — the granularity real surge and
matching engines need.

## Design at a glance

- **Parallel marts, not schema swap.** Phase 5a's borough marts
  (`gold_active_trips_now`, `gold_demand_by_borough_5min`) stay
  untouched — dashboards and human operators want the coarse
  rollup. Phase 6 ships **parallel H3 marts**
  (`gold_active_trips_h3_now`, `gold_demand_by_h3_5min`) for
  surge / matching consumers who need street-scale precision.
- **`borough` column travels alongside `h3_r8`** in the H3 marts —
  every res-8 hex is entirely within one borough, so a
  per-borough rollup is a cheap `groupBy` on top of the H3 mart.
  This gives consumers a single source of truth with drill-down
  built in.
- **Vectorised `pandas_udf`** for H3 indexing. There's no
  native-Java H3 implementation in the stock pyspark stack;
  Sedona would be the production choice but adds Java infra.
  `pandas_udf` is Arrow-batched with bounded Python overhead —
  the right point on the trade-off curve for a demo lakehouse.

## What this phase delivers

### New in `src/rides_telemetry/geo/spark.py`

- **`h3_index_of_expr(lat_col, lng_col, resolution=8)`** — returns
  a `Column` of `StringType` holding the H3 cell ID (15-char lower
  hex). `NULL` in either input propagates to `NULL`. Uses an
  `@lru_cache`-memoised `pandas_udf` factory so exactly one UDF
  is registered per unique resolution per SparkSession.

### Extended `src/rides_telemetry/gold/schemas.py`

- `H3_RESOLUTION = 8` — the phase-6 default, documented with the
  full resolution/edge/area reference table.
- `GOLD_ACTIVE_TRIPS_H3_SCHEMA`
- `GOLD_DEMAND_H3_SCHEMA`

Both schemas keep `borough` alongside `h3_r8` for drill-through.

### New marts in `src/rides_telemetry/gold/`

- `active_trips_h3.py` — `stream_active_trips_h3_mart`
- `demand_h3.py` — `stream_demand_h3_mart`

Both mirror the Phase 5a marts exactly except for the `groupBy`
dimensions (`h3_r8, borough` instead of `borough`).

### CLI

```bash
python -m rides_telemetry.gold --mart active_trips_h3 --once -v
python -m rides_telemetry.gold --mart demand_h3_5min  --once -v
```

### Tests (+21, total 140 in the suite)

| Test file | What | Count | Speed |
| --- | --- | --- | --- |
| `tests/gold/test_schemas.py` (extended) | Hermetic — `H3_RESOLUTION == 8`, resolution is in a sane band, H3 schemas have `h3_r8`+`borough` cols with correct nullability, **schema parity between borough and H3 marts** (same measure cols) | +8 | 0.1 s |
| `tests/gold/test_h3.py` (new) | `spark`-marked — H3 UDF (Times Sq / JFK cross-checked against pure-Python `h3.latlng_to_cell`, NULL propagation, nearby pings share a cell, far pings don't, different resolutions differ), mart shape (schema, nearby pings aggregate, far pings split, completed trips excluded), **H3 → borough rollup identity check** | +13 | ~10 s |

## Why H3 at all — the surge-signal argument

The Phase 5a demand mart says: "Manhattan has 5000 distinct trips
in this 5-min window." Useful for a dashboard, useless for a
surge engine — Manhattan is 60 km². The pricing needs to be
different in Times Sq than in Inwood.

The Phase 6 demand-h3 mart says: "Hex `882a100d67fffff` (Times Sq,
~0.74 km²) has 47 distinct trips in this window; hex
`882a100d0dfffff` (Central Park, same area) has 3." Now a
matching engine can place a surge multiplier per hex, per window,
directly from the mart.

Same pipeline, ~2000× more spatial precision.

## The `pandas_udf` trade-off

There is no native-Spark implementation of H3. Options:

| Approach | Speed | Setup cost | Notes |
| --- | --- | --- | --- |
| Python `udf` | 1× (slow) | trivial | per-row Python serialisation |
| `pandas_udf` (**this**) | ~10-50× vs `udf` | trivial (`pip install h3`) | Arrow batches; Python-loop inside batch is bounded |
| Sedona `ST_H3CellIDs` | ~500× vs `udf` | ships as Maven JAR, shifts stack to Sedona | correct production choice |

For our demo scale (22,697 pings), `pandas_udf` adds < 100 ms
overhead across the whole batch — invisible against Spark's
startup cost. Production ride-hail workloads (millions of pings
per hour) would justify the Sedona migration.

## Batch-replay artifacts (same as Phase 5a)

- `gold_active_trips_h3_now` is empty on a batch replay for the
  same reason as its borough peer: silver_trip_facts' single-
  batch MERGE never leaves trips in `IN_PROGRESS` transiently.
  Proven correct by unit tests and the "force `IN_PROGRESS`"
  one-liner from the Phase 5a doc, which now works identically:
  ```python
  from rides_telemetry.gold.active_trips_h3 import shape_active_trips_h3
  faked = matched.withColumn("trip_status", F.lit("IN_PROGRESS"))
  shape_active_trips_h3(faked).show(20, False)
  ```

## Running it

```bash
# Prereq: silver_gps_matched populated (phase 4b).
python -m rides_telemetry.gold --mart active_trips_h3 --once -v
python -m rides_telemetry.gold --mart demand_h3_5min  --once -v
```

Inspect the marts:

```python
from deltalake import DeltaTable

d = DeltaTable("lakehouse/warehouse/gold_demand_by_h3_5min").to_pandas()
print(f"{len(d)} rows, {d.h3_r8.nunique()} distinct hexes across {d.borough.nunique()} boroughs")

# Top 10 busiest hexes overall:
print(d.groupby(["h3_r8","borough"]).distinct_trips.sum().nlargest(10))

# Borough-level rollup identity check (should equal the borough mart):
by_borough = d.groupby(["window_start","borough"]).distinct_trips.sum()
print(by_borough.head(10))
```

For a live map, the `h3` library gives you cell boundaries:

```python
import h3
row = d.iloc[0]
print(h3.cell_to_boundary(row.h3_r8))  # [(lat, lng), ...] — hex vertices
print(h3.cell_area(row.h3_r8, unit="km^2"))  # ~0.74 for res 8
```

## Distinct counts don't compose across hex boundaries

**This is important.** A trip that moves across hex boundaries
during a 5-min window will produce pings in multiple hexes, so
it gets counted *once per hex* in `gold_demand_by_h3_5min`.
Summing `distinct_trips` from the H3 mart over-counts vs the
borough mart.

Concrete example from the end-to-end run: window
`2024-01-01 00:15:00`, Manhattan — the borough mart says
`distinct_trips = 4`, the H3 mart summed to borough says
`distinct_trips = 15`. Not a bug — those same 4 trips crossed
~15 hexes total during that 5-min window.

For a true per-borough distinct count from the H3 mart you have
two options:

1. **Prefer the borough mart** for borough-level queries (it's
   there for a reason — cheap and exact).
2. **Materialise trip IDs.** If you must query "how many distinct
   trips in Manhattan?" from an H3 source, keep `collect_set(trip_id)`
   alongside the count and roll up with a `size(array_distinct(flatten(...)))`.
   Or use HyperLogLog sketch objects (`hll_sketch_agg` in Spark 3.5+)
   which *do* compose via `hll_union_agg`.

The H3 marts as shipped are optimised for **per-hex** queries
(surge, matching). Cross-hex aggregation is a valid use case but
requires either sketch-based columns (deferred) or falling back
to the borough mart.

## Gotchas

- **`pandas_udf` registration is per-SparkSession.** Recreating
  the UDF on every call would leak session state. The
  `@lru_cache` on the factory ensures exactly one UDF is
  registered per resolution — call the same resolution twice
  and you get the same UDF, not a new one.
- **`h3-py` v4 has no vectorised scalar API.** The `pandas_udf`
  loops in Python inside each Arrow batch. For >100k rows per
  batch, the loop overhead becomes measurable — switch to
  Sedona or `h3-pandas` at that scale.
- **`H3_RESOLUTION` is a module constant, not a runtime knob.**
  Making it a CLI arg would require re-registering the UDF and
  splitting the mart schemas by resolution. If you need multiple
  resolutions in the same warehouse, add a new mart module
  (`demand_h3_r9.py`) rather than parameterising.
- **`h3` package is in `[spark]` extra**, not `[dev]`. Gold jobs
  need Spark anyway, so the coupling is natural. CI installs
  `[dev,generator,producer,spark]` which pulls it in.

## Where this leaves us

After Phase 6, gold has **six marts**:

| Mart | Grain | Purpose |
| --- | --- | --- |
| `gold_active_trips_now` | borough × 1 min | dashboards, human operators |
| `gold_demand_by_borough_5min` | borough × 5 min | high-level demand tracking |
| `gold_fraud_teleport` | per-pair | GPS-spoof alerts |
| `gold_fraud_dual_trip` | driver × 5 min | account-sharing alerts |
| `gold_active_trips_h3_now` | H3 res 8 × 1 min | matching, per-hex surge |
| `gold_demand_by_h3_5min` | H3 res 8 × 5 min | street-level demand |

All six consume `silver_gps_matched`, so adding a seventh
(borough-level fraud rollup, H3-level fraud, per-pickup-zone
matching) is again a schema + shape function + CLI switch.

**Phase 7** starts the platform side: containerising the
producer, standing up a Kubernetes deployment, and wiring in
consumer lag / Delta commit metrics via Prometheus + Grafana.
