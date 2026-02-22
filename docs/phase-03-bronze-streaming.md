# Phase 3 — Bronze streaming (Kafka → Delta)

## Goal

Stand up the first Spark streaming layer of the lakehouse: subscribe
to both Kafka topics with Spark Structured Streaming and land every
event in a Delta bronze table with exactly-once semantics via
checkpointing.

## What this phase delivers

- **`src/rides_telemetry/spark/`** — `SparkSession` builder with:
  - auto-detected `JAVA_HOME` (Homebrew `openjdk@17` on macOS) so
    users don't have to touch `.zshrc`
  - Delta 3.2.1 + `spark-sql-kafka-0-10` 3.5.3 wired via
    `spark.jars.packages` (auto-fetched on first run, cached under
    `~/.ivy2/`)
  - Delta SQL extensions + Delta catalog registered so `MERGE INTO`
    and `CREATE TABLE ... USING delta` just work
  - warehouse dir under `./lakehouse/warehouse/` (gitignored)
  - shuffle partitions capped at 4 (default 200 is wasteful locally)
- **`src/rides_telemetry/bronze/`**:
  - `schemas.py` — Spark `StructType`s for the trips + GPS payloads,
    plus a shared Kafka envelope (`kafka_topic`, `kafka_partition`,
    `kafka_offset`, `kafka_timestamp`, `kafka_key`, `ingest_ts`,
    `raw_json`)
  - `streaming.py` — symmetric `stream_trips_to_bronze` /
    `stream_gps_to_bronze` functions that subscribe, parse, enrich,
    and write with exactly-once checkpointing
  - `__main__.py` — `python -m rides_telemetry.bronze --topic {trips,gps}`
    with `--once` for bounded runs
- **Bronze tables** (physical layout):
  ```
  lakehouse/warehouse/bronze_rides_lifecycle/   # Delta table
  lakehouse/warehouse/bronze_rides_gps/         # Delta table
  lakehouse/_checkpoints/bronze_trips/          # streaming checkpoint
  lakehouse/_checkpoints/bronze_gps/            # streaming checkpoint
  ```
- **Pytest markers** — `spark` and `integration` markers, deselected
  by default so `pytest` stays fast and Docker-free. Run explicitly:
  `pytest -m spark` or `pytest -m integration`.
- **Tests**:
  - `tests/bronze/test_schemas.py` — hermetic consistency test that
    every pydantic field has a matching Spark column (and vice versa)
  - `tests/bronze/test_streaming.py` — `spark`-marked test that
    exercises the shaping logic end-to-end through a real Spark
    session with a synthetic Kafka source row

## Topology

```text
┌─────────────────┐  Kafka wire protocol (JSON)
│   Redpanda      │   rides.trips.v1
│   (phase 2)     │   rides.gps.v1
└────────┬────────┘
         │ readStream.format("kafka")
         ▼
┌────────────────────────────────────────┐
│   Spark Structured Streaming           │
│   - from_json(value, payload_schema)   │
│   - + kafka_topic / partition / offset │
│   - + ingest_ts                        │
│   - + raw_json (lossless replay)       │
│   - checkpoint = lakehouse/_checkpoints│
└────────┬───────────────────────────────┘
         │ writeStream.format("delta")
         ▼
┌────────────────────────────────────────┐
│  Delta (bronze layer)                  │
│   bronze_rides_lifecycle               │
│   bronze_rides_gps                     │
│   append-only, ACID, time-travel       │
└────────────────────────────────────────┘
```

## Bronze schema

Both tables share the Kafka envelope columns; the payload columns
mirror the pydantic models 1:1. All lifecycle-subtype-specific
fields (`driver_id`, `trip_distance_km`, `fare_amount_usd`,
`cancelled_by`) are nullable in bronze — silver enforces the
per-event-type invariants in phase 4.

| Layer | Columns |
|-------|---------|
| **Payload (parsed)** | `event_id`, `event_ts`, `event_type`, `trip_id`, `rider_id`, `driver_id?`, `pickup/dropoff_lat/lng`, `trip_distance_km?`, `fare_amount_usd?`, `cancelled_by?` (trips) / `lat`, `lng`, `speed_kmh?`, `heading_deg?`, `accuracy_m?` (gps) |
| **Kafka envelope** | `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `kafka_key`, `ingest_ts`, `raw_json` |

## Design decisions

- **Delta over Parquet** for bronze even though we only append:
  ACID transactions, schema enforcement, time-travel (`VERSION AS OF`),
  Z-ORDER for later analytical scans, and `MERGE INTO` for silver
  dedup in phase 4. The bronze storage overhead vs raw Parquet is
  ~1% — Delta wins on every non-storage axis.

- **Raw JSON preserved in `raw_json`.** If a pydantic model evolves in
  a way the current bronze schema can't express, silver can re-parse
  from `raw_json` without going back to Kafka. Kafka retention is
  finite; Delta retention isn't (until you explicitly `VACUUM`).

- **`ingest_ts` distinct from `event_ts` and `kafka_timestamp`.**
  Three-clock model: `event_ts` (business time — when the event
  happened), `kafka_timestamp` (broker append time), `ingest_ts`
  (bronze write time). Silver's watermarks use `event_ts`; latency
  SLOs use `ingest_ts - event_ts`; diagnosing "why did this row
  land late" uses all three.

- **`mergeSchema` is *off*.** Schema drift in bronze must be
  explicit. Adding a payload field means editing
  `bronze/schemas.py`, updating tests, and shipping — not letting
  Spark silently invent columns. The schema-consistency test
  catches drift the other direction (pydantic added, Spark forgot).

- **Trigger defaults to `processingTime="5 seconds"`.** Continuous-ish
  micro-batches suit a laptop demo. `--once` flips to
  `Trigger.AvailableNow` for a bounded drain — ideal for smoke tests
  and CI-style verification.

- **`spark` and `integration` pytest markers, deselected by default.**
  `pytest` stays a ~0.2 s command. `pytest -m spark` pays the Spark
  startup cost (~15 s) when you actually want to test Spark.

- **SparkSession builder auto-detects Homebrew JDK.** One less step
  in the "clone the repo, run the code" flow. On Linux / CI we
  require `JAVA_HOME` to be set externally.

## Exactly-once story

Spark Structured Streaming + Delta gives us exactly-once *from the
producer's committed offset onwards*. Two ingredients:

1. **Kafka offsets are committed to the checkpoint atomically with
   the Delta write.** If the write fails mid-batch, the next run
   re-reads the same offsets. If the write succeeds, the offsets
   advance and won't be re-read.
2. **Delta writes are atomic and transactional.** A batch either
   commits fully or not at all — there's no partial state to clean up.

Duplicate messages *upstream* of bronze (e.g. from a producer retry
with `enable.idempotence=false`) will still land as duplicates —
silver's dedup in phase 4 handles that using `event_id`.

## How to run

```bash
# 1. Prereqs
brew install openjdk@17      # macOS; Ubuntu: apt install openjdk-17-jdk
pip install -e ".[dev,generator,producer,spark]"

# 2. Start local Kafka
docker compose up -d

# 3. Fill it with real events
python -m rides_telemetry.producer \
    --month 2024-01 --max-trips 50 --seed 42 \
    --use-system-trust-store -v

# 4. Drain into bronze once (short-lived job)
python -m rides_telemetry.bronze --topic gps   --once -v
python -m rides_telemetry.bronze --topic trips --once -v

# 5. Inspect the Delta tables
python - <<'PY'
from rides_telemetry.spark import build_spark_session
spark = build_spark_session()
spark.read.format("delta").load("lakehouse/warehouse/bronze_rides_gps").show(5, truncate=False)
spark.sql("DESCRIBE HISTORY delta.`lakehouse/warehouse/bronze_rides_gps`").show(truncate=False)
PY
```

## Testing

```bash
# Fast, hermetic — no Spark, no Docker
pytest -q                                      # ~0.2 s

# Spark-mode tests (needs JDK 17)
pytest -m spark -q                             # ~15-20 s (first run)

# Real Kafka end-to-end
docker compose up -d
pytest -m integration -q                       # <1 s
```

## Gotchas

- **First Spark run downloads ~150 MB of jars** (Delta + Kafka
  connector transitive deps) into `~/.ivy2/cache/`. Subsequent
  runs are instant. Behind a corporate TLS-inspecting proxy the
  JVM will fail to fetch from Maven Central with an
  `SSLHandshakeException` — pre-download the jars from a
  friendly mirror (e.g.
  `https://maven-central.storage-download.googleapis.com/maven2`)
  into `~/.ivy2/jars/`. The session builder auto-detects the
  directory and uses `spark.jars` instead of `spark.jars.packages`,
  bypassing Ivy entirely. Required jars for Phase 3:
  ```
  io.delta:delta-spark_2.12:3.2.1
  io.delta:delta-storage:3.2.1
  org.antlr:antlr4-runtime:4.9.3
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
  org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.3
  org.apache.kafka:kafka-clients:3.4.1
  org.apache.commons:commons-pool2:2.11.1
  org.lz4:lz4-java:1.8.0
  org.xerial.snappy:snappy-java:1.1.10.5
  org.slf4j:slf4j-api:2.0.7
  org.apache.hadoop:hadoop-client-runtime:3.3.4
  org.apache.hadoop:hadoop-client-api:3.3.4
  com.google.code.findbugs:jsr305:3.0.0
  ```
- **`docker compose down -v`** wipes Kafka messages *and* the
  Delta checkpoints will point at nonexistent offsets on restart.
  Clean both: `docker compose down -v && rm -rf lakehouse/`.
- **Spark UI** on <http://localhost:4040> is invaluable while a
  streaming job runs. Streaming tab shows batch durations,
  input rates, and watermarks (once phase 4 adds them).
- **JDK version matters.** Spark 3.5 supports JDK 8, 11, 17. We
  standardise on 17 — matches Databricks Runtime 15+ and Java 21
  isn't fully supported yet.
- **On macOS the Spark UI 4040 port may collide** with Docker
  compose's occasional port grabs. Bump via
  `spark.ui.port` in `SparkSessionConfig.extra_conf` if needed.

## References

- Spark Structured Streaming + Kafka: <https://spark.apache.org/docs/3.5.3/structured-streaming-kafka-integration.html>
- Delta Lake docs: <https://docs.delta.io/latest/>
- Exactly-once semantics deep dive: <https://spark.apache.org/docs/3.5.3/structured-streaming-programming-guide.html#fault-tolerance-semantics>
- Prior phase: [Phase 2 — Kafka producer](phase-02-kafka-producer.md)
