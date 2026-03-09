# rides-telemetry-platform

[![ci](https://github.com/bapuji-dirisala/rides-telemetry-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/bapuji-dirisala/rides-telemetry-platform/actions/workflows/ci.yml)
[![python](https://img.shields.io/badge/python-3.11-blue)](pyproject.toml)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Real-time telemetry pipeline for a ride-hailing platform. Millions of GPS
pings per minute plus ride lifecycle events, streamed through Kafka (with a
parallel Kinesis path), landed into a Delta lakehouse with exactly-once
semantics, and served as geospatial gold marts for surge pricing, fraud
detection, and driver-rider matching.

> **Status:** Phase 5a — First gold marts shipped. Borough attribution
> as a reusable Spark expression, `gold_active_trips_now` (per-borough
> per-minute count of in-progress trips), and
> `gold_demand_by_borough_5min` (tumbling 5-min windows with distinct
> pings / trips / drivers) are all live. Fraud signals land in 5b,
> then H3 hex zones replace nearest-centroid attribution in Phase 6.
> Twelve-phase roadmap below; every phase gets a dedicated design doc
> under [`docs/`](docs/).

## What this project demonstrates

- **Streaming ingestion** at production settings: topic partitioning by
  `trip_id`, keyed events for ordering guarantees, watermarks for
  late-arrival tolerance
- **Two ingestion paths side by side**: self-managed Kafka vs AWS-managed
  Kinesis, with measured latency and cost trade-offs
- **Structured Streaming into Delta** with exactly-once semantics via
  checkpointing and idempotent MERGE
- **Stateful stream-stream joins** matching GPS pings to open trips within
  a bounded time window
- **Geospatial SQL** using H3 hex zones for surge computation and distance
  windows for fraud detection
- **Infrastructure as code** with Terraform: S3, IAM, Kinesis, VPC,
  Databricks workspace bindings
- **Databricks Workflows + Unity Catalog** governing the lakehouse
- **Kubernetes** hosting the producer service (`kind` locally, EKS in cloud)
- **Observability** with Prometheus + Grafana: consumer lag, throughput,
  watermark delay

## Architecture (high level)

```text
                     ┌─────────────────────────────────────┐
                     │  producer (Kubernetes)              │
                     │  - synthetic GPS + trip events      │
                     │  - keyed by trip_id                 │
                     └──────────────────┬──────────────────┘
                                        │
                     ┌──────────────────┼──────────────────┐
                     ▼                                     ▼
             ┌───────────────┐                    ┌────────────────┐
             │  Kafka        │  (self-managed)    │  Kinesis       │  (AWS-managed)
             └───────┬───────┘                    └────────┬───────┘
                     │                                     │
                     ▼                                     ▼
        Structured Streaming                     Kinesis Firehose → S3
                     │                                     │
                     ▼                                     ▼
              bronze Delta ─────────►  silver Delta  ─────────►  gold marts
                                        - dedup                     - surge_zones_5min
                                        - watermark                 - active_trips_now
                                        - stream-stream join        - fraud_signals
                                        - SCD2 (drivers)
```

Detailed component topology, ADRs, and design principles land in
`docs/architecture.md` in a later phase.

## Roadmap

| Phase | Focus | Status | Notes |
|-------|-------|--------|-------|
| 0 | Repo scaffolding, tooling, CI | ✅ done | [docs/phase-00-scaffolding.md](docs/phase-00-scaffolding.md) |
| 1 | Ride event generator (real NYC TLC data) | ✅ done | [docs/phase-01-event-generator.md](docs/phase-01-event-generator.md) |
| 2 | Kafka producer (Redpanda local) | ✅ done | [docs/phase-02-kafka-producer.md](docs/phase-02-kafka-producer.md) |
| 3 | Bronze streaming — Kafka → Delta | ✅ done | [docs/phase-03-bronze-streaming.md](docs/phase-03-bronze-streaming.md) |
| 4a | Silver streaming — dedup, quality filter, trip fact fold | ✅ done | [docs/phase-04-silver-streaming.md](docs/phase-04-silver-streaming.md) |
| 4b | Silver streaming — stream-stream join (GPS ⋈ trips) | ✅ done | [docs/phase-04b-silver-stream-join.md](docs/phase-04b-silver-stream-join.md) |
| 5a | Gold marts — borough attribution + active trips + demand | ✅ done | [docs/phase-05-gold-marts.md](docs/phase-05-gold-marts.md) |
| 5b | Gold marts — fraud signals (teleport, dual-trip driver) | ⏳ planned | |
| 6 | Geospatial layer (H3 hex zones, distance SQL) | ⏳ planned | |
| 7 | Kinesis alternative path (Firehose → S3 → Delta) | ⏳ planned | |
| 8 | Terraform for AWS infra | ⏳ planned | |
| 9 | Kubernetes producer deployment (`kind` + Helm) | ⏳ planned | |
| 10 | Databricks Workflows + Unity Catalog | ⏳ planned | |
| 11 | Observability — Prometheus + Grafana | ⏳ planned | |
| 12 | CI/CD — GitHub Actions + Jenkinsfile + docs polish | ⏳ planned | |

## Tech stack (target)

- **Language:** Python 3.11
- **Compute:** Apache Spark 3.5 (PySpark), Delta Lake 3.x
- **Streaming (self-managed):** Apache Kafka (Redpanda locally, Confluent in cloud)
- **Streaming (managed):** AWS Kinesis Data Streams + Firehose
- **Warehouse / lakehouse:** Databricks + Delta + Unity Catalog
- **Cloud infra:** AWS (S3, IAM, VPC, Kinesis), all Terraform-managed
- **Containers:** Docker
- **Orchestration (streaming):** Databricks Workflows
- **Orchestration (batch backfill):** Apache Airflow
- **Runtime (producer):** Kubernetes (`kind` locally, Helm charts)
- **Observability:** Prometheus, Grafana
- **CI/CD:** GitHub Actions, Jenkins (portfolio artifact)
- **Dev tooling:** ruff, mypy, pytest, pre-commit

## Getting started

Requires Python 3.11+. Streaming phases will additionally require a JDK
(11 or 17) for Spark and Docker for the local Kafka broker — both added
as prerequisites in their respective phase docs.

```bash
brew install openjdk@17          # macOS; Ubuntu: apt install openjdk-17-jdk
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,generator,producer,spark]"
pre-commit install
pytest                            # fast, hermetic (Spark + integration tests skipped)
```

Peek at real ride events streaming through the generator:

```bash
python -m rides_telemetry.generator --month 2024-01 --max-trips 3 --seed 42 -v
```

This downloads ~50 MB of real Uber/Lyft trips from the NYC TLC public
feed on first run (cached under `data/nyc_tlc/`) and emits lifecycle +
GPS events as NDJSON on stdout. See
[`docs/phase-01-event-generator.md`](docs/phase-01-event-generator.md).

Stand up local Kafka (Redpanda) and stream real trips onto it:

```bash
docker compose up -d                             # broker + browsable console
python -m rides_telemetry.producer \
    --month 2024-01 --max-trips 100 --seed 42 \
    --use-system-trust-store -v
open http://localhost:8080                       # Redpanda Console
```

Land it in the bronze Delta layer via Spark Structured Streaming:

```bash
python -m rides_telemetry.bronze --topic gps   --once -v
python -m rides_telemetry.bronze --topic trips --once -v
# → lakehouse/warehouse/bronze_rides_{gps,lifecycle}/  (Delta tables)
```

Promote bronze to silver (dedup + quality filter + trip-fact fold),
then join to enrich each ping with its trip context:

```bash
python -m rides_telemetry.silver --stream gps     --once -v
python -m rides_telemetry.silver --stream trips   --once -v
python -m rides_telemetry.silver --stream matched --once -v
# → lakehouse/warehouse/silver_rides_gps/       (per-ping, deduped)
# → lakehouse/warehouse/silver_trip_facts/      (one row per trip)
# → lakehouse/warehouse/silver_gps_matched/     (pings ⋈ trip context)
```

Aggregate matched pings into per-borough gold marts:

```bash
python -m rides_telemetry.gold --mart active_trips --once -v
python -m rides_telemetry.gold --mart demand_5min  --once -v
# → lakehouse/warehouse/gold_active_trips_now/       (1-min per-borough snapshots)
# → lakehouse/warehouse/gold_demand_by_borough_5min/ (5-min tumbling windows)
```

Details in
[`docs/phase-02-kafka-producer.md`](docs/phase-02-kafka-producer.md),
[`docs/phase-03-bronze-streaming.md`](docs/phase-03-bronze-streaming.md),
[`docs/phase-04-silver-streaming.md`](docs/phase-04-silver-streaming.md),
[`docs/phase-04b-silver-stream-join.md`](docs/phase-04b-silver-stream-join.md), and
[`docs/phase-05-gold-marts.md`](docs/phase-05-gold-marts.md).

## Two operating modes

The project is designed to run in either of two cost profiles:

- **Free-tier local**: Redpanda + LocalStack + Databricks Community
  Edition + `kind`. Zero recurring cost. Slightly reduced feature surface
  (no Unity Catalog, no Kinesis, no real EKS).
- **Cloud paid (~$50-100/mo)**: real AWS + Databricks trial + real EKS.
  Full feature surface, capped by billing alerts.

Both paths are exercised in the phase docs so a reader can follow along
without a credit card if they prefer.

## Development

- **Format & lint:** `ruff check --fix .` and `ruff format .` (both run on
  every commit via pre-commit)
- **Type check:** `mypy src/`
- **Test:** `pytest`

## License

MIT — see [LICENSE](LICENSE).
