# rides-telemetry-platform

[![ci](https://github.com/bapuji-dirisala/rides-telemetry-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/bapuji-dirisala/rides-telemetry-platform/actions/workflows/ci.yml)
[![python](https://img.shields.io/badge/python-3.11-blue)](pyproject.toml)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Real-time telemetry pipeline for a ride-hailing platform. Millions of GPS
pings per minute plus ride lifecycle events, streamed through Kafka (with a
parallel Kinesis path), landed into a Delta lakehouse with exactly-once
semantics, and served as geospatial gold marts for surge pricing, fraud
detection, and driver-rider matching.

> **Status:** Phase 0 — repo scaffolding shipped. Twelve-phase roadmap
> below; every phase gets a dedicated design doc under [`docs/`](docs/).

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
| 1 | Synthetic ride event generator | ⏳ planned | |
| 2 | Kafka producer (Redpanda local) | ⏳ planned | |
| 3 | Bronze streaming — Kafka → Delta | ⏳ planned | |
| 4 | Silver streaming — watermarks, dedup, stateful joins | ⏳ planned | |
| 5 | Gold real-time marts (surge, fraud, active trips) | ⏳ planned | |
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
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
pytest
```

Phase 0 is scaffolding only — no streaming code yet. Later phases will add
per-phase runbook sections and their own compose files.

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
