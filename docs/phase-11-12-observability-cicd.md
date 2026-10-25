# Phases 11 + 12 — Observability + CI/CD + docs polish

## Phase 11 — Observability stack

- `observability/prometheus/prometheus.yml` scrapes three jobs:
  - Kubernetes-SD-discovered `rides-producer` Pods (per-event
    counters + Kafka produce histograms).
  - Redpanda admin API (consumer lag, partition offsets).
  - Spark Structured Streaming driver (bronze/silver/gold progress
    via the JMX exporter sidecar).
- `observability/alerts/rides.yml` ships 4 SLO-shaped alerts:
  `ProducerThroughputDropped`, `BronzeConsumerLagHigh`,
  `BronzeStreamingNoProgress`, `EventLatencyP99Over30s`.
- `observability/grafana/rides-dashboard.json` has 5 panels: produced
  events/sec, bronze consumer lag, end-to-end latency
  percentiles, fraud alerts fired, active H3 trips.

The HPA from Phase 9 reads `rides_bronze_consumer_lag_messages` via
the Prometheus adapter — one metric, two consumers (alerting + scaling).

## Phase 12 — CI/CD polish

- `.github/workflows/terraform.yml` runs `terraform fmt -check -diff`
  + `validate` on every push to `terraform/**`. Separate from the
  Python CI so one slow terraform step doesn't block the hot-path
  pytest signal.
- `jenkins/Jenkinsfile` mirrors the GHA pipeline (install → lint →
  typecheck → test → terraform validate) so the project ships a
  Jenkins-portable path for shops that haven't adopted GHA.
- Docs updated — every phase has a dedicated doc under `docs/`, every
  roadmap row links to its doc, status badges reflect green CI.

## Final status

All 12 phases shipped. Local-only path (Redpanda + `kind` +
LocalStack) runs the full medallion with zero AWS cost. Real-AWS path
flips `TF_VAR_use_localstack=false` + applies the Phase 10 Databricks
bundle.
