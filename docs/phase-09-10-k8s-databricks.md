# Phases 9 + 10 — Kubernetes producer + Databricks Workflows / Unity Catalog

## Phase 9 — Kubernetes producer

The producer ships as a multi-stage container (`k8s/producer/Dockerfile`)
and a kustomize bundle. Hardening is in the manifest, not the image:

- Non-root UID 10001, read-only root FS, `drop: [ALL]` caps, `restricted`
  Pod Security Standard enforced at namespace level.
- Config lives in a `ConfigMap`, so the same Deployment flips between
  local Redpanda and MSK without a rebuild.
- HPA keyed on `rides_bronze_consumer_lag_messages` scales 1–5 replicas
  based on how far bronze is lagging — Prometheus-adapter populates
  the custom metric from the Phase 11 scraper.
- `ServiceAccount` is annotated for IRSA so prod deploys inherit the
  Phase 8 producer IAM role.

Apply locally against `kind`:

```bash
kind create cluster --name rides
kubectl apply -k k8s/producer/
```

## Phase 10 — Databricks Workflows + Unity Catalog

- `databricks/workflows/rides_streaming.yml` is a Databricks Asset
  Bundle with `dev` and `prod` targets, per-env catalog + lakehouse
  root variables, and a four-task DAG (`bronze_gps`, `bronze_trips`,
  `silver_matched`, `gold_h3`) running on a Photon streaming cluster.
- `databricks/notebooks/*.py` are thin entrypoints that call the
  shared `rides_telemetry` package — same code runs on a laptop and
  in Databricks.
- `databricks/unity-catalog/01_catalogs_schemas.sql` bootstraps the
  three-level namespace with managed locations on the Phase 7 bucket.
- `02_grants.sql` splits ownership: platform engineers own
  bronze/silver/gold; analysts get `SELECT` on gold only, behind a
  row filter that hides in-progress + fraud-flagged trips.

Deploy:

```bash
databricks bundle deploy -t dev
```
