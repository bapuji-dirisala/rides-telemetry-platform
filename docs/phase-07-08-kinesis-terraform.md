# Phases 7 + 8 — Kinesis Firehose + Terraform AWS infra

## Phase 7 — Kinesis Data Firehose alternative path

The Kafka path (phases 2–6) stays. Phase 7 adds a parallel AWS-native
path that lands the same events in S3 via Kinesis Data Firehose,
Hive-partitioned so a Databricks Auto Loader (or `readStream.format("json")`)
tails it idempotently:

```
  producer  →  Firehose: rides-events  →  S3 bronze/rides/event_type=…/year=…/month=…/day=…/hour=…
                                                   │
                                                   ▼
                                         bronze Delta (same table name as the Kafka path)
```

Design decisions:

- **Direct put, no Kinesis Data Streams in front.** The portfolio
  producer isn't throughput-bounded; Firehose direct-put keeps the
  stack to one managed service.
- **Dynamic partitioning via jq `MetadataExtraction`.** One stream
  routes every event type into its own prefix without a separate
  stream per type.
- **`buffer_size_mb` + `buffer_interval_seconds` are first-class vars**
  on both sides (`FirehoseConfig` in Python + `aws_kinesis_firehose_delivery_stream`
  in Terraform) so you can't silently drift the Delta reader's
  partition-pruning predicate.
- **Local emulator** (`LocalFirehoseFake`) writes the same Hive layout
  on disk so the reader side can be exercised Docker-free.

Files:

- `src/rides_telemetry/kinesis/config.py` — `FirehoseConfig` +
  `hive_partition_prefix()`.
- `src/rides_telemetry/kinesis/producer.py` — `put_event_batch()`
  (batches at 450 records to stay under the 500-cap) + `LocalFirehoseFake`.
- `src/rides_telemetry/kinesis/reader.py` — Spark Structured Streaming
  reader targeting the same bronze Delta table name as the Kafka path.
- `terraform/modules/firehose/` — delivery stream + IAM role + CloudWatch
  log group.

## Phase 8 — Terraform for AWS infra

Four modules, all composable, all gated by variables so
`terraform validate` passes with empty defaults:

- `s3-lakehouse` — one bucket, versioning on, public access blocked,
  SSE-S3 by default (CMK via `kms_key_id`), lifecycle rule tiers
  non-current versions to IA at 30 days and preserves the Delta log.
- `firehose` — Phase 7 delivery stream + role.
- `iam` — producer role (scoped to `firehose:PutRecord*` on this
  stream only) + Databricks compute role (bronze read, silver + gold
  read/write).
- `vpc` — minimal two-AZ VPC with one NAT gateway. Gated by
  `enable_vpc` because LocalStack VPC is partial.

Dual-mode providers (`use_localstack = true` by default) route every
AWS SDK call to `localhost:4566`, so the HCL runs free out-of-the-box
and lifts to real AWS by flipping one variable.

## Running

```bash
cd terraform
terraform init
terraform plan  # defaults to LocalStack
```

For a real AWS target:

```bash
TF_VAR_use_localstack=false TF_VAR_environment=dev \
    terraform plan
```
