# Phase 2 — Kafka producer (Redpanda local)

## Goal

Stand up local Kafka, publish the phase-1 event stream to it with
production-shaped settings, and end up with a broker + browsable
console that every future phase (bronze streaming, silver watermarks,
gold marts) can attach to.

## What this phase delivers

- **`compose.yaml`** at repo root — [Redpanda](https://redpanda.com/) broker + [Redpanda Console](https://docs.redpanda.com/current/console/) UI, ready to `docker compose up -d`.
- **`src/rides_telemetry/kafka/`** package:
  - `KafkaConfig` — connection, topic names, partition counts, all
    producer tuning in one dataclass
  - `EventProducer` — wraps `confluent_kafka.Producer` with correct-by-
    default settings (idempotent, `acks=all`, snappy, sensible batching)
    and event-aware routing / keying
  - `topic_for(event)` — pure routing function, unit-testable in isolation
  - `ensure_topics(admin, config)` — create-if-missing on startup
- **CLI**: `python -m rides_telemetry.producer` — wires the phase-1
  simulator directly into the producer. Real NYC TLC trips → real Kafka.
- **Tests** — hermetic unit tests using an in-memory `FakeProducerBackend`
  that duck-types the `confluent_kafka.Producer` surface, plus a real-
  broker integration test gated by `RUN_KAFKA_INTEGRATION_TESTS=1`.

## Topology

```text
┌─────────────────────────┐
│  RideSimulator (ph. 1)  │
│  real NYC TLC trips     │
└───────────┬─────────────┘
            │ Event stream (in-process iterator)
            ▼
┌─────────────────────────┐
│  EventProducer          │  key = trip_id (UTF-8 bytes)
│  - idempotent           │  value = model_dump_json (UTF-8 bytes)
│  - acks=all             │
│  - snappy compression   │
│  - batches 128 KB       │
└─────┬─────────────┬─────┘
      │             │
      ▼             ▼
 rides.trips.v1   rides.gps.v1
 3 partitions     6 partitions
 (lifecycle)      (GPS pings)
```

## Topic design

| Topic | Volume | Partitions | Keyed by | Consumers |
|-------|--------|------------|----------|-----------|
| `rides.trips.v1` | ~5 msg/trip | 3 | `trip_id` | Fraud, driver-rider matching, trip fact table |
| `rides.gps.v1` | ~1 msg/sec/active-trip | 6 | `trip_id` | Surge, active-trips, geospatial marts |

Rationale:

- **Separate topics** so each pipeline can independently choose
  retention, replication, and consumer parallelism. Bronze GPS Delta
  tables want tight retention on Kafka (Delta is the durable copy);
  fraud logic wants long Kafka retention for replay.
- **Key by `trip_id`** so all events for a single trip land on the same
  partition and consumers see them in produce order. Cross-trip
  ordering is not preserved — and doesn't need to be.
- **`.v1` suffix** for explicit schema versioning. Anything more than
  an additive pydantic change rolls a `.v2` topic instead of breaking
  consumers.

## Design decisions

- **Idempotent producer + `acks=all`.** Idempotence prevents duplicate
  writes on producer retries; `acks=all` waits for every in-sync
  replica. Together they give at-least-once semantics with no dupes on
  the wire. Exactly-once end-to-end is a phase 3+ concern via Delta's
  idempotent `MERGE`.

- **Snappy compression.** GPS ping payloads are highly repetitive
  (same trip_id, similar lat/lng, tiny numeric variance) so snappy
  cuts wire volume by 4-6x with negligible CPU cost. LZ4 is also
  fine; gzip is too slow for the throughput we're targeting.

- **Duck-typed backend.** `EventProducer.__init__` accepts any object
  implementing a small `_ProducerBackend` `Protocol`. In tests we
  substitute `FakeProducerBackend`; in production it's a real
  `confluent_kafka.Producer`. Keeps CI Docker-free and unit tests fast.

- **`compose.yaml` at repo root, not `infra/`.** Docker Compose looks
  for `compose.yaml`/`docker-compose.yaml` at the project root by
  convention; putting it there means `docker compose up` just works
  from a fresh clone without extra flags.

- **Redpanda over Apache Kafka.** Wire-compatible, single binary, no
  ZooKeeper/KRaft coordination overhead, fits comfortably on a laptop.
  Downstream code sees a Kafka wire protocol and cannot tell the
  difference.

- **`confluent-kafka` over `kafka-python`.** Industry-standard
  librdkafka underneath, higher throughput, matches what you'd run in
  production. The `manylinux` wheel bundles librdkafka so `pip install
  confluent-kafka` works out-of-the-box on Linux CI.

## How to run

```bash
# 1. Local Redpanda + console
docker compose up -d
open http://localhost:8080   # Redpanda Console

# 2. Install deps once
pip install -e ".[dev,generator,producer]"

# 3. Produce 100 real trips (~3-4k messages) to Kafka
python -m rides_telemetry.producer \
    --month 2024-01 --max-trips 100 --seed 42 \
    --use-system-trust-store -v

# 4. Inspect messages
kcat -b localhost:19092 -t rides.gps.v1 -C -q -o beginning -c 3
# ... or browse in Redpanda Console at http://localhost:8080

# 5. Tear down
docker compose down -v
```

## Testing

```bash
# Fast, hermetic — no Docker
pytest -q

# Full integration test against a real local Redpanda
docker compose up -d
RUN_KAFKA_INTEGRATION_TESTS=1 pytest tests/kafka/test_integration.py -v
```

## Gotchas

- **Bootstrap host from the laptop is `localhost:19092`** (the
  external listener). From inside the compose network it's
  `redpanda:9092`. Getting these mixed up produces confusing
  connection-refused errors — the compose file comments explain
  which is which.
- **First `produce()` before `ensure_topics()`** would work but the
  broker auto-creates the topic with the default (1) partition
  count, defeating our per-topic parallelism choices. The CLI
  always runs `ensure_topics()` first unless `--no-ensure-topics`
  is set.
- **Corporate TLS-inspecting proxies (Zscaler etc.)** still bite when
  the TLC downloader tries to fetch the parquet on first run. Pass
  `--use-system-trust-store` (added in phase 1) to route SSL through
  the OS keychain.
- **`docker compose down -v`** wipes the `redpanda-data` volume —
  topics and messages go with it. Use `docker compose down`
  (no `-v`) to preserve state across restarts.

## References

- Redpanda docker quickstart: <https://docs.redpanda.com/current/get-started/quick-start/>
- confluent-kafka Python client: <https://docs.confluent.io/kafka-clients/python/current/overview.html>
- Idempotent producer semantics: <https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/>
- Prior phase: [Phase 1 — Ride event generator](phase-01-event-generator.md)
