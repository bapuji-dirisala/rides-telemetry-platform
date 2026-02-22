"""Kafka producer + admin configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class KafkaConfig:
    """All Kafka wiring knobs for the rides platform in one place.

    Defaults target the local Redpanda instance defined in
    :file:`compose.yaml` (broker on ``localhost:19092``, single node,
    no auth). Override each field for staging / production later.
    """

    bootstrap_servers: str = "localhost:19092"
    """Comma-separated ``host:port`` list."""

    client_id: str = "rides-telemetry-producer"
    """Shows up in broker logs and metrics — useful for debugging."""

    topic_trips: str = "rides.trips.v1"
    """Topic for low-volume trip lifecycle events."""

    topic_gps: str = "rides.gps.v1"
    """Topic for high-volume GPS pings."""

    trips_partitions: int = 3
    """Trip lifecycle throughput is modest; three partitions is plenty for local."""

    gps_partitions: int = 6
    """GPS is the hot topic — twice as many partitions to spread load."""

    replication_factor: int = 1
    """Single-broker Redpanda for local dev. Bump to 3 in prod."""

    # ---- producer tuning ------------------------------------------------

    linger_ms: int = 20
    """Small linger to batch pings from the same trip without adding user-visible latency."""

    batch_size: int = 128 * 1024
    """128 KB batches — good throughput without wasting memory."""

    compression_type: str = "snappy"
    """Snappy is a good CPU / ratio trade-off. LZ4 also fine; gzip too slow."""

    enable_idempotence: bool = True
    """Idempotent producer gives exactly-once *at the producer* — no dupes on retry."""

    acks: str = "all"
    """Wait for all in-sync replicas. Safe default; irrelevant with RF=1 but sets the habit."""

    request_timeout_ms: int = 30_000
    """How long to wait for a broker ack before retrying."""

    # ---- convenience ----------------------------------------------------

    extra: dict[str, Any] = field(default_factory=dict)
    """Escape hatch for any librdkafka setting not exposed as a field."""

    def to_confluent_config(self) -> dict[str, Any]:
        """Render as the kwargs dict ``confluent_kafka.Producer`` expects.

        Kept as a plain dict (not a Producer instance) so tests can
        assert on the config without importing confluent-kafka.
        """
        cfg: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "linger.ms": self.linger_ms,
            "batch.size": self.batch_size,
            "compression.type": self.compression_type,
            "enable.idempotence": self.enable_idempotence,
            "acks": self.acks,
            "request.timeout.ms": self.request_timeout_ms,
        }
        cfg.update(self.extra)
        return cfg
