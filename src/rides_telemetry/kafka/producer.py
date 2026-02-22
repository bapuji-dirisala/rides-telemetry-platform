"""High-level Kafka producer for rides events.

Wraps :class:`confluent_kafka.Producer` with three important behaviours:

1. **Correct-by-default configuration.** Idempotent producer, ``acks=all``,
   snappy compression, sensible batching. Users can override via
   :class:`~rides_telemetry.kafka.config.KafkaConfig` but the defaults
   are already production-shaped.

2. **Event-aware routing and keying.** The caller hands us a pydantic
   :class:`~rides_telemetry.events.Event` and we work out the topic
   (via :func:`topic_for`), the key (``trip_id`` as UTF-8 bytes for
   stable partition affinity), the value (``model_dump_json`` bytes),
   and the delivery callback. No boilerplate at the call site.

3. **Duck-typed producer backend.** ``EventProducer`` accepts any
   object exposing ``produce`` / ``poll`` / ``flush`` — real
   confluent-kafka in production, an in-memory fake in unit tests.
   That's what keeps CI Docker-free.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, cast

from rides_telemetry.events import EventBase
from rides_telemetry.kafka.config import KafkaConfig
from rides_telemetry.kafka.topics import topic_for

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka import Message

_log = logging.getLogger(__name__)


class _ProducerBackend(Protocol):
    """Minimal slice of the ``confluent_kafka.Producer`` interface.

    Kept as a Protocol (not a Kafka class) so tests can supply a fake
    without pulling in librdkafka. Signatures mirror confluent-kafka's
    own — value/key are ``str | bytes | None`` because that's what the
    real client accepts, and Protocol matching is invariant.
    """

    def produce(  # noqa: PLR0913 — mirrors confluent-kafka's own signature
        self,
        topic: str,
        value: str | bytes | None = ...,
        key: str | bytes | None = ...,
        partition: int = ...,
        on_delivery: Any = ...,
        headers: Any = ...,
        timestamp: int = ...,
    ) -> None: ...

    def poll(self, timeout: float = ...) -> int: ...

    def flush(self, timeout: float = ...) -> int: ...


@dataclass
class ProducerStats:
    """Rolling counters exposed by :attr:`EventProducer.stats`."""

    produced: int = 0
    delivered: int = 0
    failed: int = 0
    bytes_produced: int = 0
    started_at: float = field(default_factory=time.monotonic)

    def throughput_per_s(self) -> float:
        elapsed = max(1e-9, time.monotonic() - self.started_at)
        return self.produced / elapsed


class EventProducer:
    """Produce rides events to Kafka with sensible defaults."""

    def __init__(
        self,
        config: KafkaConfig | None = None,
        *,
        backend: _ProducerBackend | None = None,
    ) -> None:
        """
        Parameters
        ----------
        config:
            Connection + topic + tuning knobs. Defaults to local Redpanda.
        backend:
            Injectable producer implementation. Leave as ``None`` in real
            use (creates a ``confluent_kafka.Producer``) and pass a fake
            in tests.
        """
        self.config = config or KafkaConfig()
        self.stats = ProducerStats()
        self._backend: _ProducerBackend = backend or self._make_default_backend()

    # ---- public --------------------------------------------------------

    def produce(self, event: EventBase) -> None:
        """Enqueue one event for delivery.

        Returns as soon as the message is in librdkafka's local queue —
        actual delivery is asynchronous. Use :meth:`flush` before exit
        to make sure everything drained.
        """
        # ``model_dump_json`` respects the ``use_enum_values=True`` config
        # from EventBase so enums serialize as their string values.
        payload = event.model_dump_json().encode("utf-8")
        # trip_id → partition key. Every event carries trip_id (lifecycle
        # + GPS), so per-trip ordering on Kafka is preserved because all
        # messages for one trip hash to the same partition.
        key = str(event.trip_id).encode("utf-8")  # type: ignore[attr-defined]
        topic = topic_for(event, self.config)

        try:
            self._backend.produce(
                topic=topic,
                key=key,
                value=payload,
                on_delivery=self._on_delivery,
            )
        except BufferError:
            # librdkafka's local queue is full. Serve any callbacks (which
            # frees space) and retry once. If it still fails, propagate —
            # the caller is producing faster than the broker can absorb.
            _log.warning("producer queue full, polling then retrying")
            self._backend.poll(1.0)
            self._backend.produce(
                topic=topic,
                key=key,
                value=payload,
                on_delivery=self._on_delivery,
            )

        self.stats.produced += 1
        self.stats.bytes_produced += len(payload)
        # Serve delivery callbacks between produces so ``stats`` stays
        # up-to-date and errors surface promptly.
        self._backend.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Block until all queued messages are delivered.

        Returns the number of messages still in the queue after
        ``timeout`` (0 on clean drain).
        """
        remaining = self._backend.flush(timeout)
        if remaining:
            _log.warning("flush timed out with %d messages still queued", remaining)
        return remaining

    # ---- internals -----------------------------------------------------

    def _make_default_backend(self) -> _ProducerBackend:
        # Local import: keeps the module importable in environments
        # (e.g. CI without librdkafka) that only ever use the fake.
        from confluent_kafka import Producer

        # ``Producer`` implements a superset of ``_ProducerBackend``
        # (extra optional kwargs on ``produce``), which mypy can't
        # validate through structural typing — hence the explicit cast.
        return cast("_ProducerBackend", Producer(self.config.to_confluent_config()))

    def _on_delivery(self, err: Any, msg: Message | None) -> None:
        if err is not None:
            self.stats.failed += 1
            _log.error("delivery failed: %s", err)
            return
        self.stats.delivered += 1
