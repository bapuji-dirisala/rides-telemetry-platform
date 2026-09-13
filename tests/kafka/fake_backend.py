"""A tiny in-memory stand-in for ``confluent_kafka.Producer``.

Just enough surface to satisfy :class:`~rides_telemetry.kafka.producer._ProducerBackend`
so we can unit-test the producer without spinning up a broker.

Every ``produce`` call records the message and immediately invokes the
delivery callback with success — exactly the behaviour a well-behaved
broker + client library would exhibit in the happy path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class RecordedMessage:
    topic: str
    key: bytes | None
    value: bytes | None


class FakeProducerBackend:
    """In-memory recorder of every produce call, with sync delivery callbacks."""

    def __init__(self, *, fail_every: int | None = None) -> None:
        """
        Parameters
        ----------
        fail_every:
            If set, every N-th message fails its delivery callback. Lets
            us exercise the error-counting path without a real broker.
        """
        self.messages: list[RecordedMessage] = []
        self._fail_every = fail_every
        self._n = 0
        self._pending_callbacks: list[tuple[Any, Any]] = []

    # ---- backend protocol ----------------------------------------------

    def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        partition: int = -1,  # noqa: ARG002
        on_delivery: Any = None,
        headers: Any = None,  # noqa: ARG002
        timestamp: int = 0,  # noqa: ARG002
    ) -> None:
        self._n += 1
        # Normalize to bytes so tests can assert on payloads deterministically.
        val_b = value.encode() if isinstance(value, str) else value
        key_b = key.encode() if isinstance(key, str) else key
        self.messages.append(RecordedMessage(topic=topic, key=key_b, value=val_b))
        # Queue the callback so it fires on the next poll/flush — matches
        # confluent-kafka's async delivery semantics rather than firing
        # synchronously (which would hide callback-ordering bugs).
        if on_delivery is not None:
            err = None
            if self._fail_every and self._n % self._fail_every == 0:
                err = _FakeKafkaError(f"synthetic failure #{self._n}")
            self._pending_callbacks.append((on_delivery, err))

    def poll(self, timeout: float = 0.0) -> int:  # noqa: ARG002
        fired = len(self._pending_callbacks)
        for cb, err in self._pending_callbacks:
            cb(err, None)
        self._pending_callbacks.clear()
        return fired

    def flush(self, timeout: float = 0.0) -> int:  # noqa: ARG002
        self.poll(0.0)
        return 0


class _FakeKafkaError:
    """Duck-typed KafkaError — just needs to str() to something useful."""

    def __init__(self, msg: str) -> None:
        self._msg = msg

    def __str__(self) -> str:
        return self._msg
