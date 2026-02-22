"""EventProducer tests using an in-memory fake backend.

No Docker, no confluent-kafka on the runtime path — just the pydantic
event models + the producer wrapper + our fake.
"""

from __future__ import annotations

import json

from rides_telemetry.events import EventType
from rides_telemetry.generator import RideSimulator, SimulatorConfig
from rides_telemetry.kafka import EventProducer, KafkaConfig
from rides_telemetry.sources.in_memory import InMemoryTripSource
from tests.kafka.fake_backend import FakeProducerBackend


def _run(source: InMemoryTripSource) -> tuple[EventProducer, FakeProducerBackend]:
    fake = FakeProducerBackend()
    producer = EventProducer(KafkaConfig(), backend=fake)
    sim = RideSimulator(source, SimulatorConfig(seed=7, ping_interval_s=60.0))
    for event in sim.iter_events():
        producer.produce(event)
    producer.flush()
    return producer, fake


class TestRouting:
    def test_gps_and_lifecycle_go_to_different_topics(
        self, in_memory_source: InMemoryTripSource
    ) -> None:
        _, fake = _run(in_memory_source)
        topics = {m.topic for m in fake.messages}
        assert topics == {"rides.trips.v1", "rides.gps.v1"}

    def test_every_message_is_keyed_by_trip_id(self, in_memory_source: InMemoryTripSource) -> None:
        _, fake = _run(in_memory_source)
        for msg in fake.messages:
            assert msg.key is not None
            # Keys are UTF-8 UUID strings — 36 bytes.
            assert len(msg.key) == 36

    def test_key_matches_payload_trip_id(self, in_memory_source: InMemoryTripSource) -> None:
        _, fake = _run(in_memory_source)
        for msg in fake.messages:
            payload = json.loads(msg.value)  # type: ignore[arg-type]
            assert msg.key is not None
            assert msg.key.decode() == payload["trip_id"]


class TestSerialization:
    def test_payload_is_valid_ndjson_line(self, in_memory_source: InMemoryTripSource) -> None:
        _, fake = _run(in_memory_source)
        for msg in fake.messages:
            assert msg.value is not None
            assert b"\n" not in msg.value  # single line — NDJSON-safe
            parsed = json.loads(msg.value)
            assert "event_type" in parsed
            assert parsed["event_type"] in {t.value for t in EventType}


class TestStats:
    def test_counters_track_produce_and_delivery(
        self, in_memory_source: InMemoryTripSource
    ) -> None:
        producer, fake = _run(in_memory_source)
        assert producer.stats.produced == len(fake.messages)
        # FakeProducerBackend delivers everything successfully by default.
        assert producer.stats.delivered == producer.stats.produced
        assert producer.stats.failed == 0
        assert producer.stats.bytes_produced == sum(len(m.value) for m in fake.messages if m.value)

    def test_delivery_failures_are_counted(self, in_memory_source: InMemoryTripSource) -> None:
        fake = FakeProducerBackend(fail_every=3)
        producer = EventProducer(KafkaConfig(), backend=fake)
        sim = RideSimulator(in_memory_source, SimulatorConfig(seed=7, ping_interval_s=60.0))
        for event in sim.iter_events():
            producer.produce(event)
        producer.flush()

        assert producer.stats.failed > 0
        assert producer.stats.delivered + producer.stats.failed == producer.stats.produced
