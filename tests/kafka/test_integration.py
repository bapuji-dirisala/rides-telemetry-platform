"""Real-broker integration test.

Skipped unless ``RUN_KAFKA_INTEGRATION_TESTS=1`` is set in the env,
so day-to-day ``pytest`` runs stay fast and Docker-free. To run:

.. code-block:: bash

    docker compose up -d
    RUN_KAFKA_INTEGRATION_TESTS=1 pytest tests/kafka/test_integration.py -v
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest

from rides_telemetry.events import GpsPing
from rides_telemetry.kafka import EventProducer, KafkaConfig, ensure_topics

_SKIP_REASON = "set RUN_KAFKA_INTEGRATION_TESTS=1 (with docker compose up) to enable"
_ENABLED = os.environ.get("RUN_KAFKA_INTEGRATION_TESTS") == "1"

pytestmark = pytest.mark.skipif(not _ENABLED, reason=_SKIP_REASON)


def _unique_topic_suffix() -> str:
    return f"integ-{uuid.uuid4().hex[:8]}"


def test_produces_to_real_broker() -> None:
    # Use unique topic names so parallel test runs don't collide.
    suffix = _unique_topic_suffix()
    cfg = KafkaConfig(
        topic_trips=f"rides.trips.{suffix}",
        topic_gps=f"rides.gps.{suffix}",
    )

    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": cfg.bootstrap_servers})
    ensure_topics(admin, cfg)

    producer = EventProducer(cfg)
    ping = GpsPing._make(  # noqa: SLF001
        event_ts=datetime.now(tz=UTC),
        trip_id=uuid.uuid4(),
        driver_id=uuid.uuid4(),
        lat=40.75,
        lng=-73.98,
        speed_kmh=25.0,
        heading_deg=90.0,
        accuracy_m=5.0,
    )
    producer.produce(ping)
    remaining = producer.flush(timeout=10.0)

    assert remaining == 0
    assert producer.stats.produced == 1
    assert producer.stats.delivered == 1
    assert producer.stats.failed == 0
