"""Topic naming + admin helpers.

Two topics for phase 2:

- ``rides.trips.v1`` — lifecycle events (request / accept / start / end /
  cancel). Low volume; ~5 messages per trip.
- ``rides.gps.v1`` — high-volume GPS pings. Dozens to hundreds per trip.

Keeping them separate lets us tune partitions, retention, and consumer
groups independently. The gold marts in phase 5 only care about GPS;
the fraud pipeline only cares about lifecycle. No point coupling them.

The ``.v1`` suffix is deliberate — schema evolution beyond additive
pydantic changes will roll a new topic (``rides.trips.v2``) rather than
silently break consumers.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from rides_telemetry.events import EventBase, GpsPing
from rides_telemetry.kafka.config import KafkaConfig

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka.admin import AdminClient

_log = logging.getLogger(__name__)

# Re-exported so callers don't have to know a config exists.
TOPIC_TRIPS = KafkaConfig().topic_trips
TOPIC_GPS = KafkaConfig().topic_gps


def topic_for(event: EventBase, config: KafkaConfig | None = None) -> str:
    """Return the topic name an event should be produced to.

    Pure function; no side effects. Kept separate from
    :class:`~rides_telemetry.kafka.producer.EventProducer` so it can be
    unit-tested and reused by consumers, tests, and CLIs.
    """
    cfg = config or KafkaConfig()
    return cfg.topic_gps if isinstance(event, GpsPing) else cfg.topic_trips


def ensure_topics(admin: AdminClient, config: KafkaConfig) -> None:
    """Create the two rides topics if they don't already exist.

    Safe to call at every producer startup — existing topics are a
    no-op. Uses :class:`confluent_kafka.admin.NewTopic` so we can
    encode partition / replication choices in code, versioned next
    to the producer that depends on them.
    """
    # Local import so importing this module never requires confluent-kafka.
    from confluent_kafka.admin import NewTopic

    existing = set(admin.list_topics(timeout=10).topics.keys())
    wanted = [
        NewTopic(
            config.topic_trips,
            num_partitions=config.trips_partitions,
            replication_factor=config.replication_factor,
        ),
        NewTopic(
            config.topic_gps,
            num_partitions=config.gps_partitions,
            replication_factor=config.replication_factor,
        ),
    ]
    to_create = [t for t in wanted if t.topic not in existing]
    if not to_create:
        _log.info("all rides topics already exist")
        return

    _log.info("creating topics: %s", [t.topic for t in to_create])
    futures = admin.create_topics(to_create, request_timeout=15)
    for topic, fut in futures.items():
        # ``.result()`` re-raises any KafkaException so a bad broker
        # config surfaces immediately at startup rather than silently
        # at first produce().
        fut.result(timeout=15)
        _log.info("created topic %s", topic)
