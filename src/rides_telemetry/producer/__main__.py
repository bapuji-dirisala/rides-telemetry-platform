"""``python -m rides_telemetry.producer`` — stream real trips into Kafka.

Wires the phase-1 :class:`~rides_telemetry.generator.RideSimulator`
(which turns real NYC TLC trips into events) into the phase-2
:class:`~rides_telemetry.kafka.EventProducer` (which delivers them to
Kafka with production-shaped settings).

Typical local session:

.. code-block:: bash

    # Start Redpanda + the browsable console
    docker compose up -d

    # Produce 100 real trips (~a few thousand events) to Kafka
    python -m rides_telemetry.producer \
        --month 2024-01 --max-trips 100 --seed 42 \
        --bootstrap-servers localhost:19092 \
        --use-system-trust-store

    # Browse messages at http://localhost:8080
    # Or tail them with kcat:
    kcat -b localhost:19092 -t rides.gps.v1 -C -q -o end
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from pathlib import Path
from types import FrameType

from rides_telemetry.generator import RideSimulator, SimulatorConfig
from rides_telemetry.kafka import EventProducer, KafkaConfig, ensure_topics
from rides_telemetry.sources.nyc_tlc import NycTlcTripSource

_log = logging.getLogger("rides_telemetry.producer")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m rides_telemetry.producer",
        description=(
            "Stream real NYC TLC ride events into local Kafka. "
            "Assumes a broker reachable at --bootstrap-servers "
            "(default matches the compose.yaml Redpanda listener)."
        ),
    )
    # -- source knobs (mirror generator CLI) --------------------------------
    p.add_argument("--month", default="2024-01", help="TLC month, YYYY-MM.")
    p.add_argument(
        "--max-trips",
        type=int,
        default=100,
        help="Cap on trips to produce. 0 = unlimited.",
    )
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--ping-interval", type=float, default=1.0)
    p.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("data/nyc_tlc"),
        help="TLC parquet cache dir.",
    )

    # -- kafka knobs --------------------------------------------------------
    p.add_argument(
        "--bootstrap-servers",
        default="localhost:19092",
        help="Kafka bootstrap list.",
    )
    p.add_argument("--topic-trips", default="rides.trips.v1")
    p.add_argument("--topic-gps", default="rides.gps.v1")
    p.add_argument(
        "--no-ensure-topics",
        action="store_true",
        help=(
            "Skip create-if-missing on startup. Useful when the broker "
            "user doesn't have admin rights (rare locally, common in prod)."
        ),
    )
    p.add_argument(
        "--progress-every",
        type=int,
        default=500,
        help="Log throughput every N events (0 = never).",
    )

    # -- environment knobs --------------------------------------------------
    p.add_argument(
        "--use-system-trust-store",
        action="store_true",
        help="Route SSL through the OS trust store (for corporate proxies).",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def _install_sigint_flush(producer: EventProducer) -> None:
    """Flush cleanly on Ctrl-C so no in-flight messages get dropped."""

    def _handler(signum: int, _frame: FrameType | None) -> None:  # noqa: ARG001
        _log.warning("SIGINT received — flushing producer")
        producer.flush(timeout=10.0)
        _log_stats(producer)
        sys.exit(0)

    signal.signal(signal.SIGINT, _handler)


def _log_stats(producer: EventProducer) -> None:
    s = producer.stats
    _log.info(
        "produced=%d delivered=%d failed=%d bytes=%d throughput=%.1f msg/s",
        s.produced,
        s.delivered,
        s.failed,
        s.bytes_produced,
        s.throughput_per_s(),
    )


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    if args.use_system_trust_store:
        import truststore

        truststore.inject_into_ssl()

    kafka_config = KafkaConfig(
        bootstrap_servers=args.bootstrap_servers,
        topic_trips=args.topic_trips,
        topic_gps=args.topic_gps,
    )

    # Create topics before opening the producer so first produce() lands
    # in a topic with the right partition count, not a broker-auto-created
    # one with the default 1 partition.
    if not args.no_ensure_topics:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": kafka_config.bootstrap_servers})
        ensure_topics(admin, kafka_config)

    producer = EventProducer(kafka_config)
    _install_sigint_flush(producer)

    source = NycTlcTripSource(
        month=args.month,
        cache_dir=args.cache_dir,
        seed=args.seed,
    )
    simulator = RideSimulator(
        source=source,
        config=SimulatorConfig(seed=args.seed, ping_interval_s=args.ping_interval),
    )

    limit = None if args.max_trips == 0 else args.max_trips
    last_log = time.monotonic()

    for event in simulator.iter_events(max_trips=limit):
        producer.produce(event)
        if (
            args.progress_every
            and producer.stats.produced % args.progress_every == 0
            and time.monotonic() - last_log > 1.0
        ):
            _log_stats(producer)
            last_log = time.monotonic()

    remaining = producer.flush(timeout=30.0)
    _log_stats(producer)
    return 1 if remaining else 0


if __name__ == "__main__":
    raise SystemExit(main())
