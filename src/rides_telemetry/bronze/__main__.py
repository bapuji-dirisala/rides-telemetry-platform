"""``python -m rides_telemetry.bronze`` — run a bronze streaming job.

Two symmetric jobs, one per topic. Pick one with ``--topic``:

.. code-block:: bash

    # Continuous streaming into bronze_rides_gps
    python -m rides_telemetry.bronze --topic gps -v

    # Drain everything currently on the trips topic then exit
    python -m rides_telemetry.bronze --topic trips --once -v

    # Point at a non-default broker
    python -m rides_telemetry.bronze --topic gps \
        --bootstrap-servers redpanda.staging:9092
"""

from __future__ import annotations

import argparse
import logging
import sys

from rides_telemetry.bronze.streaming import (
    BronzeStreamConfig,
    stream_gps_to_bronze,
    stream_trips_to_bronze,
)
from rides_telemetry.spark import SparkSessionConfig, build_spark_session

_log = logging.getLogger("rides_telemetry.bronze")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m rides_telemetry.bronze",
        description=(
            "Stream one Kafka topic into its Delta bronze table. "
            "Runs continuously by default; use --once for a bounded run."
        ),
    )
    p.add_argument(
        "--topic",
        required=True,
        choices=["trips", "gps"],
        help="Which topic (and matching bronze table) to stream.",
    )
    p.add_argument(
        "--bootstrap-servers",
        default="localhost:19092",
        help="Kafka bootstrap list. Defaults to local Redpanda.",
    )
    p.add_argument(
        "--starting-offsets",
        default="earliest",
        help="Kafka startingOffsets. Ignored after the first run (checkpoint wins).",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help=(
            "Drain everything currently on the topic then exit "
            "(uses Trigger.AvailableNow). Great for CI / smoke tests."
        ),
    )
    p.add_argument(
        "--processing-time",
        default="5 seconds",
        help="Micro-batch cadence in continuous mode (ignored with --once).",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    spark = build_spark_session(
        SparkSessionConfig(app_name=f"rides-bronze-{args.topic}"),
    )

    stream_config = BronzeStreamConfig(
        bootstrap_servers=args.bootstrap_servers,
        starting_offsets=args.starting_offsets,
        trigger_processing_time=None if args.once else args.processing_time,
    )

    runner = stream_trips_to_bronze if args.topic == "trips" else stream_gps_to_bronze
    query = runner(spark, stream_config, await_termination=True)

    _log.info(
        "bronze stream '%s' finished — last progress=%s",
        query.name,
        query.lastProgress,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
