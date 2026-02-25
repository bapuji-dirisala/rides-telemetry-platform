"""``python -m rides_telemetry.silver`` — run a silver streaming job.

Two symmetric jobs, one per bronze table. Pick one with ``--stream``:

.. code-block:: bash

    # Continuous silver GPS (row-level dedup + quality filter)
    python -m rides_telemetry.silver --stream gps -v

    # Drain everything currently in bronze lifecycle then exit
    python -m rides_telemetry.silver --stream trips --once -v

    # Point at a non-default warehouse (e.g. an isolated test dir)
    python -m rides_telemetry.silver --stream gps \
        --warehouse-root /tmp/rides-lakehouse

Both jobs read from the bronze Delta tables written by phase 3. If
bronze is empty, silver will simply idle waiting for new data.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from rides_telemetry.silver.gps import stream_gps_to_silver
from rides_telemetry.silver.streaming import SilverStreamConfig
from rides_telemetry.silver.trips import stream_trips_to_silver
from rides_telemetry.spark import LAKEHOUSE_ROOT, SparkSessionConfig, build_spark_session

_log = logging.getLogger("rides_telemetry.silver")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m rides_telemetry.silver",
        description=(
            "Stream one bronze Delta table into its silver counterpart. "
            "Runs continuously by default; use --once for a bounded run."
        ),
    )
    p.add_argument(
        "--stream",
        required=True,
        choices=["gps", "trips"],
        help="Which silver job to run.",
    )
    p.add_argument(
        "--warehouse-root",
        type=Path,
        default=LAKEHOUSE_ROOT / "warehouse",
        help="Root dir holding both bronze and silver Delta tables.",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help=(
            "Drain everything currently in the bronze source then exit "
            "(uses Trigger.AvailableNow). Great for CI / smoke tests."
        ),
    )
    p.add_argument(
        "--processing-time",
        default="10 seconds",
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
        SparkSessionConfig(app_name=f"rides-silver-{args.stream}"),
    )

    stream_config = SilverStreamConfig(
        warehouse_root=args.warehouse_root,
        trigger_processing_time=None if args.once else args.processing_time,
    )

    runner = stream_gps_to_silver if args.stream == "gps" else stream_trips_to_silver
    query = runner(spark, stream_config, await_termination=True)

    _log.info(
        "silver stream '%s' finished — last progress=%s",
        query.name,
        query.lastProgress,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
