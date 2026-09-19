"""``python -m rides_telemetry.gold`` — run a gold mart streaming job.

Two marts in Phase 5a. Pick one with ``--mart``:

.. code-block:: bash

    # Continuous per-borough active-trips snapshot (1-min windows)
    python -m rides_telemetry.gold --mart active_trips -v

    # Drain everything currently in silver matched then exit
    python -m rides_telemetry.gold --mart demand_5min --once -v

    # Point at a non-default warehouse
    python -m rides_telemetry.gold --mart active_trips \
        --warehouse-root /tmp/rides-lakehouse

Both marts read from ``silver_gps_matched`` written by phase 4b.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from rides_telemetry.gold.active_trips import stream_active_trips_mart
from rides_telemetry.gold.demand import stream_demand_mart
from rides_telemetry.gold.streaming import GoldStreamConfig
from rides_telemetry.spark import LAKEHOUSE_ROOT, SparkSessionConfig, build_spark_session

_log = logging.getLogger("rides_telemetry.gold")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m rides_telemetry.gold",
        description=(
            "Aggregate silver_gps_matched into a business-ready gold mart. "
            "Runs continuously by default; use --once for a bounded run."
        ),
    )
    p.add_argument(
        "--mart",
        required=True,
        choices=["active_trips", "demand_5min"],
        help="Which gold mart to build.",
    )
    p.add_argument(
        "--warehouse-root",
        type=Path,
        default=LAKEHOUSE_ROOT / "warehouse",
        help="Root dir holding all Delta tables.",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help=(
            "Drain everything currently in the source then exit "
            "(uses Trigger.AvailableNow). Great for CI / smoke tests."
        ),
    )
    p.add_argument(
        "--processing-time",
        default="30 seconds",
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
        SparkSessionConfig(app_name=f"rides-gold-{args.mart}"),
    )

    stream_config = GoldStreamConfig(
        warehouse_root=args.warehouse_root,
        trigger_processing_time=None if args.once else args.processing_time,
    )

    runners = {
        "active_trips": stream_active_trips_mart,
        "demand_5min": stream_demand_mart,
    }
    query = runners[args.mart](spark, stream_config, await_termination=True)

    _log.info(
        "gold mart '%s' finished — last progress=%s",
        query.name,
        query.lastProgress,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
