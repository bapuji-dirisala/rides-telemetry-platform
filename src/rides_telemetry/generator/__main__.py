"""``python -m rides_telemetry.generator`` — stream events as NDJSON.

Every line is one JSON-encoded event. Designed to be piped:

.. code-block:: bash

    # Peek at a few real trips as events
    python -m rides_telemetry.generator --month 2024-01 --max-trips 3 --seed 42

    # Feed a local Kafka topic (phase 2 use case)
    python -m rides_telemetry.generator --month 2024-01 | kcat -b localhost:9092 -t rides -P

    # Save a fixture for downstream tests
    python -m rides_telemetry.generator --max-trips 100 > fixtures/sample.ndjson
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from rides_telemetry.generator.simulator import RideSimulator, SimulatorConfig
from rides_telemetry.sources.nyc_tlc import NycTlcTripSource


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m rides_telemetry.generator",
        description=(
            "Stream real NYC TLC ride-hailing trips as ride-platform "
            "telemetry events (NDJSON on stdout)."
        ),
    )
    p.add_argument(
        "--month",
        default="2024-01",
        help=(
            "TLC month to load in YYYY-MM format. Defaults to 2024-01 "
            "which is available and stable."
        ),
    )
    p.add_argument(
        "--max-trips",
        type=int,
        default=100,
        help="Cap on number of trips to emit. Use 0 for unlimited.",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Seed for all RNGs. Fixed seed + fixed month → identical output.",
    )
    p.add_argument(
        "--ping-interval",
        type=float,
        default=1.0,
        help="Seconds between GPS pings for an active trip.",
    )
    p.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("data/nyc_tlc"),
        help="Where to cache the downloaded TLC parquet + zone lookup CSV.",
    )
    p.add_argument(
        "--use-system-trust-store",
        action="store_true",
        help=(
            "Use the OS trust store (macOS keychain, Windows cert store, "
            "Linux system CAs) instead of certifi's bundle. Enable this "
            "when downloads fail with 'CERTIFICATE_VERIFY_FAILED' behind "
            "a corporate TLS-inspecting proxy (Zscaler, Netskope, etc.)."
        ),
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Log download progress and cache hits to stderr.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    if args.use_system_trust_store:
        # Route every SSL context (httpx, urllib, etc.) through the OS
        # trust store so corporate root CAs are picked up without us
        # having to weaken verification.
        import truststore

        truststore.inject_into_ssl()

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
    try:
        for event in simulator.iter_events(max_trips=limit):
            sys.stdout.write(event.model_dump_json())
            sys.stdout.write("\n")
    except BrokenPipeError:
        # Consumer (e.g. ``head``) closed its end. That's normal.
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
