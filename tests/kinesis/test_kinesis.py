"""Phase 7 — Kinesis Firehose producer + local fake."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from rides_telemetry.events import GpsPing, TripRequested
from rides_telemetry.kinesis import (
    FirehoseConfig,
    LocalFirehoseFake,
    hive_partition_prefix,
    put_event_batch,
)


def _gps(trip_id=None) -> GpsPing:
    return GpsPing._make(
        event_ts=datetime(2026, 10, 1, 12, 34, 56, tzinfo=UTC),
        trip_id=trip_id or uuid4(),
        driver_id=uuid4(),
        lat=40.7128,
        lng=-74.0060,
        speed_kmh=20.0,
        heading_deg=90.0,
    )


def _trip() -> TripRequested:
    return TripRequested._make(
        event_ts=datetime(2026, 10, 1, 12, 34, 56, tzinfo=UTC),
        trip_id=uuid4(),
        rider_id=uuid4(),
        driver_id=None,
        pickup_lat=40.7128,
        pickup_lng=-74.0060,
        dropoff_lat=40.730610,
        dropoff_lng=-73.935242,
    )


def test_hive_partition_prefix_is_deterministic():
    dt = datetime(2026, 10, 1, 12, 34, 56, tzinfo=UTC)
    prefix = hive_partition_prefix("bronze/rides", dt, "gps_ping")
    assert prefix == "bronze/rides/event_type=gps_ping/year=2026/month=10/day=01/hour=12/"


def test_hive_partition_prefix_normalises_naive_to_utc():
    naive = datetime(2026, 10, 1, 12, 34, 56)
    assert "year=2026/month=10/day=01/hour=12" in hive_partition_prefix("x", naive, "e")


def test_put_event_batch_counts_accepted(tmp_path: Path):
    cfg = FirehoseConfig(delivery_stream_name="rides-local", s3_bucket="unused")
    fake = LocalFirehoseFake(root=tmp_path, prefix=cfg.s3_prefix)
    events = [_gps(), _trip(), _gps(uuid4())]
    accepted = put_event_batch(fake, cfg, events)
    assert accepted == len(events)


def test_local_fake_writes_hive_partitions(tmp_path: Path):
    cfg = FirehoseConfig(delivery_stream_name="rides-local", s3_bucket="unused")
    fake = LocalFirehoseFake(root=tmp_path, prefix=cfg.s3_prefix)
    put_event_batch(fake, cfg, [_gps(), _trip()])

    gps_files = list((tmp_path / "bronze" / "rides" / "event_type=gps_ping").rglob("*.jsonl"))
    trip_files = list(
        (tmp_path / "bronze" / "rides" / "event_type=trip_requested").rglob("*.jsonl")
    )
    assert gps_files, "gps_ping files not written"
    assert trip_files, "trip_requested files not written"

    body = json.loads(gps_files[0].read_text().strip())
    assert body["event_type"] == "gps_ping"
    assert "trip_id" in body


def test_put_event_batch_chunks_at_450(tmp_path: Path):
    """We cap each put_record_batch at 450 records."""

    calls: list[int] = []

    class _CountingBackend:
        def put_record_batch(self, DeliveryStreamName, Records):  # noqa: N803
            calls.append(len(Records))
            return {"FailedPutCount": 0, "RequestResponses": []}

    cfg = FirehoseConfig(delivery_stream_name="rides-local", s3_bucket="unused")
    events = [_gps() for _ in range(1_000)]
    accepted = put_event_batch(_CountingBackend(), cfg, events)
    assert accepted == 1_000
    assert all(n <= 450 for n in calls)
    assert sum(calls) == 1_000
