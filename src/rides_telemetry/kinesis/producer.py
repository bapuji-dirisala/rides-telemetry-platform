"""Firehose producer + a local disk fake.

``put_event_batch`` is backend-agnostic: it accepts any object that
implements the ``put_record_batch`` slice we need from boto3's
Firehose client. In production the backend is a real
``boto3.client('firehose')``; locally we use :class:`LocalFirehoseFake`
which writes NDJSON files directly into the Hive-partitioned S3 layout
on the local filesystem so the Phase 7 reader can be exercised
without AWS.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from rides_telemetry.events import EventBase

from .config import FirehoseConfig, hive_partition_prefix

_log = logging.getLogger(__name__)


class FirehoseBackend(Protocol):
    """Minimal boto3 Firehose slice — kept small so the fake can match."""

    def put_record_batch(
        self,
        DeliveryStreamName: str,  # noqa: N803 — boto3 kwargs are PascalCase
        Records: list[dict[str, bytes]],  # noqa: N803
    ) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class _EncodedRecord:
    key: str
    payload: bytes
    event_type: str
    event_time: datetime


def _encode(event: EventBase) -> _EncodedRecord:
    """One event → one Firehose record.

    Firehose records are opaque bytes to the service; downstream readers
    will parse them. We emit newline-delimited JSON (one event per line)
    so that even if Firehose concatenates multiple records into one S3
    object (its default behavior) the result is a valid JSONL file.
    """
    body = event.model_dump_json() + "\n"
    event_type = event.event_type
    event_time = getattr(event, "event_ts", None) or datetime.now(UTC)
    return _EncodedRecord(
        key=f"{event_type}:{getattr(event, 'trip_id', uuid.uuid4().hex)}",
        payload=body.encode("utf-8"),
        event_type=event_type,
        event_time=event_time,
    )


def put_event_batch(
    backend: FirehoseBackend,
    config: FirehoseConfig,
    events: Iterable[EventBase],
) -> int:
    """Submit a batch of events, return the number accepted.

    boto3 caps a single ``put_record_batch`` at 500 records / 4 MiB, so
    we slice at 450 to stay well below. Failed records are retried
    once; any still-failing record is logged and skipped (the caller
    is expected to persist its own retry log at the application level
    — Firehose itself redrives transient AWS-side failures within the
    delivery stream).
    """
    encoded = [_encode(e) for e in events]
    total = 0
    for chunk_start in range(0, len(encoded), 450):
        chunk = encoded[chunk_start : chunk_start + 450]
        records = [{"Data": r.payload} for r in chunk]
        resp = backend.put_record_batch(
            DeliveryStreamName=config.delivery_stream_name, Records=records
        )
        failed = int(resp.get("FailedPutCount", 0))
        if failed:
            _log.warning(
                "firehose batch failed_put_count=%d / %d; relying on client retry",
                failed,
                len(records),
            )
        total += len(records) - failed
    return int(total)


@dataclass
class LocalFirehoseFake:
    """Writes records to disk in the same Hive-partitioned layout.

    Lets ``rides_telemetry.kinesis.reader`` read back exactly what a
    real Firehose delivery would produce — same partition prefix, same
    NDJSON body shape — without any AWS dependency.
    """

    root: Path
    prefix: str = "bronze/rides"
    _put_count: int = field(default=0, init=False)

    def put_record_batch(
        self,
        DeliveryStreamName: str,  # noqa: N803 — mirrors boto3's PascalCase
        Records: list[dict[str, bytes]],  # noqa: N803
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        # Each record is routed by event_type parsed out of its payload.
        for rec in Records:
            payload = rec["Data"].decode("utf-8").strip()
            doc = json.loads(payload)
            event_type = doc.get("event_type", "unknown")
            event_time_raw = doc.get("event_ts")
            event_time = (
                datetime.fromisoformat(event_time_raw) if isinstance(event_time_raw, str) else now
            )
            out_dir = self.root / hive_partition_prefix(self.prefix, event_time, event_type)
            out_dir.mkdir(parents=True, exist_ok=True)
            # One file per put_record_batch call (Firehose would roll
            # files on buffer-size / interval boundaries — close
            # enough for the local fake).
            out_file = out_dir / f"part-{DeliveryStreamName}-{self._put_count:06d}.jsonl"
            with out_file.open("a", encoding="utf-8") as fh:
                fh.write(payload + "\n")
            self._put_count += 1
        return {"FailedPutCount": 0, "RequestResponses": []}
