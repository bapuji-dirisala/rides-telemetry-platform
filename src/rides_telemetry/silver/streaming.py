"""Shared config for silver streaming jobs.

Extracted so ``gps.py`` and ``trips.py`` don't each duplicate the
same knobs. New silver jobs (fraud, matching, etc.) can reuse this
verbatim.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from rides_telemetry.spark import LAKEHOUSE_ROOT


@dataclass(frozen=True, slots=True)
class SilverStreamConfig:
    """Runtime knobs for silver streaming jobs.

    The defaults target local dev on top of the bronze tables written
    by phase 3. Override ``warehouse_root`` to point at a different
    lakehouse (e.g. Databricks external location in phase 10).
    """

    warehouse_root: Path = field(default_factory=lambda: LAKEHOUSE_ROOT / "warehouse")
    """Where both bronze (read) and silver (write) Delta tables live."""

    trigger_processing_time: str | None = "10 seconds"
    """Micro-batch cadence. Set to ``None`` for ``Trigger.AvailableNow``.

    Silver runs slower than bronze on purpose — dedup + aggregation
    is cheaper amortised over a slightly larger batch, and downstream
    marts don't need sub-5s freshness.
    """

    max_files_per_trigger: int = 100
    """Cap on Delta source files read per micro-batch.

    Keeps the first backfill against a large bronze bounded. Delta's
    streaming source honours this via
    ``spark.databricks.delta.streaming.maxFilesPerTrigger``.
    """
