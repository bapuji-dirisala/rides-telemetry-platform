"""Shared config for gold streaming jobs.

Mirrors :mod:`~rides_telemetry.silver.streaming` so the CLI and
mart implementations look symmetric across layers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from rides_telemetry.spark import LAKEHOUSE_ROOT


@dataclass(frozen=True, slots=True)
class GoldStreamConfig:
    """Runtime knobs for gold streaming jobs."""

    warehouse_root: Path = field(default_factory=lambda: LAKEHOUSE_ROOT / "warehouse")
    """Where all Delta tables (silver source + gold sink) live."""

    trigger_processing_time: str | None = "30 seconds"
    """Micro-batch cadence. Set to ``None`` for ``Trigger.AvailableNow``.

    Gold runs slower than silver — aggregation is expensive per batch,
    and dashboards / surge services don't need sub-30 s freshness for
    a 1-minute rolling window."""

    max_files_per_trigger: int = 100
    """Cap on Delta source files read per micro-batch."""
