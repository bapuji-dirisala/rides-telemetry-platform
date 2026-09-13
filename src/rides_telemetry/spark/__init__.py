"""Spark session helpers for the rides telemetry platform.

Every Spark entry point in this project (bronze streaming, silver
transformations, gold marts) goes through :func:`build_spark_session`
so packaging, jars, checkpointing, and warehouse locations stay
consistent across phases.
"""

from __future__ import annotations

from rides_telemetry.spark.session import (
    LAKEHOUSE_ROOT,
    SparkSessionConfig,
    build_spark_session,
    default_checkpoint_dir,
)

__all__ = [
    "LAKEHOUSE_ROOT",
    "SparkSessionConfig",
    "build_spark_session",
    "default_checkpoint_dir",
]
