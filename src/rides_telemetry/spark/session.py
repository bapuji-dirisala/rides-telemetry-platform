"""Builder for a local Spark session with Delta + Kafka wired in.

Design goals:

- **Just works on a fresh Mac.** Auto-detects Homebrew's ``openjdk@17``
  if ``JAVA_HOME`` isn't set, so users don't have to touch shell rc
  files. Raises a clear, actionable error when no JDK is available.
- **Same code, laptop or Databricks.** All local-mode / warehouse / jar
  choices live in :class:`SparkSessionConfig`. In phase 10 we swap the
  config (or use the Databricks-provided session) without changing any
  business logic.
- **Delta + Kafka connectors auto-fetched.** ``spark.jars.packages`` is
  set so Spark downloads the right Delta and spark-sql-kafka jars from
  Maven Central on first run. Delta SQL extensions are enabled so
  ``CREATE TABLE ... USING delta`` and ``MERGE INTO`` work.
- **Local warehouse under the repo.** Everything Spark writes lives in
  ``lakehouse/`` — already gitignored, easy to inspect, trivial to
  ``rm -rf`` and start clean.
"""

from __future__ import annotations

import logging
import os
import platform
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

_log = logging.getLogger(__name__)

# All Spark/Delta artefacts live under this dir. Gitignored via the
# top-level ``lakehouse/`` rule in ``.gitignore``.
LAKEHOUSE_ROOT = Path("lakehouse")

# Pinned to match the ``spark`` extra in pyproject.toml. Bumping either
# side requires bumping both — Delta / Spark / Scala versions are
# tightly coupled.
_DELTA_VERSION = "3.2.1"
_SPARK_VERSION = "3.5.3"
_SCALA_BINARY = "2.12"

_DEFAULT_PACKAGES = (
    f"io.delta:delta-spark_{_SCALA_BINARY}:{_DELTA_VERSION},"
    f"org.apache.spark:spark-sql-kafka-0-10_{_SCALA_BINARY}:{_SPARK_VERSION}"
)


_DEFAULT_IVY_JARS_DIR = Path.home() / ".ivy2" / "jars"


@dataclass(frozen=True, slots=True)
class SparkSessionConfig:
    """Everything the SparkSession builder needs, in one place."""

    app_name: str = "rides-telemetry"
    """Shows up in the Spark UI (``http://localhost:4040``) and driver logs."""

    master: str = "local[*]"
    """Local mode uses one process, all cores. Override to ``spark://…`` or ``databricks`` later."""

    warehouse_dir: Path = field(default_factory=lambda: LAKEHOUSE_ROOT / "warehouse")
    """Where managed Delta tables live on disk."""

    packages: str = _DEFAULT_PACKAGES
    """Maven coordinates for Delta + spark-sql-kafka.

    Used only when ``jars_dir`` doesn't already contain the required
    jars — otherwise ``spark.jars`` (direct file paths) is used instead,
    which is far more reliable behind corporate TLS-inspecting proxies
    that break the JVM's default trust store on Maven Central.
    """

    jars_dir: Path = field(default_factory=lambda: _DEFAULT_IVY_JARS_DIR)
    """Directory of pre-downloaded jars to pass via ``spark.jars``.

    Defaults to Ivy's cache dir (``~/.ivy2/jars``). If it exists and
    contains a Delta jar, the builder uses ``spark.jars`` and skips
    the ``packages`` resolution entirely — no network hit on startup.
    """

    driver_memory: str = "2g"
    """Enough for local dev with millions-row Delta tables."""

    shuffle_partitions: int = 4
    """Default 200 is wasteful on a laptop; four keeps small streaming batches efficient."""

    log_level: str = "WARN"
    """``WARN`` silences Spark's verbose startup INFO logs. Bump to ``INFO`` when debugging."""

    extra_conf: dict[str, str] = field(default_factory=dict)
    """Escape hatch for any ``spark.…`` setting not exposed as a field."""


def build_spark_session(config: SparkSessionConfig | None = None) -> SparkSession:
    """Create (or return the existing) local SparkSession.

    Downloads Delta + Kafka connector jars from Maven Central on first
    call (~30 seconds); subsequent calls in the same process return the
    cached session immediately.
    """
    _ensure_java_home()
    _ensure_pyspark_python()

    # Local import so importing this module never requires pyspark.
    from pyspark.sql import SparkSession

    cfg = config or SparkSessionConfig()
    cfg.warehouse_dir.mkdir(parents=True, exist_ok=True)

    builder = SparkSession.builder.appName(cfg.app_name).master(cfg.master)

    # Prefer pre-downloaded jars (no network) over Maven resolution.
    cached_jars = _cached_jars_or_none(cfg.jars_dir)
    if cached_jars:
        _log.info("using %d pre-downloaded jars from %s", len(cached_jars), cfg.jars_dir)
        builder = builder.config("spark.jars", ",".join(str(j) for j in cached_jars))
    else:
        _log.info("no cached jars found; resolving packages=%s via Ivy", cfg.packages)
        builder = builder.config("spark.jars.packages", cfg.packages)

    builder = (
        builder
        # ---- Delta wiring -------------------------------------------------
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ---- Warehouse ----------------------------------------------------
        .config("spark.sql.warehouse.dir", str(cfg.warehouse_dir.resolve()))
        # ---- Performance --------------------------------------------------
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        # Delta's auto-optimize + auto-compact keep bronze small files
        # under control on a streaming write. Very useful, essentially
        # free at our volumes.
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
    )
    for k, v in cfg.extra_conf.items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(cfg.log_level)
    _log.info("SparkSession ready — version=%s", spark.version)
    return spark


def default_checkpoint_dir(name: str) -> Path:
    """Standard checkpoint location for a given streaming job.

    Checkpoints are what give Structured Streaming its exactly-once
    guarantee — they record the Kafka offsets and Delta versions that
    the job has already committed, so a restart resumes precisely
    where the previous run left off.
    """
    return LAKEHOUSE_ROOT / "_checkpoints" / name


# ---- internals ----------------------------------------------------------


def _ensure_java_home() -> None:
    """Populate ``JAVA_HOME`` if the user hasn't set it.

    Local Spark refuses to start without ``JAVA_HOME``. On macOS with
    Homebrew we can auto-discover ``openjdk@17`` at its well-known
    keg-only path, sparing the user a shell rc edit. On Linux / CI
    we require the environment to be set up externally.
    """
    if os.environ.get("JAVA_HOME"):
        return

    if platform.system() == "Darwin":
        brew_home = Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
        if brew_home.is_dir():
            os.environ["JAVA_HOME"] = str(brew_home)
            _log.info("auto-detected JAVA_HOME=%s", brew_home)
            return
        # Intel-Mac homebrew prefix.
        intel_home = Path("/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
        if intel_home.is_dir():
            os.environ["JAVA_HOME"] = str(intel_home)
            _log.info("auto-detected JAVA_HOME=%s", intel_home)
            return

    raise RuntimeError(
        "JAVA_HOME is not set and no OpenJDK 17 was auto-detected. "
        "Install a JDK 11 or 17 (macOS: `brew install openjdk@17`, "
        "Ubuntu: `apt install openjdk-17-jdk`) and re-run."
    )


def _cached_jars_or_none(jars_dir: Path) -> list[Path] | None:
    """Return the list of jar files in ``jars_dir`` if it looks usable.

    ``Usable`` means: the dir exists and contains a Delta jar. If Delta
    isn't there, Spark won't be able to write Delta tables anyway, so
    we fall back to Ivy resolution rather than start a session that
    would fail on the first Delta operation.
    """
    if not jars_dir.is_dir():
        return None
    jars = sorted(jars_dir.glob("*.jar"))
    if not jars:
        return None
    if not any("delta-spark" in j.name for j in jars):
        return None
    return jars


def _ensure_pyspark_python() -> None:
    """Pin Spark's Python worker path to the currently-running interpreter.

    Without this, macOS's default ``python3`` (system 3.9 in
    ``/usr/bin/``) gets picked up by Spark's Java worker launcher
    while the driver runs in the venv's 3.11. That mismatch fails
    every UDF / Arrow-serialised operation with a cryptic
    ``PYTHON_VERSION_MISMATCH``. Setting both env vars to
    ``sys.executable`` keeps driver and workers in lockstep.
    """
    for var in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
        if not os.environ.get(var):
            os.environ[var] = sys.executable
