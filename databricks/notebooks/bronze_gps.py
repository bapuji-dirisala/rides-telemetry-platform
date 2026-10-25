# Databricks notebook source
# ----------------------------------------------------------------------
# Bronze GPS — Databricks Workflows entrypoint that re-uses the shared
# rides_telemetry package. The actual stream config lives in the
# library, not the notebook, so the laptop + Databricks runs are the
# same code path.
# ----------------------------------------------------------------------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog") if "dbutils" in dir() else "rides_dev"  # type: ignore[name-defined]
LAKEHOUSE_ROOT = dbutils.widgets.get("lakehouse_root") if "dbutils" in dir() else "/tmp/warehouse"  # type: ignore[name-defined]

# COMMAND ----------

from rides_telemetry.bronze.streaming import BronzeStreamConfig, run_bronze_stream

spark.sql(f"USE CATALOG {CATALOG}")  # type: ignore[name-defined]

config = BronzeStreamConfig(topic="rides.gps", once=False)
run_bronze_stream(spark, config)  # type: ignore[name-defined]
