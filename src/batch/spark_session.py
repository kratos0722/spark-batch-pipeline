"""SparkSession construction."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


def build_spark(app_name: str = "spark-batch-pipeline", master: str | None = None) -> SparkSession:
    # Ensure any Python workers use the same interpreter as the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    return (
        builder
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
