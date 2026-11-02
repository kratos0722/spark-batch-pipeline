"""Batch job orchestration: read -> incremental -> clean -> aggregate -> write."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from .io import read_orders, write_partitioned
from .schema import validate_columns
from .transforms import clean_orders, daily_revenue, incremental_filter

log = logging.getLogger("batch.job")


def run_batch(spark: SparkSession, input_path: str, output_path: str,
              watermark: str | None = None) -> int:
    """Run the full batch and return the number of aggregate rows written."""
    raw = read_orders(spark, input_path)
    validate_columns(raw)
    log.info("read input path=%s", input_path)

    if watermark:
        raw = incremental_filter(raw, watermark)
        log.info("applied incremental watermark=%s", watermark)

    cleaned = clean_orders(raw)
    daily = daily_revenue(cleaned)

    write_partitioned(daily, output_path)
    rows = daily.count()
    log.info("wrote aggregates rows=%d path=%s", rows, output_path)
    return rows
