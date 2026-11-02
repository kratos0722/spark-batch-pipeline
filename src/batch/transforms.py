"""Pure DataFrame-to-DataFrame transforms — the testable core of the pipeline."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

VALID_STATUSES = ["PLACED", "PAID", "SHIPPED", "REFUNDED"]


def dedupe_latest(df: DataFrame, key: str = "order_id", ts: str = "event_ts") -> DataFrame:
    """Keep only the most recent row per ``key`` (late/replayed events win)."""
    window = Window.partitionBy(key).orderBy(F.col(ts).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def clean_orders(df: DataFrame) -> DataFrame:
    """Drop invalid rows (null keys, non-positive amount, unknown status) and
    de-duplicate to the latest event per order."""
    cleaned = (
        df.filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
        .filter(F.col("amount") > 0)
        .filter(F.col("status").isin(VALID_STATUSES))
    )
    return dedupe_latest(cleaned)


def incremental_filter(df: DataFrame, watermark: str, ts: str = "event_ts") -> DataFrame:
    """Keep only rows strictly newer than ``watermark`` (incremental loading)."""
    return df.filter(F.col(ts) > F.lit(watermark).cast("timestamp"))


def daily_revenue(df: DataFrame) -> DataFrame:
    """Aggregate revenue and distinct order counts per day and product."""
    return (
        df.withColumn("ds", F.to_date("event_ts"))
        .groupBy("ds", "product")
        .agg(
            F.round(F.sum("amount"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
        )
        .orderBy("ds", "product")
    )
