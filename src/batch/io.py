"""Read/write adapters for the warehouse (Parquet; BigQuery via connector in prod)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from .schema import ORDERS_SCHEMA


def read_orders(spark: SparkSession, path: str) -> DataFrame:
    """Read raw order events. Parquet keeps the declared types; the schema is
    applied explicitly so a bad file fails loudly rather than silently."""
    return spark.read.schema(ORDERS_SCHEMA).parquet(path)


def write_partitioned(df: DataFrame, path: str, partition_col: str = "ds", mode: str = "overwrite") -> None:
    """Write the curated output partitioned by day — the physical layout that
    keeps downstream date-filtered queries and incremental loads cheap."""
    (
        df.write.mode(mode)
        .partitionBy(partition_col)
        .parquet(path)
    )
