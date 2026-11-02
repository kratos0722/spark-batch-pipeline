"""Expected input schema and a lightweight validation helper."""

from __future__ import annotations

from pyspark.sql import DataFrame

# Raw order events as they land in the lake.
ORDERS_SCHEMA = (
    "order_id STRING, customer_id STRING, product STRING, "
    "amount DOUBLE, status STRING, event_ts TIMESTAMP"
)

REQUIRED_COLUMNS = ["order_id", "customer_id", "product", "amount", "status", "event_ts"]


def validate_columns(df: DataFrame, required: list[str] | None = None) -> None:
    """Raise if the DataFrame is missing any required column."""
    needed = required or REQUIRED_COLUMNS
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Input is missing required columns: {missing}")
