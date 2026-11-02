"""PySpark batch ETL pipeline.

Reads raw order events, cleans and de-duplicates them, applies incremental
(watermark-based) loading, aggregates daily revenue per product, and writes the
result to a date-partitioned warehouse. The transforms are pure
DataFrame-to-DataFrame functions, so they are unit-tested on a local Spark
session without any external storage.
"""

__version__ = "0.1.0"
