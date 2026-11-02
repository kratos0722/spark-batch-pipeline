"""CLI: run the batch job, or a storage-free in-memory demo."""

from __future__ import annotations

import argparse

from .job import run_batch
from .logging_setup import configure_logging
from .spark_session import build_spark
from .transforms import clean_orders, daily_revenue

_DEMO_ORDERS_SQL = """
SELECT * FROM VALUES
  ('o1','c1','widget', CAST(10.00 AS double),'PAID',    TIMESTAMP '2026-01-01 09:00:00'),
  ('o1','c1','widget', CAST(12.00 AS double),'SHIPPED', TIMESTAMP '2026-01-01 10:00:00'),
  ('o2','c2','gadget', CAST(25.00 AS double),'PAID',    TIMESTAMP '2026-01-01 11:00:00'),
  ('o3','c3','widget', CAST(-5.00 AS double),'PAID',    TIMESTAMP '2026-01-01 12:00:00'),
  ('o4','c4','gadget', CAST(40.00 AS double),'PLACED',  TIMESTAMP '2026-01-02 09:00:00')
AS t(order_id, customer_id, product, amount, status, event_ts)
"""


def cmd_run(args: argparse.Namespace) -> int:
    spark = build_spark("spark-batch-pipeline")
    try:
        rows = run_batch(spark, args.input, args.output, args.watermark)
        print(f"wrote {rows} aggregate rows to {args.output}")
    finally:
        spark.stop()
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    spark = build_spark("spark-batch-pipeline-demo", master="local[*]")
    try:
        raw = spark.sql(_DEMO_ORDERS_SQL)
        daily = daily_revenue(clean_orders(raw))
        print("Daily revenue (cleaned + de-duplicated):")
        for r in daily.collect():
            print(f"  {r['ds']}  {r['product']:<8s} revenue={r['revenue']:>8.2f} orders={r['orders']}")
    finally:
        spark.stop()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="batch", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run the batch job over Parquet input")
    run.add_argument("--input", required=True)
    run.add_argument("--output", required=True)
    run.add_argument("--watermark", default=None, help="only process events after this timestamp")
    run.set_defaults(func=cmd_run)

    demo = sub.add_parser("demo", help="Run an in-memory demo (no storage needed)")
    demo.set_defaults(func=cmd_demo)

    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    return args.func(args)
