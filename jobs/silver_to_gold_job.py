"""
Silver → Gold transformation job entry point.

Invoked by a Databricks Workflow task after the DLT Bronze→Silver pipeline
completes.

Parameters:
    --env      dev | qa | prod         (default: dev)
    --catalog  Unity Catalog catalog    (default: resolved from env)
    --transform all | daily_sales | finance  (default: all)
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

from src.transforms.gold.gold_transforms import (
    run_daily_sales_summary,
    run_finance_summary,
)

_ENV_CATALOG_MAP = {
    "dev": "dev_lakehouse",
    "qa": "qa_lakehouse",
    "prod": "prod_lakehouse",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver → Gold transforms")
    parser.add_argument("--env", default="dev", choices=["dev", "qa", "prod"])
    parser.add_argument("--catalog", default=None)
    parser.add_argument(
        "--transform",
        default="all",
        choices=["all", "daily_sales", "finance"],
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    catalog = args.catalog or _ENV_CATALOG_MAP[args.env]

    spark = (
        SparkSession.builder.appName(f"silver-to-gold-{args.env}")
        .getOrCreate()
    )

    if args.transform in ("all", "daily_sales"):
        run_daily_sales_summary(spark, catalog=catalog)

    if args.transform in ("all", "finance"):
        run_finance_summary(spark, catalog=catalog)

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
