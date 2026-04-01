"""
Collibra metadata + lineage publish job entry point.

Reads Unity Catalog metadata and pushes to Collibra DGC.

Parameters:
    --env      dev | qa | prod   (default: dev)
    --catalog  catalog override  (default: resolved from env)
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession

from src.collibra.lineage import CollibraLineageBulkPublisher
from src.collibra.publisher import CollibraPublisher

_ENV_CATALOG_MAP = {
    "dev": "dev_lakehouse",
    "qa": "qa_lakehouse",
    "prod": "prod_lakehouse",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collibra publish job")
    parser.add_argument("--env", default="dev", choices=["dev", "qa", "prod"])
    parser.add_argument("--catalog", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    catalog = args.catalog or _ENV_CATALOG_MAP[args.env]

    spark = SparkSession.builder.appName(f"collibra-publish-{args.env}").getOrCreate()

    # Collibra connection – resolved from env vars / Databricks secrets
    collibra = CollibraPublisher(
        base_url=os.environ.get("COLLIBRA_BASE_URL"),
        username=os.environ.get("COLLIBRA_USERNAME"),
        password=os.environ.get("COLLIBRA_PASSWORD"),
    )

    publisher = CollibraLineageBulkPublisher(
        spark=spark, catalog=catalog, collibra=collibra
    )
    publisher.publish_all()

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
