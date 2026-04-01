"""
Postgres → Bronze ingestion job entry point.

Invoked by a Databricks Workflow task.  Accepts environment and config
path as task parameters.

Parameters (passed via Databricks Workflow task `python_wheel_task` or
as notebook widgets):
    --env      dev | qa | prod   (default: dev)
    --config   path to config YAML relative to repo root
               (default: conf/<env>/postgres_config.yml)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from src.ingestion.postgres.postgres_ingestor import PostgresIngestor
from src.utils.config import load_config


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Postgres → Bronze ingestion")
    parser.add_argument("--env", default="dev", choices=["dev", "qa", "prod"])
    parser.add_argument("--config", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    config_path = args.config or str(
        Path(__file__).resolve().parent.parent / "conf" / args.env / "postgres_config.yml"
    )

    config = load_config(config_path)

    spark = (
        SparkSession.builder.appName(
            f"postgres-ingest-{config.get('source_system', 'unknown')}-{args.env}"
        )
        .getOrCreate()
    )

    ingestor = PostgresIngestor(spark=spark, config=config)
    ingestor.run()
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
