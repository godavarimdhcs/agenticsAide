"""
Silver → Gold transformation pattern.

Gold tables are business-oriented aggregates or curated views.
Each Gold transform is a pure Spark job (no DLT dependency) so it can be
scheduled independently or triggered as a task in a Databricks Workflow.

This module shows the recommended pattern:
  1. Read from Silver (Delta tables in Unity Catalog).
  2. Apply business logic / aggregation.
  3. Write to Gold layer using MERGE (upsert) for idempotency.

Example Gold table: daily_sales_summary
    Aggregates orders + customer enrichment from Silver.

Usage (called from jobs/silver_to_gold_job.py or a notebook):
    from src.transforms.gold.gold_transforms import run_daily_sales_summary
    run_daily_sales_summary(spark, catalog="dev_lakehouse")
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def run_daily_sales_summary(
    spark: SparkSession,
    catalog: str = "dev_lakehouse",
    source_system: str = "crm",
) -> None:
    """Aggregate Silver orders + customers into a Gold daily sales summary.

    The result is upserted into ``<catalog>.gold.daily_sales_summary``
    keyed on (report_date, region).

    Args:
        spark:         Active SparkSession.
        catalog:       Unity Catalog catalog name.
        source_system: Logical source system label.
    """
    silver_db = f"{catalog}.silver"
    gold_db = f"{catalog}.gold"
    target_table = f"{gold_db}.daily_sales_summary"

    orders = spark.table(f"{silver_db}.{source_system}_orders")
    customers = spark.table(f"{silver_db}.{source_system}_customers")

    # Business logic: join, aggregate
    summary: DataFrame = (
        orders.join(customers, on="customer_id", how="left")
        .withColumn("report_date", F.to_date("order_date"))
        .groupBy("report_date", "region")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("_gold_ingest_time", F.current_timestamp())
    )

    # Ensure Gold schema / table exists
    _ensure_gold_table(spark, target_table)

    # Upsert (MERGE) for idempotency
    summary.createOrReplaceTempView("_gold_update")
    spark.sql(
        f"""
        MERGE INTO {target_table} AS tgt
        USING _gold_update AS src
          ON tgt.report_date = src.report_date
         AND tgt.region      = src.region
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
        """
    )


# ---------------------------------------------------------------------------
# Finance reports Gold aggregate (from SFTP Excel Silver)
# ---------------------------------------------------------------------------

def run_finance_summary(
    spark: SparkSession,
    catalog: str = "dev_lakehouse",
) -> None:
    """Aggregate Silver finance reports into a Gold monthly finance summary.

    Upserts into ``<catalog>.gold.monthly_finance_summary``.
    """
    silver_db = f"{catalog}.silver"
    gold_db = f"{catalog}.gold"
    target_table = f"{gold_db}.monthly_finance_summary"

    reports = spark.table(f"{silver_db}.finance_reports")

    summary: DataFrame = (
        reports.withColumn("year_month", F.date_format("report_date", "yyyy-MM"))
        .groupBy("year_month", "entity_id")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("*").alias("row_count"),
        )
        .withColumn("_gold_ingest_time", F.current_timestamp())
    )

    _ensure_gold_table(spark, target_table)

    summary.createOrReplaceTempView("_finance_gold_update")
    spark.sql(
        f"""
        MERGE INTO {target_table} AS tgt
        USING _finance_gold_update AS src
          ON tgt.year_month = src.year_month
         AND tgt.entity_id  = src.entity_id
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
        """
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_gold_table(spark: SparkSession, full_table_name: str) -> None:
    """Create the Gold table using CTAS if it does not exist (first run).

    Args:
        spark:           Active SparkSession.
        full_table_name: Fully-qualified table name (catalog.schema.table).
                         Must match the pattern ``<word>.<word>.<word>`` to
                         prevent SQL injection from configuration values.
    """
    import re

    if not re.match(r"^[\w`]+\.[\w`]+\.[\w`]+$", full_table_name):
        raise ValueError(f"Invalid table name: {full_table_name!r}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA AS "
        "SELECT * FROM (SELECT 1) WHERE 1=0"
    )
