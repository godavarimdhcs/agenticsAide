"""
DLT Bronze → Silver pipeline with data quality expectations.

This file is deployed as a Databricks Delta Live Tables (DLT) pipeline.
Run it from the Databricks UI or via a DABS-deployed workflow; it cannot
be executed as a plain Python script.

Naming conventions followed:
    Bronze tables:  <catalog>.bronze.<source>_<table>
    Silver tables:  <catalog>.silver.<source>_<table>

DLT pipeline parameters (set in databricks.yml or Workflows UI):
    source_catalog   – Unity Catalog catalog (e.g. dev_lakehouse)
    source_system    – logical source tag   (e.g. crm)
"""
import dlt  # type: ignore[import]
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# ---------------------------------------------------------------------------
# Pipeline-level parameters (resolved at DLT runtime)
# ---------------------------------------------------------------------------

spark = dlt.get_spark()  # type: ignore[attr-defined]

SOURCE_CATALOG = spark.conf.get("pipeline.source_catalog", "dev_lakehouse")
SOURCE_SYSTEM = spark.conf.get("pipeline.source_system", "crm")

BRONZE_DB = f"{SOURCE_CATALOG}.bronze"
SILVER_DB = f"{SOURCE_CATALOG}.silver"


# ---------------------------------------------------------------------------
# Helper: read Bronze table
# ---------------------------------------------------------------------------

def _bronze(table: str) -> DataFrame:
    return dlt.read(f"{BRONZE_DB}.{SOURCE_SYSTEM}_{table}")


# ---------------------------------------------------------------------------
# Silver: customers  (example table)
# ---------------------------------------------------------------------------

@dlt.table(
    name=f"{SILVER_DB}.{SOURCE_SYSTEM}_customers",
    comment="Cleansed and deduplicated customer records",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'")
@dlt.expect("non_negative_age", "age >= 0")
def silver_customers() -> DataFrame:
    return (
        _bronze("customers")
        .dropDuplicates(["customer_id"])
        .withColumn(
            "updated_at",
            F.col("updated_at").cast(TimestampType()),
        )
        .withColumn("_silver_ingest_time", F.current_timestamp())
        .drop("_watermark_value", "_batch_id")
    )


# ---------------------------------------------------------------------------
# Silver: orders  (example table)
# ---------------------------------------------------------------------------

@dlt.table(
    name=f"{SILVER_DB}.{SOURCE_SYSTEM}_orders",
    comment="Cleansed order records",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_ref", "customer_id IS NOT NULL")
@dlt.expect("positive_amount", "amount > 0")
def silver_orders() -> DataFrame:
    return (
        _bronze("orders")
        .dropDuplicates(["order_id"])
        .withColumn(
            "order_date",
            F.col("order_date").cast(TimestampType()),
        )
        .withColumn("_silver_ingest_time", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Silver: sftp Excel reports  (example table)
# ---------------------------------------------------------------------------

@dlt.table(
    name=f"{SILVER_DB}.finance_reports",
    comment="Cleansed finance reports from SFTP Excel ingestion",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_report_date", "report_date IS NOT NULL")
@dlt.expect("non_null_amount", "amount IS NOT NULL")
def silver_finance_reports() -> DataFrame:
    return (
        dlt.read(f"{BRONZE_DB}.finance_reports")
        .withColumn(
            "report_date",
            F.to_date(F.col("report_date"), "yyyy-MM-dd"),
        )
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("_silver_ingest_time", F.current_timestamp())
        .dropDuplicates(["report_date", "entity_id"])
    )
