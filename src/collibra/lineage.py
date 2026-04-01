"""
Unity Catalog → Collibra bulk publisher.

Reads table/column metadata from Unity Catalog information_schema views
and pushes them to Collibra in bulk.

Usage (notebook or job):
    from src.collibra.lineage import CollibraLineageBulkPublisher
    pub = CollibraLineageBulkPublisher(spark, catalog="dev_lakehouse")
    pub.publish_all()
"""
from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from src.collibra.publisher import CollibraPublisher


class CollibraLineageBulkPublisher:
    """Read Unity Catalog metadata and push to Collibra."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        collibra: CollibraPublisher | None = None,
    ) -> None:
        self._spark = spark
        self._catalog = catalog
        self._collibra = collibra or CollibraPublisher()

    def publish_all(self) -> None:
        """Publish all tables and lineage edges for the configured catalog."""
        tables = self._list_tables()
        for tbl in tables:
            columns = self._list_columns(tbl["schema_name"], tbl["table_name"])
            self._collibra.publish_table(
                catalog=self._catalog,
                schema=tbl["schema_name"],
                table=tbl["table_name"],
                columns=columns,
                description=tbl.get("comment", ""),
            )

        # Lineage edges based on schema naming conventions
        self._publish_medallion_lineage(tables)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _list_tables(self) -> list[dict[str, Any]]:
        rows = self._spark.sql(
            f"""
            SELECT table_catalog, table_schema AS schema_name,
                   table_name, comment
            FROM {self._catalog}.information_schema.tables
            WHERE table_schema NOT IN ('information_schema')
            ORDER BY table_schema, table_name
            """
        ).collect()
        return [r.asDict() for r in rows]

    def _list_columns(self, schema: str, table: str) -> list[dict[str, Any]]:
        from pyspark.sql import functions as F  # lazy import

        rows = (
            self._spark.table(f"{self._catalog}.information_schema.columns")
            .filter(
                (F.col("table_schema") == schema)
                & (F.col("table_name") == table)
            )
            .select(
                F.col("column_name").alias("name"),
                F.col("data_type"),
                F.col("is_nullable").alias("nullable"),
                F.col("comment"),
            )
            .orderBy("ordinal_position")
            .collect()
        )
        return [r.asDict() for r in rows]

    def _publish_medallion_lineage(self, tables: list[dict[str, Any]]) -> None:
        """Publish lineage for Bronze, Silver, Gold tables."""
        layer_map = {
            "bronze": "bronze",
            "silver": "silver",
            "gold": "gold",
        }
        for tbl in tables:
            schema = tbl["schema_name"]
            layer = layer_map.get(schema.split("_")[0])
            if layer == "bronze":
                # Infer source from table name prefix e.g. crm_customers → crm
                parts = tbl["table_name"].split("_", 1)
                source_system = parts[0] if len(parts) > 1 else "unknown"
                fqn_bronze = f"{self._catalog}.{schema}.{tbl['table_name']}"
                fqn_silver = fqn_bronze.replace(
                    f"{self._catalog}.bronze", f"{self._catalog}.silver"
                )
                fqn_gold = fqn_bronze.replace(
                    f"{self._catalog}.bronze", f"{self._catalog}.gold"
                )
                raw_src = f"{source_system}/{tbl['table_name']}"
                self._collibra.publish_lineage(raw_src, fqn_bronze, "raw ingest")
                self._collibra.publish_lineage(fqn_bronze, fqn_silver, "DLT cleanse")
                self._collibra.publish_lineage(fqn_silver, fqn_gold, "Gold aggregate")
