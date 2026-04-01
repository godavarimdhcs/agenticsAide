"""
Config-driven, incremental PostgreSQL → Bronze Delta ingestion.

Config keys (YAML):
    source_system:    human label, e.g. "crm"
    jdbc_database:    database name
    jdbc_host:        hostname / FQDN
    jdbc_port:        port (default 5432)
    tables:
      - schema:           public
        table:            customers
        watermark_col:    updated_at
        target_table:     bronze.crm.customers
        partition_column: id         (optional, for parallel JDBC reads)
        num_partitions:   8          (optional)
    watermark_table:  bronze.metadata.watermarks
    secret_scope:     dev-postgres   (Databricks secret scope)

Metadata columns appended to every ingested row:
    _ingest_time      TIMESTAMP  – wall-clock time of this batch
    _batch_id         STRING     – unique run identifier (UUID)
    _watermark_value  STRING     – watermark value used for this batch
    _source_system    STRING     – value of source_system config key
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from src.ingestion.postgres.watermark import WatermarkManager
from src.utils.secrets import get_secret


class PostgresIngestor:
    """Incremental JDBC ingestor for PostgreSQL tables."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]) -> None:
        """
        Args:
            spark:  Active SparkSession.
            config: Parsed ingestion config dictionary.
        """
        self._spark = spark
        self._cfg = config
        self._batch_id = str(uuid.uuid4())
        self._ingest_time = datetime.now(tz=timezone.utc)
        self._wm_manager = WatermarkManager(
            spark=spark,
            watermark_table=config["watermark_table"],
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Ingest all tables defined in config."""
        for table_cfg in self._cfg["tables"]:
            self._ingest_table(table_cfg)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _jdbc_url(self) -> str:
        host = self._cfg["jdbc_host"]
        port = self._cfg.get("jdbc_port", 5432)
        db = self._cfg["jdbc_database"]
        return f"jdbc:postgresql://{host}:{port}/{db}"

    def _jdbc_properties(self) -> dict[str, str]:
        scope = self._cfg["secret_scope"]
        return {
            "user": get_secret(scope, "username"),
            "password": get_secret(scope, "password"),
            "driver": "org.postgresql.Driver",
            "fetchsize": str(self._cfg.get("fetchsize", 10000)),
        }

    def _ingest_table(self, table_cfg: dict[str, Any]) -> None:
        source_system = self._cfg["source_system"]
        schema = table_cfg["schema"]
        table = table_cfg["table"]
        qualified = f"{schema}.{table}"
        watermark_col = table_cfg["watermark_col"]
        target_table = table_cfg["target_table"]

        current_wm = self._wm_manager.get_watermark(source_system, qualified)

        # Build predicate pushdown query
        if current_wm:
            query = (
                f"(SELECT * FROM {qualified} "
                f"WHERE {watermark_col} > '{current_wm}') AS inc_load"
            )
        else:
            query = f"(SELECT * FROM {qualified}) AS full_load"

        read_opts: dict[str, Any] = {
            "url": self._jdbc_url(),
            "query": query,
            "properties": self._jdbc_properties(),
        }

        # Optional parallel partitioned read
        if "partition_column" in table_cfg:
            read_opts.update(
                {
                    "partitionColumn": table_cfg["partition_column"],
                    "numPartitions": str(table_cfg.get("num_partitions", 8)),
                    "lowerBound": str(table_cfg.get("lower_bound", 0)),
                    "upperBound": str(table_cfg.get("upper_bound", 2147483647)),
                }
            )

        df = self._spark.read.jdbc(**read_opts)  # type: ignore[arg-type]

        if df.rdd.isEmpty():
            return

        # Attach metadata columns via selectExpr (avoids top-level pyspark import)
        ingest_time_str = self._ingest_time.isoformat()
        df.createOrReplaceTempView("_pg_batch")
        df = self._spark.sql(
            f"""
            SELECT *,
                   CAST('{ingest_time_str}' AS TIMESTAMP) AS _ingest_time,
                   '{self._batch_id}'                     AS _batch_id,
                   '{current_wm or ""}'                   AS _watermark_value,
                   '{source_system}'                       AS _source_system
            FROM _pg_batch
            """
        )

        # Write to Bronze Delta (append)
        df.write.format("delta").mode("append").saveAsTable(target_table)

        # Update watermark to max value seen in this batch
        new_wm = (
            df.selectExpr(f"MAX(CAST({watermark_col} AS STRING)) AS max_wm")
            .collect()[0]["max_wm"]
        )
        if new_wm:
            self._wm_manager.set_watermark(
                source_system=source_system,
                table_name=qualified,
                watermark_col=watermark_col,
                watermark_value=new_wm,
            )
