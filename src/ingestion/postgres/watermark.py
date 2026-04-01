"""
Watermark manager for incremental ingestion.

Tracks the last-seen watermark value for each source table in a Delta
metadata table so that subsequent runs only load new/updated rows.

Schema of the watermark Delta table (managed by Unity Catalog):
    source_system   STRING
    table_name      STRING
    watermark_col   STRING
    watermark_value STRING   (stored as string; cast to target type at read)
    updated_at      TIMESTAMP
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_.`\-]+$")


def _validate_identifier(name: str) -> None:
    """Raise ValueError if *name* is not a safe SQL identifier."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")


def _escape_sql_string(value: str) -> str:
    """Escape single quotes in a string value for safe SQL literal embedding."""
    return value.replace("'", "''")


class WatermarkManager:
    """Read and write watermark values from/to a Delta table."""

    def __init__(
        self,
        spark: SparkSession,
        watermark_table: str,
    ) -> None:
        """
        Args:
            spark:            Active SparkSession.
            watermark_table:  Fully-qualified Delta table, e.g.
                              ``bronze.metadata.watermarks``.
        """
        self._spark = spark
        self._table = watermark_table
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the watermark table if it does not exist."""
        self._spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table} (
                source_system  STRING  NOT NULL,
                table_name     STRING  NOT NULL,
                watermark_col  STRING  NOT NULL,
                watermark_value STRING,
                updated_at     TIMESTAMP NOT NULL
            )
            USING DELTA
            """
        )

    def get_watermark(
        self, source_system: str, table_name: str
    ) -> Optional[str]:
        """Return the current watermark value or None if not yet recorded."""
        from pyspark.sql import functions as F  # lazy import

        rows = (
            self._spark.table(self._table)
            .filter(
                (F.col("source_system") == source_system)
                & (F.col("table_name") == table_name)
            )
            .select("watermark_value")
            .collect()
        )
        if rows:
            return rows[0]["watermark_value"]
        return None

    def set_watermark(
        self,
        source_system: str,
        table_name: str,
        watermark_col: str,
        watermark_value: str,
    ) -> None:
        """Upsert the watermark value for the given source/table pair."""
        _validate_identifier(self._table)
        now = datetime.now(tz=timezone.utc).isoformat()
        # Escape single quotes to prevent SQL injection from config/data values
        esc = _escape_sql_string
        self._spark.sql(
            f"""
            MERGE INTO {self._table} AS target
            USING (
                SELECT
                    '{esc(source_system)}'       AS source_system,
                    '{esc(table_name)}'           AS table_name,
                    '{esc(watermark_col)}'        AS watermark_col,
                    '{esc(watermark_value)}'      AS watermark_value,
                    CAST('{esc(now)}' AS TIMESTAMP) AS updated_at
            ) AS source
              ON  target.source_system = source.source_system
              AND target.table_name    = source.table_name
            WHEN MATCHED THEN
              UPDATE SET
                watermark_col   = source.watermark_col,
                watermark_value = source.watermark_value,
                updated_at      = source.updated_at
            WHEN NOT MATCHED THEN
              INSERT *
            """
        )
