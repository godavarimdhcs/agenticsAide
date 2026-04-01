"""
File manifest for idempotent SFTP/Excel ingestion.

The manifest is a Delta table that tracks every file that has been
successfully ingested.  Before processing a file, its checksum + size +
mtime are compared against the manifest.  Files that match are skipped.

Manifest table schema:
    file_name        STRING
    remote_path      STRING
    file_size        LONG
    file_mtime       LONG
    checksum_md5     STRING
    landing_path     STRING   – ADLS path where file was written
    processed_at     TIMESTAMP
    batch_id         STRING
    status           STRING   (success | failed)
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class FileManifest:
    """Idempotency guard using a Delta manifest table."""

    def __init__(self, spark: SparkSession, manifest_table: str) -> None:
        """
        Args:
            spark:           Active SparkSession.
            manifest_table:  Fully-qualified Delta table name, e.g.
                             ``bronze.metadata.sftp_manifest``.
        """
        self._spark = spark
        self._table = manifest_table
        self._ensure_table()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_processed(self, file_meta: dict[str, Any], checksum: str) -> bool:
        """Return True if the file (by checksum + size) has already been processed."""
        from pyspark.sql import functions as F  # lazy import; requires Spark at runtime

        rows = (
            self._spark.table(self._table)
            .filter(
                (F.col("file_name") == file_meta["name"])
                & (F.col("checksum_md5") == checksum)
                & (F.col("file_size") == int(file_meta["size"]))
                & (F.col("status") == "success")
            )
            .limit(1)
            .collect()
        )
        return len(rows) > 0

    def record(
        self,
        file_meta: dict[str, Any],
        checksum: str,
        landing_path: str,
        batch_id: str,
        status: str = "success",
    ) -> None:
        """Write a manifest entry for the processed file."""
        now = datetime.now(tz=timezone.utc)
        row_df = self._spark.createDataFrame(
            [
                {
                    "file_name": file_meta["name"],
                    "remote_path": file_meta["remote_path"],
                    "file_size": int(file_meta["size"]),
                    "file_mtime": int(file_meta["mtime"]),
                    "checksum_md5": checksum,
                    "landing_path": landing_path,
                    "processed_at": now,
                    "batch_id": batch_id,
                    "status": status,
                }
            ]
        )
        row_df.write.format("delta").mode("append").saveAsTable(self._table)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_table(self) -> None:
        self._spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table} (
                file_name    STRING  NOT NULL,
                remote_path  STRING,
                file_size    LONG,
                file_mtime   LONG,
                checksum_md5 STRING,
                landing_path STRING,
                processed_at TIMESTAMP,
                batch_id     STRING,
                status       STRING
            )
            USING DELTA
            """
        )


# ---------------------------------------------------------------------------
# Standalone checksum helper (no Spark required)
# ---------------------------------------------------------------------------

def md5_checksum(file_path: str, chunk_size: int = 1 << 20) -> str:
    """Return the MD5 hex digest of a local file."""
    h = hashlib.md5()
    with open(file_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()
