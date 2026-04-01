"""
End-to-end SFTP → Excel → Bronze Delta ingestion pipeline.

Orchestrates:
1. Connect to SFTP and list files in the configured remote directory.
2. Skip files already recorded in the file manifest (idempotency).
3. Download new files to a local temp dir.
4. Upload to ADLS Gen2 landing zone (via dbutils.fs.cp).
5. Parse Excel sheets into Spark DataFrames.
6. Write to Bronze Delta tables.
7. Record each file in the manifest.

Config keys (sftp_config.yml):
    source_system:   e.g. "finance_reports"
    sftp_host / sftp_port / sftp_username / sftp_remote_dir
    secret_scope:    e.g. "dev-sftp"
    landing_path:    abfss://landing@<acct>.dfs.core.windows.net/sftp/dev/
    manifest_table:  bronze.metadata.sftp_manifest
    files:
      - pattern:      "*.xlsx"
        sheet_name:   Sheet1
        target_table: bronze.finance.reports
        header_row:   0
"""
from __future__ import annotations

import fnmatch
import os
import tempfile
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession

from src.ingestion.sftp_excel.excel_parser import excel_to_spark
from src.ingestion.sftp_excel.manifest import FileManifest, md5_checksum
from src.ingestion.sftp_excel.sftp_client import SFTPClient


class SFTPExcelIngestor:
    """Download, parse and land Excel files from SFTP to Bronze."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]) -> None:
        self._spark = spark
        self._cfg = config
        self._batch_id = str(uuid.uuid4())
        self._ingest_time = datetime.now(tz=timezone.utc).isoformat()
        self._manifest = FileManifest(spark, config["manifest_table"])

    def run(self) -> None:
        """Execute the full SFTP → Bronze pipeline."""
        file_patterns = {fc["pattern"]: fc for fc in self._cfg["files"]}

        with SFTPClient(self._cfg) as sftp:
            remote_files = sftp.list_files()

            with tempfile.TemporaryDirectory() as tmpdir:
                for file_meta in remote_files:
                    matched_cfg = self._match_file(file_meta["name"], file_patterns)
                    if matched_cfg is None:
                        continue

                    local_path = os.path.join(tmpdir, file_meta["name"])
                    sftp.download(file_meta["remote_path"], local_path)

                    checksum = md5_checksum(local_path)
                    if self._manifest.is_processed(file_meta, checksum):
                        continue

                    landing_path = self._upload_to_landing(local_path, file_meta["name"])

                    sdf = excel_to_spark(
                        spark=self._spark,
                        local_path=local_path,
                        sheet_name=matched_cfg.get("sheet_name", 0),
                        header_row=matched_cfg.get("header_row", 0),
                        source_file=file_meta["name"],
                        batch_id=self._batch_id,
                        ingest_time=self._ingest_time,
                    )

                    target_table = matched_cfg["target_table"]
                    sdf.write.format("delta").mode("append").saveAsTable(target_table)

                    self._manifest.record(
                        file_meta=file_meta,
                        checksum=checksum,
                        landing_path=landing_path,
                        batch_id=self._batch_id,
                        status="success",
                    )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _match_file(
        filename: str, patterns: dict[str, dict]
    ) -> dict | None:
        for pattern, cfg in patterns.items():
            if fnmatch.fnmatch(filename, pattern):
                return cfg
        return None

    def _upload_to_landing(self, local_path: str, filename: str) -> str:
        """Copy local file to ADLS Gen2 landing zone using dbutils.fs."""
        landing_root = self._cfg["landing_path"].rstrip("/")
        dest = f"{landing_root}/{filename}"
        try:
            from pyspark.dbutils import DBUtils  # type: ignore[import]

            dbutils = DBUtils(self._spark)
            dbutils.fs.cp(f"file://{local_path}", dest)
        except ImportError:
            # Local / test fallback – just record the local path
            dest = local_path
        return dest
