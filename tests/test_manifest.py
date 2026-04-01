"""
Unit tests for src.ingestion.sftp_excel.manifest module.

Uses an in-memory DuckDB-backed Spark lookalike (via unittest.mock) so the
tests run without a live Spark cluster.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Stub pyspark.sql.functions before importing the module under test
# ---------------------------------------------------------------------------

def _make_pyspark_stub() -> None:
    """Insert minimal pyspark stubs into sys.modules if pyspark is absent."""
    if "pyspark" in sys.modules:
        return
    pyspark = ModuleType("pyspark")
    pyspark_sql = ModuleType("pyspark.sql")
    pyspark_sql_functions = ModuleType("pyspark.sql.functions")

    # col() returns a MagicMock so that column expressions evaluate to
    # MagicMock objects that support & and ==
    def _col(name: str) -> MagicMock:
        m = MagicMock()
        m.__eq__ = lambda self, other: MagicMock()
        m.__and__ = lambda self, other: MagicMock()
        return m

    pyspark_sql_functions.col = _col  # type: ignore[attr-defined]
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.functions"] = pyspark_sql_functions


_make_pyspark_stub()

from src.ingestion.sftp_excel.manifest import FileManifest, md5_checksum  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _make_file_meta(
    name: str = "report_2024.xlsx",
    size: int = 1024,
    mtime: int = 1700000000,
    remote_path: str = "/outbound/report_2024.xlsx",
) -> dict:
    return {
        "name": name,
        "size": size,
        "mtime": mtime,
        "remote_path": remote_path,
    }


def _make_mock_spark() -> MagicMock:
    """Return a mock SparkSession that records SQL calls."""
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    return spark


# ---------------------------------------------------------------------------
# Tests: md5_checksum
# ---------------------------------------------------------------------------

class TestMd5Checksum:
    def test_known_content(self, tmp_path: Path) -> None:
        p = tmp_path / "test.bin"
        p.write_bytes(b"hello world")
        expected = hashlib.md5(b"hello world").hexdigest()
        assert md5_checksum(str(p)) == expected

    def test_empty_file(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.bin"
        p.write_bytes(b"")
        expected = hashlib.md5(b"").hexdigest()
        assert md5_checksum(str(p)) == expected

    def test_large_file_chunked(self, tmp_path: Path) -> None:
        content = b"x" * (3 * 1024 * 1024)  # 3 MB
        p = tmp_path / "large.bin"
        p.write_bytes(content)
        expected = hashlib.md5(content).hexdigest()
        assert md5_checksum(str(p)) == expected


# ---------------------------------------------------------------------------
# Tests: FileManifest.is_processed
# ---------------------------------------------------------------------------

class TestFileManifestIsProcessed:
    def _make_manifest(self, rows: list[dict]) -> FileManifest:
        """Build a FileManifest with a mocked Spark that returns *rows* on table scan."""
        spark = _make_mock_spark()

        # Mock the chained call: spark.table(...).filter(...).limit(1).collect()
        mock_collect = MagicMock(return_value=rows)
        spark.table.return_value.filter.return_value.limit.return_value.collect = (
            mock_collect
        )
        manifest = FileManifest.__new__(FileManifest)
        manifest._spark = spark
        manifest._table = "bronze.metadata.sftp_manifest"
        return manifest

    def test_returns_false_when_not_in_manifest(self) -> None:
        manifest = self._make_manifest(rows=[])
        fm = _make_file_meta()
        assert manifest.is_processed(fm, "abc123") is False

    def test_returns_true_when_in_manifest(self) -> None:
        manifest = self._make_manifest(rows=[{"status": "success"}])
        fm = _make_file_meta()
        assert manifest.is_processed(fm, "abc123") is True


# ---------------------------------------------------------------------------
# Tests: FileManifest.record
# ---------------------------------------------------------------------------

class TestFileManifestRecord:
    def test_calls_write(self) -> None:
        spark = _make_mock_spark()
        mock_df = MagicMock()
        spark.createDataFrame.return_value = mock_df

        manifest = FileManifest.__new__(FileManifest)
        manifest._spark = spark
        manifest._table = "bronze.metadata.sftp_manifest"

        fm = _make_file_meta()
        manifest.record(
            file_meta=fm,
            checksum="deadbeef",
            landing_path="abfss://landing@storage.dfs.core.windows.net/file.xlsx",
            batch_id="run-001",
            status="success",
        )

        # Verify createDataFrame was called with a list of 1 dict
        spark.createDataFrame.assert_called_once()
        args = spark.createDataFrame.call_args[0]
        assert len(args[0]) == 1
        row = args[0][0]
        assert row["file_name"] == fm["name"]
        assert row["checksum_md5"] == "deadbeef"
        assert row["status"] == "success"

        # Verify write.format("delta").mode("append").saveAsTable was invoked
        mock_df.write.format.assert_called_once_with("delta")
        mock_df.write.format.return_value.mode.assert_called_once_with("append")


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _make_file_meta(
    name: str = "report_2024.xlsx",
    size: int = 1024,
    mtime: int = 1700000000,
    remote_path: str = "/outbound/report_2024.xlsx",
) -> dict:
    return {
        "name": name,
        "size": size,
        "mtime": mtime,
        "remote_path": remote_path,
    }


def _make_mock_spark() -> MagicMock:
    """Return a mock SparkSession that records SQL calls."""
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    return spark


# ---------------------------------------------------------------------------
# Tests: md5_checksum
# ---------------------------------------------------------------------------

class TestMd5Checksum:
    def test_known_content(self, tmp_path: Path) -> None:
        p = tmp_path / "test.bin"
        p.write_bytes(b"hello world")
        expected = hashlib.md5(b"hello world").hexdigest()
        assert md5_checksum(str(p)) == expected

    def test_empty_file(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.bin"
        p.write_bytes(b"")
        expected = hashlib.md5(b"").hexdigest()
        assert md5_checksum(str(p)) == expected

    def test_large_file_chunked(self, tmp_path: Path) -> None:
        content = b"x" * (3 * 1024 * 1024)  # 3 MB
        p = tmp_path / "large.bin"
        p.write_bytes(content)
        expected = hashlib.md5(content).hexdigest()
        assert md5_checksum(str(p)) == expected


# ---------------------------------------------------------------------------
# Tests: FileManifest.is_processed
# ---------------------------------------------------------------------------

class TestFileManifestIsProcessed:
    def _make_manifest(self, rows: list[dict]) -> FileManifest:
        """Build a FileManifest with a mocked Spark that returns *rows* on table scan."""
        spark = _make_mock_spark()

        # Mock the chained call: spark.table(...).filter(...).limit(1).collect()
        mock_collect = MagicMock(return_value=rows)
        spark.table.return_value.filter.return_value.limit.return_value.collect = (
            mock_collect
        )
        manifest = FileManifest.__new__(FileManifest)
        manifest._spark = spark
        manifest._table = "bronze.metadata.sftp_manifest"
        return manifest

    def test_returns_false_when_not_in_manifest(self) -> None:
        manifest = self._make_manifest(rows=[])
        fm = _make_file_meta()
        assert manifest.is_processed(fm, "abc123") is False

    def test_returns_true_when_in_manifest(self) -> None:
        manifest = self._make_manifest(rows=[{"status": "success"}])
        fm = _make_file_meta()
        assert manifest.is_processed(fm, "abc123") is True


# ---------------------------------------------------------------------------
# Tests: FileManifest.record
# ---------------------------------------------------------------------------

class TestFileManifestRecord:
    def test_calls_write(self) -> None:
        spark = _make_mock_spark()
        mock_df = MagicMock()
        spark.createDataFrame.return_value = mock_df

        manifest = FileManifest.__new__(FileManifest)
        manifest._spark = spark
        manifest._table = "bronze.metadata.sftp_manifest"

        fm = _make_file_meta()
        manifest.record(
            file_meta=fm,
            checksum="deadbeef",
            landing_path="abfss://landing@storage.dfs.core.windows.net/file.xlsx",
            batch_id="run-001",
            status="success",
        )

        # Verify createDataFrame was called with a list of 1 dict
        spark.createDataFrame.assert_called_once()
        args = spark.createDataFrame.call_args[0]
        assert len(args[0]) == 1
        row = args[0][0]
        assert row["file_name"] == fm["name"]
        assert row["checksum_md5"] == "deadbeef"
        assert row["status"] == "success"

        # Verify write.format("delta").mode("append").saveAsTable was invoked
        mock_df.write.format.assert_called_once_with("delta")
        mock_df.write.format.return_value.mode.assert_called_once_with("append")
