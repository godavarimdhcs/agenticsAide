"""
Unit tests for src.ingestion.postgres.postgres_ingestor module.

Mocks SparkSession and WatermarkManager to avoid requiring a live Spark
cluster or database connection.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.postgres.postgres_ingestor import PostgresIngestor


# ---------------------------------------------------------------------------
# Sample config (matches conf/dev/postgres_config.yml structure)
# ---------------------------------------------------------------------------

SAMPLE_CONFIG = {
    "source_system": "crm",
    "jdbc_host": "localhost",
    "jdbc_port": 5432,
    "jdbc_database": "test_db",
    "fetchsize": 5000,
    "watermark_table": "bronze.metadata.watermarks",
    "secret_scope": "dev-postgres",
    "tables": [
        {
            "schema": "public",
            "table": "customers",
            "watermark_col": "updated_at",
            "target_table": "dev_lakehouse.bronze.crm_customers",
        }
    ],
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_spark() -> MagicMock:
    spark = MagicMock()
    # Simulate non-empty DataFrame
    spark.read.jdbc.return_value.rdd.isEmpty.return_value = False
    return spark


@pytest.fixture()
def mock_watermark_manager() -> MagicMock:
    wm = MagicMock()
    wm.get_watermark.return_value = "2024-01-01 00:00:00"
    return wm


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPostgresIngestorInit:
    def test_creates_batch_id(self, mock_spark: MagicMock) -> None:
        with (
            patch("src.ingestion.postgres.postgres_ingestor.WatermarkManager"),
            patch("src.ingestion.postgres.postgres_ingestor.get_secret", return_value="x"),
        ):
            ingestor = PostgresIngestor(spark=mock_spark, config=SAMPLE_CONFIG)
        assert ingestor._batch_id is not None
        assert len(ingestor._batch_id) == 36  # UUID format


class TestPostgresIngestorJdbcUrl:
    def test_jdbc_url_format(self, mock_spark: MagicMock) -> None:
        with (
            patch("src.ingestion.postgres.postgres_ingestor.WatermarkManager"),
            patch("src.ingestion.postgres.postgres_ingestor.get_secret", return_value="x"),
        ):
            ingestor = PostgresIngestor(spark=mock_spark, config=SAMPLE_CONFIG)
        url = ingestor._jdbc_url()
        assert url == "jdbc:postgresql://localhost:5432/test_db"

    def test_default_port_5432(self, mock_spark: MagicMock) -> None:
        cfg = {**SAMPLE_CONFIG}
        del cfg["jdbc_port"]  # should default to 5432
        with (
            patch("src.ingestion.postgres.postgres_ingestor.WatermarkManager"),
            patch("src.ingestion.postgres.postgres_ingestor.get_secret", return_value="x"),
        ):
            ingestor = PostgresIngestor(spark=mock_spark, config=cfg)
        assert ":5432/" in ingestor._jdbc_url()


class TestPostgresIngestorRun:
    def test_run_calls_read_jdbc(self, mock_spark: MagicMock) -> None:
        with (
            patch(
                "src.ingestion.postgres.postgres_ingestor.WatermarkManager"
            ) as MockWM,
            patch(
                "src.ingestion.postgres.postgres_ingestor.get_secret",
                return_value="secret",
            ),
        ):
            mock_wm_instance = MockWM.return_value
            mock_wm_instance.get_watermark.return_value = None  # full load

            # Mock the DataFrame
            mock_df = mock_spark.read.jdbc.return_value
            mock_df.rdd.isEmpty.return_value = False
            # createOrReplaceTempView is a void method
            mock_df.createOrReplaceTempView.return_value = None
            # spark.sql returns a new mock df
            mock_sql_df = MagicMock()
            mock_spark.sql.return_value = mock_sql_df
            mock_sql_df.selectExpr.return_value.collect.return_value = [
                {"max_wm": "2024-06-01"}
            ]
            mock_sql_df.write.format.return_value.mode.return_value.saveAsTable = (
                MagicMock()
            )

            ingestor = PostgresIngestor(spark=mock_spark, config=SAMPLE_CONFIG)
            ingestor.run()

        mock_spark.read.jdbc.assert_called_once()

    def test_run_skips_empty_dataframe(self, mock_spark: MagicMock) -> None:
        with (
            patch("src.ingestion.postgres.postgres_ingestor.WatermarkManager") as MockWM,
            patch(
                "src.ingestion.postgres.postgres_ingestor.get_secret",
                return_value="secret",
            ),
        ):
            mock_wm_instance = MockWM.return_value
            mock_wm_instance.get_watermark.return_value = None

            mock_spark.read.jdbc.return_value.rdd.isEmpty.return_value = True

            ingestor = PostgresIngestor(spark=mock_spark, config=SAMPLE_CONFIG)
            ingestor.run()

        # saveAsTable should NOT be called if DataFrame is empty
        mock_spark.sql.return_value.write.format.assert_not_called()

    def test_incremental_query_uses_watermark(self, mock_spark: MagicMock) -> None:
        with (
            patch("src.ingestion.postgres.postgres_ingestor.WatermarkManager") as MockWM,
            patch(
                "src.ingestion.postgres.postgres_ingestor.get_secret",
                return_value="secret",
            ),
        ):
            mock_wm_instance = MockWM.return_value
            mock_wm_instance.get_watermark.return_value = "2024-01-01 00:00:00"

            mock_df = mock_spark.read.jdbc.return_value
            mock_df.rdd.isEmpty.return_value = False
            mock_df.createOrReplaceTempView.return_value = None

            mock_sql_df = MagicMock()
            mock_spark.sql.return_value = mock_sql_df
            mock_sql_df.selectExpr.return_value.collect.return_value = [
                {"max_wm": "2024-06-01"}
            ]
            mock_sql_df.write.format.return_value.mode.return_value.saveAsTable = (
                MagicMock()
            )

            ingestor = PostgresIngestor(spark=mock_spark, config=SAMPLE_CONFIG)
            ingestor.run()

        call_kwargs = mock_spark.read.jdbc.call_args[1]
        assert "WHERE updated_at > '2024-01-01 00:00:00'" in call_kwargs["query"]
