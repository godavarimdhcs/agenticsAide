"""
Excel → Spark DataFrame parser.

Uses pandas + openpyxl to read a local Excel file, then converts the
result into a Spark DataFrame.  Supports multi-sheet configs.

Config keys per file entry (from sftp_config.yml):
    sheet_name:   Sheet1          (default: first sheet)
    header_row:   0               (0-based row index, default 0)
    skip_rows:    []              (list of row indices to skip after header)
    dtype_map:    {}              (optional pandas dtype hints)
"""
from __future__ import annotations

from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def excel_to_spark(
    spark: SparkSession,
    local_path: str,
    sheet_name: str | int = 0,
    header_row: int = 0,
    dtype_map: dict[str, Any] | None = None,
    source_file: str = "",
    batch_id: str = "",
    ingest_time: str = "",
) -> DataFrame:
    """Read an Excel file and return a Spark DataFrame with metadata columns.

    Args:
        spark:       Active SparkSession.
        local_path:  Path to the local .xlsx / .xls file.
        sheet_name:  Sheet name or 0-based index (default: first sheet).
        header_row:  0-based row index of the header row (default: 0).
        dtype_map:   Optional dict mapping column names to pandas dtypes.
        source_file: Original file name (added as ``_source_file`` column).
        batch_id:    Batch identifier (added as ``_batch_id`` column).
        ingest_time: ISO timestamp string (added as ``_ingest_time`` column).

    Returns:
        Spark DataFrame with all columns from the Excel sheet plus
        metadata columns.
    """
    pdf = pd.read_excel(
        local_path,
        sheet_name=sheet_name,
        header=header_row,
        dtype=dtype_map or {},
        engine="openpyxl",
    )

    # Normalise column names: strip whitespace, replace spaces with underscores
    pdf.columns = [str(c).strip().replace(" ", "_").lower() for c in pdf.columns]

    # Convert all-NaN rows (often trailing blank rows in Excel) to empty string
    pdf = pdf.dropna(how="all")

    sdf: DataFrame = spark.createDataFrame(pdf.astype(str))

    if source_file:
        sdf = sdf.withColumn("_source_file", F.lit(source_file))
    if batch_id:
        sdf = sdf.withColumn("_batch_id", F.lit(batch_id))
    if ingest_time:
        sdf = sdf.withColumn(
            "_ingest_time", F.lit(ingest_time).cast("timestamp")
        )

    return sdf
