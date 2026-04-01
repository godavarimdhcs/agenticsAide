"""
DLT pipeline entry point referenced by databricks.yml.

This file is imported by Databricks when a DLT pipeline runs; it simply
re-exports all @dlt.table definitions from the transform module.
"""
from src.transforms.silver.dlt_bronze_to_silver import *  # noqa: F401, F403
