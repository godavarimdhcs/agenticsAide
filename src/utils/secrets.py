"""
Databricks secret scope helper.

All credential lookups must go through this module – never hard-code
secrets in pipeline code.

Secret scope naming convention:
    <env>-<source-system>    e.g.  dev-postgres, prod-sftp

Usage:
    from src.utils.secrets import get_secret
    password = get_secret("dev-postgres", "password")
"""
from __future__ import annotations

import os


def get_secret(scope: str, key: str) -> str:
    """Retrieve a secret from a Databricks secret scope.

    When running locally (outside a Databricks cluster) the value is read
    from an environment variable named ``<SCOPE>__<KEY>`` (upper-cased,
    hyphens replaced by underscores).  This allows local development and CI
    testing without a live Databricks workspace.

    Args:
        scope: Databricks secret scope name.
        key:   Key within the scope.

    Returns:
        Secret value as a plain string.

    Raises:
        ValueError: If the secret cannot be resolved in the current context.
    """
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import]
        from pyspark.sql import SparkSession  # type: ignore[import]

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active SparkSession")
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope=scope, key=key)
    except (ImportError, RuntimeError):
        # Fall back to environment variables for local / CI usage
        env_var = f"{scope}__{key}".upper().replace("-", "_")
        value = os.environ.get(env_var)
        if value is None:
            raise ValueError(
                f"Secret '{scope}/{key}' not found. "
                f"Set env-var '{env_var}' for local testing."
            )
        return value
