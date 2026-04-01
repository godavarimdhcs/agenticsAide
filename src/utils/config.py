"""
Config loader utility.

Reads YAML configuration files and resolves environment-specific overrides.
Usage:
    cfg = load_config("conf/dev/postgres_config.yml")
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def load_config(config_path: str | Path) -> dict[str, Any]:
    """Load a YAML config file and return as a dictionary.

    Args:
        config_path: Absolute or relative path to the YAML file.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.YAMLError: If the file cannot be parsed.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data or {}


def load_env_config(base_dir: str | Path, env: str, filename: str) -> dict[str, Any]:
    """Convenience wrapper that resolves <base_dir>/<env>/<filename>.

    Args:
        base_dir: Root conf/ directory.
        env: Environment name (dev | qa | prod).
        filename: Config file name, e.g. 'postgres_config.yml'.

    Returns:
        Parsed configuration dictionary.
    """
    config_path = Path(base_dir) / env / filename
    return load_config(config_path)


def get_env(default: str = "dev") -> str:
    """Return the current environment from the DATABRICKS_ENV env-var."""
    return os.environ.get("DATABRICKS_ENV", default).lower()
