"""
Unit tests for src.utils.config module.
"""
import os
import textwrap
from pathlib import Path

import pytest
import yaml

from src.utils.config import get_env, load_config, load_env_config


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_config(tmp_path: Path) -> Path:
    """Write a minimal YAML config to a temp file and return the path."""
    data = {
        "source_system": "crm",
        "jdbc_host": "localhost",
        "jdbc_port": 5432,
        "jdbc_database": "test_db",
        "watermark_table": "bronze.metadata.watermarks",
        "secret_scope": "test-scope",
        "tables": [
            {
                "schema": "public",
                "table": "customers",
                "watermark_col": "updated_at",
                "target_table": "bronze.crm.customers",
            }
        ],
    }
    p = tmp_path / "postgres_config.yml"
    p.write_text(yaml.dump(data))
    return p


@pytest.fixture()
def tmp_env_config(tmp_path: Path) -> tuple[Path, str]:
    """Create conf/<env>/sftp_config.yml structure and return (base_dir, env)."""
    env = "dev"
    data = {
        "source_system": "finance",
        "sftp_host": "sftp.example.com",
        "sftp_port": 22,
        "sftp_remote_dir": "/outbound",
        "landing_path": "abfss://landing@storage.dfs.core.windows.net/sftp/",
        "manifest_table": "bronze.metadata.manifest",
        "secret_scope": "dev-sftp",
        "files": [{"pattern": "*.xlsx", "target_table": "bronze.finance.reports"}],
    }
    conf_dir = tmp_path / "conf" / env
    conf_dir.mkdir(parents=True)
    (conf_dir / "sftp_config.yml").write_text(yaml.dump(data))
    return tmp_path / "conf", env


# ---------------------------------------------------------------------------
# Tests: load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_loads_valid_yaml(self, tmp_config: Path) -> None:
        cfg = load_config(tmp_config)
        assert cfg["source_system"] == "crm"
        assert cfg["jdbc_port"] == 5432

    def test_loads_tables_list(self, tmp_config: Path) -> None:
        cfg = load_config(tmp_config)
        assert isinstance(cfg["tables"], list)
        assert cfg["tables"][0]["table"] == "customers"

    def test_raises_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nonexistent.yml")

    def test_empty_yaml_returns_empty_dict(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.yml"
        p.write_text("")
        cfg = load_config(p)
        assert cfg == {}

    def test_accepts_string_path(self, tmp_config: Path) -> None:
        cfg = load_config(str(tmp_config))
        assert "source_system" in cfg


# ---------------------------------------------------------------------------
# Tests: load_env_config
# ---------------------------------------------------------------------------

class TestLoadEnvConfig:
    def test_loads_sftp_config(self, tmp_env_config: tuple[Path, str]) -> None:
        base_dir, env = tmp_env_config
        cfg = load_env_config(base_dir, env, "sftp_config.yml")
        assert cfg["source_system"] == "finance"
        assert cfg["sftp_port"] == 22

    def test_raises_for_missing_env(self, tmp_env_config: tuple[Path, str]) -> None:
        base_dir, _ = tmp_env_config
        with pytest.raises(FileNotFoundError):
            load_env_config(base_dir, "prod", "sftp_config.yml")


# ---------------------------------------------------------------------------
# Tests: get_env
# ---------------------------------------------------------------------------

class TestGetEnv:
    def test_default_is_dev(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DATABRICKS_ENV", raising=False)
        assert get_env() == "dev"

    def test_reads_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABRICKS_ENV", "PROD")
        assert get_env() == "prod"

    def test_custom_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DATABRICKS_ENV", raising=False)
        assert get_env(default="qa") == "qa"
