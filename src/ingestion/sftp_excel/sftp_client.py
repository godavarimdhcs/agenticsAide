"""
SFTP client wrapper.

Downloads files from an SFTP server to a local staging path, then
uploads them to an ADLS Gen2 landing zone via the Databricks File System
(dbutils.fs).

Config keys relevant to this module (from sftp_config.yml):
    sftp_host
    sftp_port     (default 22)
    sftp_username
    sftp_remote_dir
    landing_path  abfss://landing@<storage>.dfs.core.windows.net/sftp/<env>/
    secret_scope  e.g. dev-sftp
"""
from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path
from typing import Any

import paramiko  # type: ignore[import]

from src.utils.secrets import get_secret


class SFTPClient:
    """Thin wrapper around paramiko SFTP for file listing and download."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._cfg = config
        self._scope = config["secret_scope"]
        self._client: paramiko.SFTPClient | None = None
        self._transport: paramiko.Transport | None = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "SFTPClient":
        self._connect()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_files(self, remote_dir: str | None = None) -> list[dict[str, Any]]:
        """Return metadata for all regular files in *remote_dir*.

        Returns:
            List of dicts with keys: name, size, mtime, remote_path.
        """
        assert self._client is not None, "Not connected"
        rdir = remote_dir or self._cfg["sftp_remote_dir"]
        entries = []
        for attr in self._client.listdir_attr(rdir):
            if stat.S_ISREG(attr.st_mode or 0):
                entries.append(
                    {
                        "name": attr.filename,
                        "size": attr.st_size or 0,
                        "mtime": attr.st_mtime or 0,
                        "remote_path": f"{rdir.rstrip('/')}/{attr.filename}",
                    }
                )
        return entries

    def download(self, remote_path: str, local_path: str) -> None:
        """Download *remote_path* to *local_path*."""
        assert self._client is not None, "Not connected"
        os.makedirs(Path(local_path).parent, exist_ok=True)
        self._client.get(remote_path, local_path)

    def close(self) -> None:
        if self._client:
            self._client.close()
        if self._transport:
            self._transport.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        host = self._cfg["sftp_host"]
        port = int(self._cfg.get("sftp_port", 22))
        username = self._cfg.get("sftp_username") or get_secret(
            self._scope, "username"
        )
        password = get_secret(self._scope, "password")

        self._transport = paramiko.Transport((host, port))
        self._transport.connect(username=username, password=password)
        self._client = paramiko.SFTPClient.from_transport(self._transport)
