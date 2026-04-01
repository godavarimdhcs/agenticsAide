"""
Collibra DGC metadata + lineage publisher.

Pushes Unity Catalog data dictionary (tables/columns) and lineage edges
to Collibra via its REST API.

Authentication is read from Databricks secret scope or environment variables:
    COLLIBRA_BASE_URL   – e.g. https://acme.collibra.com
    COLLIBRA_USERNAME   – service account
    COLLIBRA_PASSWORD   – service account password (or API token)

These can be wired via databricks.yml variable substitution or Databricks
secret scopes (recommended for production).

Endpoints used (placeholder paths – adjust to your Collibra instance version):
    POST /rest/2.0/assets                  – create/update asset
    POST /rest/2.0/attributes               – attach attributes
    POST /rest/2.0/dataSourceLineages       – publish lineage
"""
from __future__ import annotations

import os
from typing import Any

import requests


class CollibraPublisher:
    """REST-based Collibra metadata and lineage publisher."""

    def __init__(
        self,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: int = 30,
    ) -> None:
        self._base_url = (
            base_url or os.environ.get("COLLIBRA_BASE_URL", "https://placeholder.collibra.com")
        ).rstrip("/")
        self._username = username or os.environ.get("COLLIBRA_USERNAME", "")
        self._password = password or os.environ.get("COLLIBRA_PASSWORD", "")
        self._timeout = timeout
        self._session = requests.Session()
        self._session.auth = (self._username, self._password)
        self._session.headers.update({"Content-Type": "application/json"})

    # ------------------------------------------------------------------
    # Data dictionary
    # ------------------------------------------------------------------

    def publish_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        columns: list[dict[str, Any]],
        description: str = "",
    ) -> None:
        """Register a table and its columns in Collibra as Data Set assets.

        Args:
            catalog:     Unity Catalog catalog name.
            schema:      Schema name.
            table:       Table name.
            columns:     List of dicts with keys: name, data_type, nullable, comment.
            description: Optional table description.
        """
        fqn = f"{catalog}.{schema}.{table}"

        # Upsert the Table asset
        table_asset = self._upsert_asset(
            name=fqn,
            type_name="Table",
            attributes={"description": description, "qualifiedName": fqn},
        )
        table_id = table_asset.get("id")

        # Upsert each column
        for col in columns:
            col_fqn = f"{fqn}.{col['name']}"
            self._upsert_asset(
                name=col_fqn,
                type_name="Column",
                attributes={
                    "description": col.get("comment", ""),
                    "qualifiedName": col_fqn,
                    "dataType": col.get("data_type", ""),
                    "isNullable": str(col.get("nullable", True)),
                    "parentTableId": table_id or "",
                },
            )

    # ------------------------------------------------------------------
    # Lineage
    # ------------------------------------------------------------------

    def publish_lineage(
        self,
        source_fqn: str,
        target_fqn: str,
        transformation: str = "",
    ) -> None:
        """Publish a directional lineage edge source → target.

        Args:
            source_fqn:      Qualified name of the source asset.
            target_fqn:      Qualified name of the target asset.
            transformation:  Human-readable description of the transformation.
        """
        payload = {
            "sourceAssetQualifiedName": source_fqn,
            "targetAssetQualifiedName": target_fqn,
            "transformationDescription": transformation,
        }
        self._post("/rest/2.0/dataSourceLineages", payload)

    def publish_medallion_lineage(
        self,
        source_system: str,
        catalog: str,
        table: str,
    ) -> None:
        """Publish standard Bronze→Silver→Gold lineage for a table.

        Lineage edges created:
            <source_system>/<table> → <catalog>.bronze.<table>
            <catalog>.bronze.<table> → <catalog>.silver.<table>
            <catalog>.silver.<table> → <catalog>.gold.<table>
        """
        raw_src = f"{source_system}/{table}"
        bronze_fqn = f"{catalog}.bronze.{table}"
        silver_fqn = f"{catalog}.silver.{table}"
        gold_fqn = f"{catalog}.gold.{table}"

        self.publish_lineage(raw_src, bronze_fqn, "raw ingest")
        self.publish_lineage(bronze_fqn, silver_fqn, "DLT cleanse")
        self.publish_lineage(silver_fqn, gold_fqn, "Gold aggregate")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _upsert_asset(
        self, name: str, type_name: str, attributes: dict[str, Any]
    ) -> dict[str, Any]:
        payload = {"name": name, "typeName": type_name, "attributes": attributes}
        resp = self._post("/rest/2.0/assets", payload)
        return resp

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        try:
            resp = self._session.post(url, json=payload, timeout=self._timeout)
            resp.raise_for_status()
            return resp.json() if resp.content else {}
        except requests.exceptions.RequestException as exc:
            # Log and continue – Collibra publish failures must not block data pipelines
            print(f"[CollibraPublisher] WARNING: POST {url} failed: {exc}")
            return {}
