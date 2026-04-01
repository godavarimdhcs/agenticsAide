# Architecture Deep Dive

## Storage Layout (ADLS Gen2)

Each environment has its own storage account (or separate containers within one account):

```
<env>-storage.dfs.core.windows.net
  ├── landing/          ← Raw files dropped by SFTP / external systems
  │   └── sftp/<env>/   ← Downloaded Excel files before Bronze write
  ├── bronze/           ← Delta tables (external or managed)
  ├── silver/           ← Delta tables
  ├── gold/             ← Delta tables
  └── checkpoints/      ← Spark Streaming / DLT checkpoints
```

## Unity Catalog External Locations

```sql
CREATE EXTERNAL LOCATION dev_adls
  URL 'abfss://data@devstorage.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL dev_storage_credential);
```

## Ingestion Patterns

### Postgres Watermark

1. On first run: full table extract (no WHERE filter).
2. On subsequent runs: `WHERE <watermark_col> > '<last_watermark_value>'`.
3. Parallel JDBC read via `partitionColumn` / `numPartitions` for large tables.
4. Metadata columns appended: `_ingest_time`, `_batch_id`, `_watermark_value`, `_source_system`.
5. After successful write, new max watermark is upserted into `bronze.metadata.watermarks`.

### SFTP Excel Idempotency

1. List all files in remote SFTP directory.
2. Compute MD5 checksum of each downloaded file.
3. Check manifest table `bronze.metadata.sftp_manifest` for (file_name, checksum, size) match.
4. Skip if already recorded with `status = 'success'`.
5. On success, upload to ADLS landing zone and write Bronze Delta, then record manifest entry.

## DLT Pipeline (Bronze → Silver)

- Runs in triggered (non-continuous) mode.
- Uses `@dlt.expect_or_drop` for hard data quality rules.
- Uses `@dlt.expect` for soft monitoring rules.
- Delta Change Data Feed enabled on Silver tables for downstream CDC.

## Gold Transforms

- Pure Spark MERGE (upsert) jobs — no DLT dependency.
- Idempotent: re-running produces same result.
- Scheduled after DLT pipeline completes (Databricks Workflow dependency).

## Security Model

| Principal          | Bronze | Silver | Gold  | Metadata |
|--------------------|--------|--------|-------|----------|
| sp-ingest-*        | RW     | —      | —     | RW       |
| sp-transform-*     | R      | RW     | RW    | R        |
| data-analysts      | —      | R      | R     | —        |
| data-scientists    | —      | R      | R     | —        |
| data-engineers-*   | RW     | RW     | RW    | RW       |

## Data Classification

Tags applied via Unity Catalog `ALTER TABLE … ALTER COLUMN … SET TAGS`:

| Tag              | Values                    | Action                              |
|------------------|---------------------------|-------------------------------------|
| `classification` | `public`, `internal`, `sensitive`, `restricted` | Drives masking policy |
| `pii`            | `true` / `false`          | Triggers column-level masking       |
| `layer`          | `bronze`, `silver`, `gold`| Lineage and discovery               |
