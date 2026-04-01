# Operations Runbook

## Daily Health Checks

1. Open Databricks Workflows UI → verify last run of `full_pipeline_job` succeeded.
2. Check DLT pipeline expectations dashboard for quality metric drift.
3. Verify Collibra publish job completed (lineage up-to-date).

---

## Troubleshooting

### Postgres ingestion fails with "Connection refused"

**Symptoms:** Job fails in `step_1_postgres` with JDBC connection error.

**Steps:**
1. Verify VNet peering / private endpoint between Databricks and PostgreSQL.
2. Check secret scope: `databricks secrets get-secret <env>-postgres username`.
3. Test JDBC URL from a notebook:
   ```python
   spark.read.jdbc(
       url="jdbc:postgresql://<host>:5432/<db>",
       table="(SELECT 1) AS t",
       properties={"user": "...", "password": "..."}
   ).show()
   ```

### SFTP ingestion fails with "Authentication failed"

**Symptoms:** `paramiko.ssh_exception.AuthenticationException`

**Steps:**
1. Verify secret scope credentials: `databricks secrets list-secrets <env>-sftp`.
2. Test SFTP connectivity from a cluster terminal:
   ```bash
   sftp -P 22 <user>@<host>
   ```
3. Check if key-based auth is required and configure `paramiko` accordingly.

### DLT expectations dropping too many rows

**Symptoms:** Silver table row count much lower than Bronze.

**Steps:**
1. Open DLT pipeline → Expectations tab → identify failing rule.
2. Query Bronze table to understand data distribution:
   ```sql
   SELECT COUNT(*) FROM dev_lakehouse.bronze.crm_customers WHERE customer_id IS NULL;
   ```
3. Adjust expectation from `expect_or_drop` to `expect` if appropriate, or fix upstream data.

### Watermark not advancing

**Symptoms:** Postgres ingestion reprocesses same rows every run.

**Steps:**
1. Check watermark table:
   ```sql
   SELECT * FROM dev_lakehouse.bronze.metadata_watermarks;
   ```
2. Verify `updated_at` column is properly indexed on the source PostgreSQL table.
3. Check that the watermark value written matches the actual max in Bronze:
   ```sql
   SELECT MAX(updated_at) FROM dev_lakehouse.bronze.crm_customers;
   ```

### Duplicate files in Bronze (SFTP)

**Symptoms:** Bronze table has duplicate rows from the same Excel file.

**Steps:**
1. Check manifest table:
   ```sql
   SELECT * FROM dev_lakehouse.bronze.metadata_sftp_manifest
   WHERE file_name = 'report_2024.xlsx' ORDER BY processed_at DESC;
   ```
2. If `status = 'failed'`, the file will be re-processed on next run (expected).
3. If duplicate `status = 'success'` entries exist, delete duplicates from Bronze and manifest, then re-run.

---

## Re-running a Failed Job

```bash
# Re-run a specific Databricks Workflow job run
databricks jobs run-now --job-id <job_id>

# Or trigger via DABS
databricks bundle run --target dev postgres_ingestion_job
```

---

## Rolling Back a Bad Deploy

```bash
# List recent bundle deploys
databricks bundle status --target prod

# Re-deploy a previous git commit
git checkout <previous-sha>
databricks bundle deploy --target prod
```

---

## Monitoring Alerts

Set up Databricks SQL alerts on:
- `SELECT COUNT(*) FROM dev_lakehouse.bronze.metadata_watermarks WHERE updated_at < CURRENT_TIMESTAMP - INTERVAL 2 HOURS` (stale watermark)
- DLT pipeline failure notifications (built-in Databricks alert)
- SFTP manifest `status = 'failed'` rows
