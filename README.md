# agenticsAide – Azure Databricks Lakehouse Platform

A production-ready, end-to-end Azure Databricks lakehouse scaffold built on:
- **Azure Data Lake Storage Gen2** (ADLS Gen2) for storage
- **Delta Lake** as the open table format
- **Unity Catalog** for governance and fine-grained access control
- **Databricks Workflows** for orchestration
- **Databricks Asset Bundles (DABS)** for deployment
- **GitHub Actions** for CI/CD
- **Collibra DGC** for metadata governance and lineage

---

## Architecture

### Medallion Layers

| Layer  | Catalog schema              | Storage path                                      | Description                          |
|--------|-----------------------------|---------------------------------------------------|--------------------------------------|
| Bronze | `<env>_lakehouse.bronze`    | `abfss://bronze@<storage>.dfs.core.windows.net/`  | Raw, immutable copy of source data   |
| Silver | `<env>_lakehouse.silver`    | `abfss://silver@<storage>.dfs.core.windows.net/`  | Cleansed, validated, deduplicated    |
| Gold   | `<env>_lakehouse.gold`      | `abfss://gold@<storage>.dfs.core.windows.net/`    | Business aggregates, reporting-ready |

### Unity Catalog naming

```
<env>_lakehouse             ← catalog
  ├── bronze                ← schema
  │   ├── crm_customers     ← Delta table
  │   ├── crm_orders
  │   ├── finance_reports
  │   └── metadata_watermarks
  ├── silver
  │   ├── crm_customers
  │   ├── crm_orders
  │   └── finance_reports
  └── gold
      ├── daily_sales_summary
      └── monthly_finance_summary
```

### Data Flow

```
PostgreSQL ──(JDBC incremental)──▶ Bronze Delta
SFTP/Excel ──(manifest+ADLS)────▶ Bronze Delta
                                     │
                                     ▼ DLT (expectations)
                                  Silver Delta
                                     │
                                     ▼ Spark MERGE
                                   Gold Delta
                                     │
                                     ▼
                              Collibra DGC
```

---

## Repository Structure

```
agenticsAide/
├── .github/workflows/      # GitHub Actions CI/CD
│   ├── pr_validation.yml   # Lint + tests + bundle validate on PR
│   └── deploy.yml          # Deploy to dev/qa/prod
├── src/
│   ├── ingestion/
│   │   ├── postgres/       # JDBC incremental watermark ingestion
│   │   └── sftp_excel/     # SFTP download + manifest + Excel parse
│   ├── transforms/
│   │   ├── silver/         # DLT Bronze→Silver pipeline
│   │   └── gold/           # Silver→Gold Spark MERGE jobs
│   ├── collibra/           # Collibra metadata + lineage publisher
│   └── utils/              # Config loader, secrets helper
├── pipelines/              # DLT pipeline entry point
├── jobs/                   # Databricks Workflow task entry points
├── conf/
│   ├── dev/                # Dev YAML configs
│   ├── qa/                 # QA YAML configs
│   └── prod/               # Prod YAML configs
├── infra/
│   ├── unity_catalog/      # Unity Catalog grant SQL scripts
│   ├── cluster_policies/   # Cluster policy JSON templates
│   └── secret_scopes.md    # Secret scope naming guide
├── docs/                   # Architecture + runbooks
├── tests/                  # Unit tests
├── databricks.yml          # DABS bundle definition
├── pyproject.toml          # Python packaging
├── requirements.txt        # Runtime dependencies
└── requirements-dev.txt    # Dev/CI dependencies
```

---

## Setup

### Prerequisites

- Python 3.10+
- Databricks CLI v0.200+ (`databricks --version`)
- Azure subscription with ADLS Gen2 storage account
- Databricks workspace with Unity Catalog enabled

### Local development

```bash
# 1. Clone
git clone https://github.com/godavarimdhcs/agenticsAide.git
cd agenticsAide

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements-dev.txt

# 4. Set secrets as env vars (for local testing)
export DEV_POSTGRES__USERNAME=myuser
export DEV_POSTGRES__PASSWORD=mypassword
export DEV_SFTP__PASSWORD=sftppass

# 5. Run tests
pytest tests/ -v
```

### Running jobs locally

```bash
# Postgres ingestion (uses local config)
python jobs/postgres_ingestion_job.py --env dev

# SFTP Excel ingestion
python jobs/sftp_excel_ingestion_job.py --env dev

# Silver → Gold
python jobs/silver_to_gold_job.py --env dev

# Collibra publish
python jobs/collibra_publish_job.py --env dev
```

---

## Deploy with DABS

### Configure workspace credentials

```bash
databricks configure --token
# or use service principal:
export DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...
```

### Deploy

```bash
# Deploy to dev (default target)
databricks bundle deploy --target dev

# Deploy to QA (requires QA workspace creds)
databricks bundle deploy --target qa

# Deploy to prod (requires prod workspace creds)
databricks bundle deploy --target prod
```

### Run a job manually

```bash
databricks bundle run --target dev postgres_ingestion_job
databricks bundle run --target dev full_pipeline_job
```

---

## Secret Scopes

See [`infra/secret_scopes.md`](infra/secret_scopes.md) for full conventions.

Quick start:
```bash
databricks secrets create-scope dev-postgres
databricks secrets put-secret dev-postgres username --string-value "dbuser"
databricks secrets put-secret dev-postgres password --string-value "dbpass"
```

---

## Unity Catalog Setup

1. Create catalogs: `dev_lakehouse`, `qa_lakehouse`, `prod_lakehouse`
2. Create schemas: `bronze`, `silver`, `gold` inside each catalog
3. Apply grants from `infra/unity_catalog/grants.sql`
4. Configure external locations for ADLS Gen2 containers

---

## CI/CD

| Trigger                        | Workflow            | Action                              |
|--------------------------------|---------------------|-------------------------------------|
| PR to `main`                   | `pr_validation.yml` | Lint + tests + `bundle validate`    |
| Push to `main`                 | `deploy.yml`        | Auto-deploy to DEV                  |
| Manual dispatch (target=qa)    | `deploy.yml`        | Deploy to QA (requires env approval)|
| Manual dispatch (target=prod)  | `deploy.yml`        | Deploy to PROD (requires approval)  |

### Required GitHub Secrets

| Secret                   | Description                     |
|--------------------------|---------------------------------|
| `DATABRICKS_DEV_HOST`    | DEV workspace URL               |
| `DATABRICKS_DEV_TOKEN`   | DEV service principal token     |
| `DATABRICKS_QA_HOST`     | QA workspace URL                |
| `DATABRICKS_QA_TOKEN`    | QA service principal token      |
| `DATABRICKS_PROD_HOST`   | PROD workspace URL              |
| `DATABRICKS_PROD_TOKEN`  | PROD service principal token    |

---

## Collibra Integration

See [`docs/collibra_setup.md`](docs/collibra_setup.md) for full setup.

Quick config:
```bash
export COLLIBRA_BASE_URL=https://acme.collibra.com
export COLLIBRA_USERNAME=service-account
export COLLIBRA_PASSWORD=token
```

Run the Collibra publish job:
```bash
databricks bundle run --target prod collibra_publish_job
```

---

## Adding a new source

1. Add a config file under `conf/<env>/` (copy from existing example)
2. Create a job entry point in `jobs/` (copy `postgres_ingestion_job.py`)
3. Add the new job task to `databricks.yml` under `resources.jobs`
4. Add DLT expectations for the new table in `src/transforms/silver/dlt_bronze_to_silver.py`
5. Add Gold transform in `src/transforms/gold/gold_transforms.py`
6. Run `pytest tests/` to ensure nothing is broken
