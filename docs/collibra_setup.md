# Collibra DGC Integration Setup

## Overview

The `src/collibra/` module publishes:
1. **Data dictionary** – table and column metadata from Unity Catalog `information_schema`
2. **Lineage edges** – source system → Bronze → Silver → Gold

Publishing is implemented as a lightweight REST push (no Collibra SDK dependency)
and is designed to be non-blocking: failures log a warning but do not halt data pipelines.

---

## Configuration

### Environment variables (or Databricks secrets)

| Variable              | Description                                    | Example                              |
|-----------------------|------------------------------------------------|--------------------------------------|
| `COLLIBRA_BASE_URL`   | Collibra REST API base URL                     | `https://acme.collibra.com`          |
| `COLLIBRA_USERNAME`   | Collibra service account username              | `svc-databricks`                     |
| `COLLIBRA_PASSWORD`   | Service account password or API token          | `my-api-token`                       |

### Setting via Databricks secret scope (recommended for prod)

```bash
databricks secrets create-scope prod-collibra
databricks secrets put-secret prod-collibra base_url  --string-value "https://acme.collibra.com"
databricks secrets put-secret prod-collibra username  --string-value "svc-databricks"
databricks secrets put-secret prod-collibra password  --string-value "<token>"
```

Then wire in `jobs/collibra_publish_job.py`:
```python
from src.utils.secrets import get_secret

collibra = CollibraPublisher(
    base_url=get_secret("prod-collibra", "base_url"),
    username=get_secret("prod-collibra", "username"),
    password=get_secret("prod-collibra", "password"),
)
```

---

## Collibra REST API Endpoints Used

| Operation            | Method | Path                              |
|----------------------|--------|-----------------------------------|
| Create/update asset  | POST   | `/rest/2.0/assets`                |
| Add attribute        | POST   | `/rest/2.0/attributes`            |
| Publish lineage      | POST   | `/rest/2.0/dataSourceLineages`    |

> **Note:** Endpoint paths are placeholders for Collibra's standard REST API.
> Adjust the paths in `src/collibra/publisher.py` to match your Collibra version
> and domain configuration.

---

## Running the Publish Job

```bash
# Manual trigger via DABS
databricks bundle run --target prod collibra_publish_job

# Direct Python (local)
export COLLIBRA_BASE_URL=https://acme.collibra.com
export COLLIBRA_USERNAME=svc-databricks
export COLLIBRA_PASSWORD=token
python jobs/collibra_publish_job.py --env prod --catalog prod_lakehouse
```

---

## Customising Lineage

By default, `CollibraLineageBulkPublisher.publish_all()` publishes lineage for all tables in the catalog.  To publish lineage for a specific table only:

```python
from src.collibra.publisher import CollibraPublisher

pub = CollibraPublisher()
pub.publish_medallion_lineage(
    source_system="crm",
    catalog="prod_lakehouse",
    table="crm_customers",
)
```

---

## Extending Asset Types

The current implementation maps all Unity Catalog tables to Collibra's `Table` and `Column` asset types.  To map to custom Collibra asset types (e.g., `Data Set`, `Business Term`):

1. Update `CollibraPublisher._upsert_asset()` to pass the correct `typeName`.
2. Add domain / community IDs to the payload as required by your Collibra configuration.
3. Update `CollibraPublisher.publish_table()` to include additional attribute mappings.
