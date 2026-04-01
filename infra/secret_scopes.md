# Secret Scope Conventions

## Naming pattern

```
<env>-<source-system>
```

| Scope name        | Usage                           |
|-------------------|---------------------------------|
| `dev-postgres`    | PostgreSQL credentials (dev)    |
| `qa-postgres`     | PostgreSQL credentials (qa)     |
| `prod-postgres`   | PostgreSQL credentials (prod)   |
| `dev-sftp`        | SFTP credentials (dev)          |
| `qa-sftp`         | SFTP credentials (qa)           |
| `prod-sftp`       | SFTP credentials (prod)         |
| `dev-collibra`    | Collibra API credentials (dev)  |
| `prod-collibra`   | Collibra API credentials (prod) |

## Creating a scope (Databricks CLI)

```bash
databricks secrets create-scope <scope-name>
```

## Adding keys

```bash
# Postgres
databricks secrets put-secret dev-postgres username --string-value "<user>"
databricks secrets put-secret dev-postgres password --string-value "<password>"

# SFTP
databricks secrets put-secret dev-sftp username --string-value "<sftp-user>"
databricks secrets put-secret dev-sftp password --string-value "<sftp-pass>"

# Collibra
databricks secrets put-secret prod-collibra base_url  --string-value "https://acme.collibra.com"
databricks secrets put-secret prod-collibra username  --string-value "<collibra-sa>"
databricks secrets put-secret prod-collibra password  --string-value "<collibra-token>"
```

## Required keys per scope

| Scope         | Key        | Description                             |
|---------------|------------|-----------------------------------------|
| `*-postgres`  | `username` | PostgreSQL service account user         |
| `*-postgres`  | `password` | PostgreSQL service account password     |
| `*-sftp`      | `username` | SFTP user (if not set in config YAML)   |
| `*-sftp`      | `password` | SFTP password or private key passphrase |
| `*-collibra`  | `base_url` | Collibra REST API base URL              |
| `*-collibra`  | `username` | Collibra service account user           |
| `*-collibra`  | `password` | Collibra service account password       |

## Access control

Grant scope access only to the service principals used by ingestion/transform jobs:

```bash
databricks secrets put-acl dev-postgres "sp-ingest-dev"   READ
databricks secrets put-acl prod-postgres "sp-ingest-prod" READ
```

## Local / CI testing

For local development, set environment variables following the pattern:

```
<SCOPE>__<KEY>   (upper-case, hyphens → underscores)
```

Examples:
```bash
export DEV_POSTGRES__USERNAME=myuser
export DEV_POSTGRES__PASSWORD=mypassword
export DEV_SFTP__PASSWORD=sftppass
```

The `src/utils/secrets.get_secret()` helper automatically falls back to
these variables when `dbutils` is not available.
