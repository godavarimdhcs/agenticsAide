-- =============================================================================
-- Unity Catalog Grants per Layer and Environment
-- =============================================================================
-- Run these statements in a Databricks SQL warehouse connected to the Unity
-- Catalog metastore.  Replace <env> with dev / qa / prod.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Catalogs
-- ---------------------------------------------------------------------------

-- Data engineers (full access per environment)
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG dev_lakehouse TO `data-engineers-dev`;
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG qa_lakehouse  TO `data-engineers-qa`;
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG prod_lakehouse TO `data-engineers-prod`;

-- Data analysts (read Silver + Gold only)
GRANT USE CATALOG ON CATALOG dev_lakehouse  TO `data-analysts`;
GRANT USE CATALOG ON CATALOG qa_lakehouse   TO `data-analysts`;
GRANT USE CATALOG ON CATALOG prod_lakehouse TO `data-analysts`;

-- ---------------------------------------------------------------------------
-- Bronze layer – restricted; ingestion service principals only
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA dev_lakehouse.bronze  TO `sp-ingest-dev`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA qa_lakehouse.bronze   TO `sp-ingest-qa`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA prod_lakehouse.bronze TO `sp-ingest-prod`;

-- Deny direct analyst access to Bronze (raw / unmasked data)
-- (No explicit DENY needed in Unity Catalog; simply do not grant.)

-- ---------------------------------------------------------------------------
-- Silver layer – analysts get read; engineers + DLT get write
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA, SELECT ON SCHEMA dev_lakehouse.silver  TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA qa_lakehouse.silver   TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod_lakehouse.silver TO `data-analysts`;

GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA dev_lakehouse.silver  TO `sp-transform-dev`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA qa_lakehouse.silver   TO `sp-transform-qa`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA prod_lakehouse.silver TO `sp-transform-prod`;

-- ---------------------------------------------------------------------------
-- Gold layer – broad read; limited write
-- ---------------------------------------------------------------------------
GRANT USE SCHEMA, SELECT ON SCHEMA dev_lakehouse.gold  TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA qa_lakehouse.gold   TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod_lakehouse.gold TO `data-analysts`;

GRANT USE SCHEMA, SELECT ON SCHEMA dev_lakehouse.gold  TO `data-scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod_lakehouse.gold TO `data-scientists`;

GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA dev_lakehouse.gold  TO `sp-transform-dev`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA prod_lakehouse.gold TO `sp-transform-prod`;

-- ---------------------------------------------------------------------------
-- External Locations (ADLS Gen2 storage credentials)
-- ---------------------------------------------------------------------------
GRANT READ FILES ON EXTERNAL LOCATION dev_adls   TO `sp-ingest-dev`;
GRANT READ FILES ON EXTERNAL LOCATION qa_adls    TO `sp-ingest-qa`;
GRANT READ FILES ON EXTERNAL LOCATION prod_adls  TO `sp-ingest-prod`;

GRANT WRITE FILES ON EXTERNAL LOCATION dev_adls  TO `sp-ingest-dev`;
GRANT WRITE FILES ON EXTERNAL LOCATION qa_adls   TO `sp-ingest-qa`;
GRANT WRITE FILES ON EXTERNAL LOCATION prod_adls TO `sp-ingest-prod`;

-- ---------------------------------------------------------------------------
-- Data classification tags
-- ---------------------------------------------------------------------------
-- Apply tags to PII columns for masking / audit purposes.
-- Requires Unity Catalog system tables or ALTER TABLE ... SET TAGS.

-- Example: tag customer email as PII
-- ALTER TABLE prod_lakehouse.silver.crm_customers
--   ALTER COLUMN email SET TAGS ('pii' = 'true', 'classification' = 'sensitive');

-- ---------------------------------------------------------------------------
-- Row/Column-level security (masking example)
-- ---------------------------------------------------------------------------
-- Mask email column for non-privileged users:
-- CREATE OR REPLACE FUNCTION prod_lakehouse.silver.mask_email(email STRING)
--   RETURNS STRING
--   RETURN CASE
--     WHEN is_member('data-privileged') THEN email
--     ELSE CONCAT(LEFT(email, 2), '***@***.***')
--   END;
--
-- ALTER TABLE prod_lakehouse.silver.crm_customers
--   ALTER COLUMN email
--   SET MASK prod_lakehouse.silver.mask_email;
