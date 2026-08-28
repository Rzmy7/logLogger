-- 000001_create_metadata_tables.down.sql
-- Reversible teardown of multi-tenant metadata tables

DROP TABLE IF EXISTS retention_policies CASCADE;
DROP TABLE IF EXISTS services CASCADE;
DROP TABLE IF EXISTS api_keys CASCADE;
DROP TABLE IF EXISTS tenants CASCADE;
