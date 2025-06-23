-- Chapter 11 Part 1: Create Schemas and Grant Privileges
-- Follows Snowflake best practices: idempotency, casing, clear comments, and privilege management

-- Set context to SYSADMIN for schema creation
use role sysadmin;
use database bakery_db;

-- Create schemas with managed access (idempotent)
create schema if not exists ext with managed access;
create schema if not exists stg with managed access;
create schema if not exists dwh with managed access;
create schema if not exists mgmt with managed access;

-- Switch to SECURITYADMIN for grants (required for MANAGE GRANTS privilege)
use role securityadmin;

-- Grant full privileges on all schemas to the bakery_full role
grant all on schema bakery_db.ext to role bakery_full;
grant all on schema bakery_db.stg to role bakery_full;
grant all on schema bakery_db.dwh to role bakery_full;
grant all on schema bakery_db.mgmt to role bakery_full;

-- Grant read-only privileges on mgmt schema to bakery_read role
grant select on all tables in schema bakery_db.mgmt to role bakery_read;
grant select on all views in schema bakery_db.mgmt to role bakery_read;

-- Grant future read-only privileges on mgmt schema to bakery_read role
grant select on future tables in schema bakery_db.mgmt to role bakery_read;
grant select on future views in schema bakery_db.mgmt to role bakery_read;