-- Chapter 13 Part 1: Create Orchestration Schema
-- Follows Snowflake best practices: naming, idempotency, context, and clear comments

use role sysadmin;
use database bakery_db;

-- Create schema with managed access (idempotent)
create schema if not exists orchestration with managed access;

-- Grant full privileges on the orchestration schema to the bakery_full role (run as securityadmin)
use role securityadmin;
grant all on schema bakery_db.orchestration to role bakery_full;