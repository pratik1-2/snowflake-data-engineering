-- Chapter 11 Part 2: EXT Layer Setup
-- Snowflake best practices: idempotency, casing, context, and privilege management

-- Set up storage integration (run as ACCOUNTADMIN)
use role accountadmin;

create or replace storage integration park_inn_integration
  type = external_stage
  storage_provider = 'S3'
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::783764576334:role/mysnowflakerole'
  storage_allowed_locations = ('s3://pratik12-snowflake-data-engineering/');

describe storage integration park_inn_integration;

grant usage on integration park_inn_integration to role sysadmin;
grant usage on integration park_inn_integration to role data_engineer;

-- Create stage for JSON orders (run as SYSADMIN)
use role sysadmin;
use database bakery_db;

create or replace stage public.json_orders_stage
  storage_integration = park_inn_integration
  url = 's3://pratik12-snowflake-data-engineering/json_orders'
  file_format = (type = 'json');

list @public.json_orders_stage;

grant usage on stage public.json_orders_stage to role data_engineer;

-- Data engineer context for loading data
use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema ext;

-- Create extract table for raw JSON orders (idempotent)
create or replace table json_orders_ext (
  customer_orders variant,
  source_file_name varchar,
  load_ts timestamp
);

-- Copy data from stage into extract table
copy into json_orders_ext
from (
  select $1, metadata$filename, current_timestamp()
  from @public.json_orders_stage
)
on_error = abort_statement;

-- Verify load
select * from json_orders_ext;
-- Output: one row per file uploaded