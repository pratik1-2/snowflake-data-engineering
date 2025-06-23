-- Chapter 12 Part 1: Stream Orders Example
-- Follows Snowflake best practices: naming, casing, idempotency, and clear comments

use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema ext;

-- Delete all files from the object storage location used in the json_orders_stage stage
-- Upload the json file orders_2023-09-05.json to the object storage location

-- Recreate the table to remove any data from previous exercises
create or replace table json_orders_ext (
  customer_orders variant,
  source_file_name varchar,
  load_ts timestamp
);

-- Create a stream on the table
create or replace stream json_orders_stream on table json_orders_ext;

-- View data in the stream (should be empty)
select * from json_orders_stream;

-- View files in the stage
list @public.json_orders_stage;

-- Copy data from the stage into the json_orders_ext table
copy into json_orders_ext
from (
  select $1, metadata$filename, current_timestamp()
  from @public.json_orders_stage
)
on_error = abort_statement;

-- The output from the copy command should indicate that data from the orders_2023-09-05.json file was copied into the table

-- Check the data in the stream again (should contain the newly uploaded file)
select * from json_orders_stream;

-- Create a staging table in the stg schema for flattened data
create or replace table stg.json_orders_tbl_stg (
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);

-- Insert the flattened data from the stream into the staging table
insert into stg.json_orders_tbl_stg
select 
  customer_orders:"Customer"::varchar as customer, 
  customer_orders:"Order date"::date as order_date, 
  co.value:"Delivery date"::date as delivery_date,
  do_.value:"Baked good type"::varchar as baked_good_type,
  do_.value:"Quantity"::number as quantity,
  source_file_name,
  load_ts
from ext.json_orders_stream,
  lateral flatten(input => customer_orders:"Orders") co,
  lateral flatten(input => co.value:"Orders by day") do_;

-- Check the data in the table (should show 8 rows)
select * from stg.json_orders_tbl_stg;

-- Check the data in the stream again (should now be empty)
select * from json_orders_stream;

-- Repeat with another file
-- Upload the json file orders_2023-09-06.json to the object storage location

-- View files in the stage
list @public.json_orders_stage;

-- Copy data from the stage into the json_orders_ext table
copy into json_orders_ext
from (
  select $1, metadata$filename, current_timestamp()
  from @public.json_orders_stage
)
on_error = abort_statement;

-- The output from the copy command should indicate that data from the orders_2023-09-06.json file was copied into the table

-- Check the data in the stream again (should contain the newly uploaded file)
select * from json_orders_stream;

-- Perform the insert statement again
insert into stg.json_orders_tbl_stg
select 
  customer_orders:"Customer"::varchar as customer, 
  customer_orders:"Order date"::date as order_date, 
  co.value:"Delivery date"::date as delivery_date,
  do_.value:"Baked good type"::varchar as baked_good_type,
  do_.value:"Quantity"::number as quantity,
  source_file_name,
  load_ts
from ext.json_orders_stream,
  lateral flatten(input => customer_orders:"Orders") co,
  lateral flatten(input => co.value:"Orders by day") do_;

-- Should insert 4 rows