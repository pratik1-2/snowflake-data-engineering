-- Set up schema and stage for delivery orders
use role sysadmin;
use database BAKERY_DB;
create schema if not exists DELIVERY_ORDERS;
use schema DELIVERY_ORDERS;

-- Create an external stage using the storage integration
create or replace stage SPEEDY_STAGE
    storage_integration = BISTRO_INTEGRATION
    url = 's3://pratik12-snowflake-data-engineering/speedyservicefiles'
    file_format = (type = 'json');

-- View files in the external stage
list @SPEEDY_STAGE;

-- View data in the staged files
select $1 from @SPEEDY_STAGE;

-- Extract ORDER_ID and ORDER_DATETIME columns from the JSON, leave ITEMS as variant
select
    $1:"Order id" as order_id,
    $1:"Order datetime" as order_datetime,
    $1:"Items" as items,
    metadata$filename as source_file_name,
    current_timestamp() as load_ts
from @SPEEDY_STAGE;

-- Create staging table for delivery orders
create or replace table SPEEDY_ORDERS_RAW_STG (
    order_id varchar,
    order_datetime timestamp,
    items variant,
    source_file_name varchar,
    load_ts timestamp
);

-- Create a Snowpipe to ingest data from the S3 external stage into the staging table
create or replace pipe SPEEDY_ORDERS_PIPE
auto_ingest = true
as
copy into SPEEDY_ORDERS_RAW_STG
from (
    select
        $1:"Order id"::varchar as order_id,
        $1:"Order datetime"::timestamp as order_datetime,
        $1:"Items" as items,
        metadata$filename as source_file_name,
        current_timestamp() as load_ts
    from @SPEEDY_STAGE
);

-- Load historical data from files that existed in the external stage before Event Grid messages were configured
alter pipe SPEEDY_ORDERS_PIPE refresh;

-- View data in the staging table
select * from SPEEDY_ORDERS_RAW_STG;

-- Check the status of the pipe
select system$pipe_status('SPEEDY_ORDERS_PIPE');

-- View the copy history in the last hour
select *
from table(information_schema.copy_history(
    table_name => 'SPEEDY_ORDERS_RAW_STG',
    start_time => dateadd(hours, -1, current_timestamp())
));

-- Select the values from the second level keys (flatten items array)
select
    order_id,
    order_datetime,
    value:"Item"::varchar as baked_good_type,
    value:"Quantity"::number as quantity
from SPEEDY_ORDERS_RAW_STG,
lateral flatten(input => items);

-- Create a dynamic table that materializes the output of the previous query
create or replace dynamic table SPEEDY_ORDERS
    target_lag = '1 minute'
    warehouse = BAKERY_WH
as
select
    order_id,
    order_datetime,
    value:"Item"::varchar as baked_good_type,
    value:"Quantity"::number as quantity
from SPEEDY_ORDERS_RAW_STG,
lateral flatten(input => items);

-- Query the data in the dynamic table
select *
from SPEEDY_ORDERS
order by order_datetime desc;

-- Query the dynamic table refresh history
select *
from table(information_schema.dynamic_table_refresh_history())
order by refresh_start_time desc;