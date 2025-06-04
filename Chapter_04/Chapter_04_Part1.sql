-- Chapter 04 Part 1: Load and transform JSON restaurant orders
use role ACCOUNTADMIN;
describe storage integration BISTRO_INTEGRATION;

-- create a new schema in the BAKERY_DB database 
use role SYSADMIN;
use warehouse BAKERY_WH;
use database BAKERY_DB;
create schema if not exists EXTERNAL_JSON_ORDERS;
use schema EXTERNAL_JSON_ORDERS;

-- create an external stage using the storage integration
create or replace stage PARK_INN_STAGE
    storage_integration = BISTRO_INTEGRATION
    url = 's3://pratik12-snowflake-data-engineering/parkinnorders001'
    file_format = (type = 'json');

-- view files in the external stage
list @PARK_INN_STAGE;

-- view data in the staged file
select $1 from @PARK_INN_STAGE;

-- create staging table for restaurant orders in raw (json) format
create or replace table ORDERS_PARK_INN_RAW_STAGE (
    customer_orders variant,
    source_file_name varchar,
    load_ts timestamp
);

-- load data from the stage into the staging table
copy into ORDERS_PARK_INN_RAW_STAGE
from (
    select $1, metadata$filename, current_timestamp()
    from @PARK_INN_STAGE
)
on_error = abort_statement;

-- view data in the staging table
select * from BAKERY_DB.EXTERNAL_JSON_ORDERS.ORDERS_PARK_INN_RAW_STAGE;

-- select the values from the first level keys
-- Listing 4.2 
select 
    customer_orders:"Customer"::varchar as customer,
    customer_orders:"Order date"::date as order_date,
    customer_orders:"Orders"
from ORDERS_PARK_INN_RAW_STAGE;

-- select the values from the second level keys using LATERAL FLATTEN
-- Listing 4.3 
select 
    customer_orders:"Customer"::varchar as customer,
    customer_orders:"Order date"::date as order_date,
    value:"Delivery date"::date as delivery_date,
    value:"Orders by day"
from ORDERS_PARK_INN_RAW_STAGE,
lateral flatten (input => customer_orders:"Orders");

-- select the values from the third level keys using another LATERAL FLATTEN
-- Listing 4.4 
select 
    customer_orders:"Customer"::varchar as customer,
    customer_orders:"Order date"::date as order_date,
    CO.value:"Delivery date"::date as delivery_date,
    DO.value:"Baked good type"::varchar as baked_good_type,
    DO.value:"Quantity"::number as quantity
from ORDERS_PARK_INN_RAW_STAGE,
lateral flatten (input => customer_orders:"Orders") CO,
lateral flatten (input => CO.value:"Orders by day") DO;

-- create a view to represent a relational staging table using the previous query
create or replace view ORDERS_PARK_INN_STG as
select 
    customer_orders:"Customer"::varchar as customer,
    customer_orders:"Order date"::date as order_date,
    CO.value:"Delivery date"::date as delivery_date,
    DO.value:"Baked good type"::varchar as baked_good_type,
    DO.value:"Quantity"::number as quantity,
    source_file_name,
    load_ts
from ORDERS_PARK_INN_RAW_STAGE,
lateral flatten (input => customer_orders:"Orders") CO,
lateral flatten (input => CO.value:"Orders by day") DO;

-- view data in the view
select *
from ORDERS_PARK_INN_STG;