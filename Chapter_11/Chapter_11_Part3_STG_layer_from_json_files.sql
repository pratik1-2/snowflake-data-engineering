-- Chapter 11 Part 3: STG Layer from JSON Files
-- Follows Snowflake best practices: naming, casing, idempotency, and clear comments

use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema stg;

-- Create a view in the stg schema flattening the JSON into a relational structure
create or replace view json_orders_stg as
select 
    e.customer_orders:"Customer"::varchar as customer,
    e.customer_orders:"Order date"::date as order_date,
    co.value:"Delivery date"::date as delivery_date,
    do_.value:"Baked good type"::varchar as baked_good_type,
    do_.value:"Quantity"::number as quantity,
    e.source_file_name,
    e.load_ts
from 
    ext.json_orders_ext e
    , lateral flatten(input => e.customer_orders:"Orders") co
    , lateral flatten(input => co.value:"Orders by day") do_;

-- View data in the view
select * from json_orders_stg;