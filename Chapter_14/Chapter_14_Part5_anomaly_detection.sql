+-- Chapter 14 Part 5: Anomaly Detection with Snowflake ML

-- Set context
use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema stg;

-- Generate random data representing supermarket orders
create or replace table stg.country_market_orders as
with raw_data as (
    select
        dateadd('day', uniform(1, 180, random()), '2023-11-01'::date) as delivery_date,
        uniform(1, 14, random()) as product_id,
        uniform(500, 1000, random()) as quantity
    from table(generator(rowcount => 10000))
)
select 
    'Country Market' as customer, 
    delivery_date, 
    product_id, 
    sum(quantity) as quantity
from raw_data
group by all;

-- Simulate data anomalies
update stg.country_market_orders 
    set quantity = 0.2 * quantity 
    where delivery_date between '2024-03-10' and '2024-03-15';

update stg.country_market_orders 
    set quantity = 0 
    where delivery_date between '2024-03-21' and '2024-03-22';

-- View the quantity distribution as a line chart
select * from stg.country_market_orders;

-- Grant the create anomaly detection privilege to the data_engineer role
use role accountadmin;
grant create snowflake.ml.anomaly_detection 
    on schema bakery_db.dq
    to role data_engineer;

-- Continue working with the data_engineer role in the dq schema
use role data_engineer;
use schema dq;

-- Historical data before March 1 on which the model trains
create or replace view orders_historical_data as
select 
    delivery_date::timestamp as delivery_ts, 
    sum(quantity) as quantity
from stg.country_market_orders
where delivery_date < '2024-03-01'
group by delivery_ts;

-- New data after March 1 on which the model looks for anomalies
create or replace view orders_new_data as
select 
    delivery_date::timestamp as delivery_ts, 
    sum(quantity) as quantity
from stg.country_market_orders
where delivery_date >= '2024-03-01'
group by delivery_ts;

-- Train the model on historical data
create or replace snowflake.ml.anomaly_detection orders_model (
    input_data => system$reference('VIEW', 'ORDERS_HISTORICAL_DATA'),
    timestamp_colname => 'delivery_ts',
    target_colname => 'quantity',
    label_colname => ''
);

-- Calculate anomalies on new data
call orders_model!detect_anomalies(
    input_data => system$reference('VIEW', 'ORDERS_NEW_DATA'),
    timestamp_colname => 'delivery_ts',
    target_colname => 'quantity'
);

-- Save the output to a table
create or replace table orders_model_anomalies as 
select * from table(result_scan(last_query_id()));