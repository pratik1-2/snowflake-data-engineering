-- Chapter 12 Part 2: Stream Product Example
-- Follows Snowflake best practices: naming, casing, idempotency, and clear comments

use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema ext;

-- Create a table in the dwh layer and populate initially with data from the staging layer
create or replace table dwh.product_tbl as select * from stg.product;
select * from dwh.product_tbl;

-- Create a stream on the table in the staging layer
use schema stg;
create or replace stream product_stream on table product;

-- Make changes in the staging table: one update and one insert
update product
  set category = 'Pastry', valid_from = '2023-08-08'
  where product_id = 3;

insert into product values
  (13, 'Sourdough Bread', 'Bread', 1, 3.6, '2023-08-08');

-- View the contents of the stream
select * from product_stream;

-- Consume the stream by inserting into the target table
insert into dwh.product_tbl
select product_id, product_name, category, min_quantity, price, valid_from
from product_stream
where metadata$action = 'INSERT';

-- Check that the stream is now empty
select * from product_stream;

-- View data in the target table
select * from dwh.product_tbl;

-- Create a view in the dwh layer that calculates the end timestamp of the validity interval
create or replace view dwh.product_valid_ts as
select 
  product_id, 
  product_name, 
  category, 
  min_quantity,
  price,
  valid_from,
  nvl(
    lead(valid_from) over (partition by product_id order by valid_from),
    '9999-12-31'
  ) as valid_to
from dwh.product_tbl
order by product_id;

select * from dwh.product_valid_ts;