-- Chapter 12 Part 3: Stream Partner Example
-- Follows Snowflake best practices: naming, casing, idempotency, and clear comments

use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema dwh;

-- Create a table in the dwh layer and populate initially with data from the staging layer
create or replace table dwh.partner_tbl as select * from stg.partner;
select * from dwh.partner_tbl;

-- Create a stream on the table in the staging layer
use schema stg;
create or replace stream partner_stream on table partner;

-- Make changes in the staging table: one update
update partner
  set rating = 'A', valid_from = '2023-08-08'
  where partner_id = 103;

-- View the contents of the stream
select * from partner_stream;

-- Consume the stream by inserting into the target table
insert into dwh.partner_tbl
select partner_id, partner_name, address, rating, valid_from
from partner_stream
where metadata$action = 'INSERT';

-- Check that the stream is now empty
select * from partner_stream;

-- View data in the target table
select * from dwh.partner_tbl;

-- Create a view in the dwh layer that calculates the end timestamp of the validity interval
create or replace view dwh.partner_valid_ts as
select 
  partner_id, 
  partner_name, 
  address, 
  rating,
  valid_from,
  nvl(
    lead(valid_from) over (partition by partner_id order by valid_from),
    '9999-12-31'
  ) as valid_to
from dwh.partner_tbl
order by partner_id;

select * from dwh.partner_valid_ts;
