-- Chapter 04 Part 2: Combine and merge orders from all sources
use role SYSADMIN;
use database BAKERY_DB;
create schema if not exists TRANSFORM;
use schema TRANSFORM;

-- create a view that combines data from individual staging tables
create or replace view ORDERS_COMBINED_STG as
select 
    customer,
    order_date,
    delivery_date,
    baked_good_type,
    quantity,
    source_file_name,
    load_ts
from bakery_db.orders.ORDERS_STG
union all
select 
    customer,
    order_date,
    delivery_date,
    baked_good_type,
    quantity,
    source_file_name,
    load_ts
from bakery_db.external_orders.ORDERS_BISTRO_STG
union all
select 
    customer,
    order_date,
    delivery_date,
    baked_good_type,
    quantity,
    source_file_name,
    load_ts
from bakery_db.external_json_orders.ORDERS_PARK_INN_STG;

-- create target table that will store historical orders combined from all sources
use database BAKERY_DB;
use schema TRANSFORM;
create or replace table CUSTOMER_ORDERS_COMBINED  (
    customer varchar,
    order_date date,
    delivery_date date,
    baked_good_type varchar,
    quantity number,
    source_file_name varchar,
    load_ts timestamp
);

-- merge combined staging data into the target table
-- deletion of duplicates is recommended before merge to avoid errors
merge into CUSTOMER_ORDERS_COMBINED as tgt
using (
    select * from (
        select *,
            row_number() over (partition by customer, order_date, baked_good_type order by load_ts desc) as rn
        from ORDERS_COMBINED_STG
    ) where rn = 1
) as src
on tgt.customer = src.customer 
    and tgt.delivery_date = src.delivery_date
    and tgt.baked_good_type = src.baked_good_type
when matched then
    update set 
        tgt.quantity = src.quantity,
        tgt.source_file_name = src.source_file_name,
        tgt.load_ts = current_timestamp()
when not matched then
    insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
    values (src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_timestamp());


-- create a stored procedure that executes the previous MERGE statement
-- Listing 4.6 
use database BAKERY_DB;
use schema TRANSFORM;
create or replace procedure LOAD_CUSTOMER_ORDERS()
returns varchar
language sql
as
$$
begin
    merge into CUSTOMER_ORDERS_COMBINED as tgt
    using ORDERS_COMBINED_STG as src
    on tgt.customer = src.customer 
        and tgt.delivery_date = src.delivery_date
        and tgt.baked_good_type = src.baked_good_type
    when matched then
        update set 
            tgt.quantity = src.quantity,
            tgt.source_file_name = src.source_file_name,
            tgt.load_ts = current_timestamp()
    when not matched then
        insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
        values (src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_timestamp());
    
    return 'Customer orders loaded successfully';
end;
$$;

-- execute the stored procedure
call LOAD_CUSTOMER_ORDERS();

select * from CUSTOMER_ORDERS_COMBINED;


use database BAKERY_DB;
use schema TRANSFORM;
create or replace procedure LOAD_CUSTOMER_ORDERS()
returns varchar
language sql
as
$$
begin
  merge into CUSTOMER_ORDERS_COMBINED tgt
using ORDERS_COMBINED_STG as src
on src.customer = tgt.customer and src.delivery_date = tgt.delivery_date and src.baked_good_type = tgt.baked_good_type
when matched then 
  update set tgt.quantity = src.quantity, 
    tgt.source_file_name = src.source_file_name, 
    tgt.load_ts = current_timestamp()
when not matched then
  insert (customer, order_date, delivery_date, 
    baked_good_type, quantity, source_file_name, load_ts)
  values(src.customer, src.order_date, src.delivery_date,
    src.baked_good_type, src.quantity, src.source_file_name,
    current_timestamp());
  return 'Load completed. ' || SQLROWCOUNT || ' rows affected.';
exception
  when other then
    return 'Load failed with error message: ' || SQLERRM;
end;
$$
;