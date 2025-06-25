-- Chapter 13 Part 2: Create Tasks for Orders
-- Follows Snowflake best practices: naming, idempotency, context, and clear comments

-- Grant execute task privilege to the data_engineer role (run as accountadmin)
use role accountadmin;
grant execute task on account to role data_engineer;

-- Continue as data_engineer
use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema orchestration;

-- Create a task that performs the copy into operation from the stage into the table
create or replace task copy_orders_task
  warehouse = bakery_wh
  schedule = '10 minute'
  as
  copy into ext.json_orders_ext
  from (
    select $1, metadata$filename, current_timestamp()
    from @public.json_orders_stage
  )
  on_error = abort_statement;

-- Execute the task once to verify that it is working
execute task copy_orders_task;

-- View the task history ordered by scheduled_time descending
select * from table(information_schema.task_history()) order by scheduled_time desc;

-- Create a task that inserts data from the stream into the staging table
create or replace task insert_orders_stg_task
  warehouse = bakery_wh
  after copy_orders_task
  when system$stream_has_data('ext.json_orders_stream')
  as
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

-- To test the task, you may remove the dependency, execute manually, then add the dependency again
alter task insert_orders_stg_task remove after copy_orders_task;
execute task insert_orders_stg_task;
select * from table(information_schema.task_history()) order by scheduled_time desc;
alter task insert_orders_stg_task add after copy_orders_task;

-- Enable the child and parent tasks
alter task insert_orders_stg_task resume;
alter task copy_orders_task resume;

-- Upload the json file orders_2023-09-07.json to the object storage location
-- Wait until the tasks execute on schedule

-- View the task history
select * from table(information_schema.task_history()) order by scheduled_time desc;

-- Suspend the task when you are done testing
alter task copy_orders_task suspend;

