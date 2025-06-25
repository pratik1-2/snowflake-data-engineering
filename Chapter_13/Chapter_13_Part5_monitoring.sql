-- Chapter 13 Part 5: Monitoring and Logging
-- Follows Snowflake best practices: naming, idempotency, context, and clear comments

-- Set context
use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema orchestration;

-- Create a table to store logging information
create or replace table pipeline_log (
    run_group_id varchar,
    root_task_name varchar,
    task_name varchar,
    log_ts timestamp,
    row_processed number,
    user varchar,
    role varchar
);

-- Recreate the copy_orders_task with logging
create or replace task copy_orders_task
    warehouse = bakery_wh
    schedule = '10 minute'  -- FIXED: Use 'minute' not 'M'
as
begin
    copy into ext.json_orders_ext
    from (
        select 
            $1, 
            metadata$filename, 
            current_timestamp() 
        from @public.json_orders_stage
    )
    on_error = abort_statement;

    insert into pipeline_log
    select
        system$task_runtime_info('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
        system$task_runtime_info('CURRENT_ROOT_TASK_NAME'),
        system$task_runtime_info('CURRENT_TASK_NAME'),
        current_timestamp(),
        :sqlrowcount,
        current_user(),
        current_role();
end;

-- Execute the task manually
execute task copy_orders_task;

-- Check the task history
select *
from table(information_schema.task_history())
order by scheduled_time desc;

-- Verify that data was inserted into the logging table
select * from pipeline_log;

-- Alter the task to unset the schedule and add a dependency on the root task
alter task pipeline_start_task suspend;
alter task copy_orders_task unset schedule;
alter task copy_orders_task add after pipeline_start_task;

-- Recreate the insert_orders_stg_task and insert data into the logging table
create or replace task insert_orders_stg_task
    warehouse = bakery_wh
    after copy_orders_task
    when system$stream_has_data('ext.json_orders_stream')
as
begin
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

    insert into pipeline_log
    select
        system$task_runtime_info('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
        system$task_runtime_info('CURRENT_ROOT_TASK_NAME'),
        system$task_runtime_info('CURRENT_TASK_NAME'),
        current_timestamp(),
        :sqlrowcount,
        current_user(),
        current_role();
end;

-- Recreate the insert_product_task and insert data into the logging table
create or replace task insert_product_task
    warehouse = bakery_wh
    after pipeline_start_task
    when system$stream_has_data('stg.product_stream')
as
begin
    insert into dwh.product_tbl
    select product_id, product_name, category, 
        min_quantity, price, valid_from
    from stg.product_stream
    where metadata$action = 'INSERT';

    insert into pipeline_log
    select
        system$task_runtime_info('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
        system$task_runtime_info('CURRENT_ROOT_TASK_NAME'),
        system$task_runtime_info('CURRENT_TASK_NAME'),
        current_timestamp(),
        :sqlrowcount,
        current_user(),
        current_role();
end;

-- Recreate the insert_partner_task and insert data into the logging table
create or replace task insert_partner_task
    warehouse = bakery_wh
    after pipeline_start_task
    when system$stream_has_data('stg.partner_stream')
as
begin
    insert into dwh.partner_tbl
    select partner_id, partner_name, address, rating, valid_from
    from stg.partner_stream
    where metadata$action = 'INSERT';

    insert into pipeline_log
    select
        system$task_runtime_info('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
        system$task_runtime_info('CURRENT_ROOT_TASK_NAME'),
        system$task_runtime_info('CURRENT_TASK_NAME'),
        current_timestamp(),
        :sqlrowcount,
        current_user(),
        current_role();
end;

-- Recreate the finalizer task by constructing a return_message string with the logging information from all tasks in the current run
create or replace task PIPELINE_END_TASK
  warehouse = BAKERY_WH
  finalize = PIPELINE_START_TASK
as
  declare
    return_message varchar := '';
  begin
    let log_cur cursor for
      select task_name, row_processed 
      from PIPELINE_LOG 
      where run_group_id = 
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID');

    for log_rec in log_cur loop
      return_message := return_message ||
        'Task: '|| log_rec.task_name || 
        ' Rows processed: ' || log_rec.row_processed ||  '\n';
    end loop;
  
    call SYSTEM$SEND_EMAIL(
      'PIPELINE_EMAIL_INTEGRATION',
      'pratik1218patil@gmail.com',    
      'Daily pipeline end',
      'The daily pipeline finished at ' || current_timestamp || '.' ||
        '\n\n' || :return_message

    );
  end;
-- add data to the sources
-- upload the Orders_2023-09-08.json file to the cloud storage location
-- insert partner data
insert into STG.PARTNER values(
  113, 'Lazy Brunch', '1012 Astoria Avenue', 'A', '2023-09-01'
);
-- update product data
update STG.PRODUCT set min_quantity = 5 where product_id = 5;

-- resume all tasks
alter task PIPELINE_END_TASK resume;
alter task INSERT_PRODUCT_TASK resume;
alter task INSERT_PARTNER_TASK resume;
alter task INSERT_ORDERS_STG_TASK resume;
alter task COPY_ORDERS_TASK resume;
alter task PIPELINE_START_TASK resume;

-- execute the root task manually
execute task PIPELINE_START_TASK;

 -- check the TASK_HISTORY()
select *
from table(information_schema.task_history())
order by scheduled_time desc;

-- view data in the logging table
select * from PIPELINE_LOG order by log_ts desc;

-- suspend the pipeline so it doesn't continue to consume resources and send emails
alter task PIPELINE_START_TASK suspend;