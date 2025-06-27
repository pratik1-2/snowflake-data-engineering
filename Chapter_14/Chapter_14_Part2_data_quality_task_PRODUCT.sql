-- Chapter 14 Part 2: Data Quality Task for PRODUCT

-- Set context
use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema orchestration;

-- Select rows where the category is not one of the allowed values
select * 
from dwh.product_tbl 
where category not in ('Bread', 'Pastry');

-- Create the product data quality task
create or replace task product_dq_task
    warehouse = bakery_wh
    schedule = '10 MINUTE'
as
declare
    error_info variant;
    error_cnt integer;
begin
    select array_agg(product_id) into :error_info
    from dwh.product_tbl
    where category not in ('Bread', 'Pastry');

    error_cnt := array_size(:error_info);

    if (error_cnt > 0) then
        insert into dq.dq_log
        select
            system$task_runtime_info('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
            system$task_runtime_info('CURRENT_ROOT_TASK_NAME'),
            system$task_runtime_info('CURRENT_TASK_NAME'),
            current_timestamp(),
            'BAKERY_DB',
            'DWH',
            'PRODUCT_TBL',
            'Invalid values in the CATEGORY column',
            :error_cnt,
            :error_info;
    end if;
end;

-- Execute the task manually to test
execute task product_dq_task;

-- Check the task history
select *
from table(information_schema.task_history())
order by scheduled_time desc;

-- View the data inserted into the dq_log table
select * from dq.dq_log;

-- Unset the schedule from the task and make it dependent on the insert_product_task
alter task product_dq_task unset schedule;
alter task product_dq_task add after insert_product_task;

-- Resume the task so it will run in the pipeline
alter task product_dq_task resume;

-- Before executing the pipeline, update the rows in the staging table so the streams have data
update stg.partner set valid_from = '2023-10-11' where partner_id = 114;
update stg.product set valid_from = '2023-10-11' where product_id = 14;

-- Execute the pipeline manually
execute task orchestration.pipeline_start_task;

-- Check the task history
select *
from table(information_schema.task_history())
order by scheduled_time desc;

-- You should also receive two emails, one when the pipeline started and one when the pipeline completed

-- Check the dq_log table
select * from dq.dq_log order by log_ts desc;