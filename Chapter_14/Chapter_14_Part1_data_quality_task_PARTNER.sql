-- Chapter 14 Part 1: Data Quality Task for PARTNER

-- Set context
use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema stg;

-- Insert a new partner into the partner staging table
insert into stg.partner values
    (114, 'Country Market', '12 Meadow Lane', null, '2023-10-10');

-- Insert a new product into the product staging table
insert into stg.product values
    (14, 'Banana Muffin', 'Cake', 12, 3.20, '2023-10-10');

-- Execute pipeline manually
execute task orchestration.pipeline_start_task;

select * from table(information_schema.task_history())
order by scheduled_time desc;

-- You should also receive two emails, one when the pipeline started and one when the pipeline completed

-- Create DQ schema
use role sysadmin;
use database bakery_db;
create schema if not exists dq with managed access;

use role securityadmin;
grant all on schema bakery_db.dq to role data_engineer;

use role data_engineer;
use schema dq;

-- Create a table to store data quality information
create or replace table dq_log (
    run_group_id varchar,         -- CURRENT_TASK_GRAPH_RUN_GROUP_ID
    root_task_name varchar,       -- CURRENT_ROOT_TASK_NAME
    task_name varchar,            -- CURRENT_TASK_NAME
    log_ts timestamp,
    database_name varchar,
    schema_name varchar,
    table_name varchar,
    dq_rule_name varchar,
    error_cnt number,
    error_info variant
);

-- Go back to the orchestration schema to work on the tasks
use schema orchestration;

-- Select rows where the rating is null
select * from dwh.partner_tbl where rating is null;

-- Select an array of partner ids of all rows where the rating is null
select array_agg(partner_id) from dwh.partner_tbl where rating is null;

-- Create a partner data quality task
create or replace task partner_dq_task
    warehouse = bakery_wh
    schedule = '10 MINUTE'
as
declare
    error_info variant;
    error_cnt integer;
begin
    select array_agg(partner_id) into :error_info
    from dwh.partner_tbl
    where rating is null;

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
            'PARTNER_TBL',
            'Null values in the RATING column',
            :error_cnt,
            :error_info;
    end if;
end;

-- Execute the task manually to test
execute task partner_dq_task;

-- Check the task history
select *
from table(information_schema.task_history())
order by scheduled_time desc;

-- View the data inserted into the dq_log table
select * from dq.dq_log;

-- Unset the schedule from the task and make it dependent on the insert_partner_task
alter task partner_dq_task unset schedule;
alter task partner_dq_task add after insert_partner_task;

-- Resume the task so it will run in the pipeline
alter task partner_dq_task resume;