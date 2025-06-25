-- Chapter 13 Part 4: Create Task Graph for Pipeline
-- Follows Snowflake best practices: naming, idempotency, context, and clear comments

use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema orchestration;

-- Create the root task
create or replace task pipeline_start_task
  warehouse = bakery_wh
  schedule = '10 minute'
  as
    call system$send_email(
      'pipeline_email_integration',
      'pratik1218patil@gmail.com',
      'Daily pipeline start',
      'The daily pipeline started at ' || current_timestamp || '.'
    );

-- Create a task that inserts the product data from the stream to the target table
create or replace task insert_product_task
  warehouse = bakery_wh
  after pipeline_start_task
  when system$stream_has_data('stg.product_stream')
  as
    insert into dwh.product_tbl
      select product_id, product_name, category, min_quantity, price, valid_from
      from stg.product_stream
      where metadata$action = 'INSERT';

-- Create a task that inserts the partner data from the stream to the target table
create or replace task insert_partner_task
  warehouse = bakery_wh
  after pipeline_start_task
  when system$stream_has_data('stg.partner_stream')
  as
    insert into dwh.partner_tbl
      select partner_id, partner_name, address, rating, valid_from
      from stg.partner_stream
      where metadata$action = 'INSERT';

-- Create the finalizer task
create or replace task pipeline_end_task
  warehouse = bakery_wh
  finalize = pipeline_start_task
  as
    call system$send_email(
      'pipeline_email_integration',
      'pratik1218patil@gmail.com',
      'Daily pipeline end',
      'The daily pipeline ended at ' || current_timestamp || '.'
    );

-- Modify the copy_orders_task to remove the schedule and to run after the pipeline_start_task
alter task copy_orders_task suspend;
alter task copy_orders_task unset schedule;
alter task copy_orders_task add after pipeline_start_task;

-- Resume all tasks
alter task pipeline_end_task resume;
alter task insert_product_task resume;
alter task insert_partner_task resume;
alter task insert_orders_stg_task resume;
alter task copy_orders_task resume;
alter task pipeline_start_task resume;

-- Wait 10 minutes (or execute the task graph manually), then view the task history
select * from table(information_schema.task_history()) order by scheduled_time desc;

-- Suspend the pipeline so it doesn't continue to consume resources and send emails
alter task pipeline_start_task suspend;