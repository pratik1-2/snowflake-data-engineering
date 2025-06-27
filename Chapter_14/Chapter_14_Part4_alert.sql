-- Chapter 14 Part 4: Data Quality Alert

-- Grant privilege to execute alerts
use role accountadmin;
grant execute alert on account to role data_engineer;

-- Use the data_engineer role to create an alert in the dq schema
use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema dq;

-- Query: sum values reported by data metric functions on all tables in the dwh schema within the last hour
select sum(value)
from snowflake.local.data_quality_monitoring_results
where table_database = 'BAKERY_DB'
  and table_schema = 'DWH'
  and measurement_time > dateadd('hour', -1, current_timestamp())
having sum(value) > 0;

-- Create an alert that sends an email when the previous query returns data
create or replace alert data_quality_monitoring_alert
  warehouse = bakery_wh
  schedule = '5 minute'
if (exists(
  select sum(value)
  from snowflake.local.data_quality_monitoring_results
  where table_database = 'BAKERY_DB'
    and table_schema = 'DWH'
    and measurement_time > dateadd('hour', -1, current_timestamp())
  having sum(value) > 0
))
then
  call system$send_email(
    'pipeline_email_int',
    'firstname.lastname@youremail.com', -- substitute your email address
    'Data quality monitoring alert',
    'Data metric functions reported invalid values since ' ||
      to_char(dateadd('hour', -1, current_timestamp()), 'YYYY-MM-DD HH24:MI:SS') || '.'
  );

-- Resume the alert
alter alert data_quality_monitoring_alert resume;

-- Check the execution status of the alert
select *
from table(information_schema.alert_history())
order by scheduled_time desc;

-- Suspend the alert
alter alert data_quality_monitoring_alert suspend;

-- Change the schedule to execute every hour
alter alert data_quality_monitoring_alert set schedule = '60 minute';