-- Chapter 13 Part 3: Send Email Notification
-- Follows Snowflake best practices: naming, context, privilege management, and clear comments

-- Set context for notification integration creation
use role accountadmin;

create or replace notification integration pipeline_email_integration
  type = 'email'
  enabled = true;

-- Grant usage on the integration to the data_engineer role
grant usage on integration pipeline_email_integration to role data_engineer;

-- Set context for sending email
use role data_engineer;
use warehouse bakery_wh;
use database bakery_db;
use schema orchestration;

-- Send a test email using the integration
call system$send_email(
  'pipeline_email_integration',
  'pratik1218patil@gmail.com',
  'Test Email from Snowflake',
  'This is a test email sent from Snowflake using the pipeline_email_integration.'
);