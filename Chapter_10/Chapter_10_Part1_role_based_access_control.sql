-- Use SYSADMIN role to set up warehouse and database
USE ROLE SYSADMIN;
CREATE WAREHOUSE IF NOT EXISTS bakery_wh WITH WAREHOUSE_SIZE = 'XSMALL';
CREATE DATABASE IF NOT EXISTS bakery_db;
USE DATABASE bakery_db;

-- Create schemas with managed access
CREATE SCHEMA IF NOT EXISTS raw WITH MANAGED ACCESS;
CREATE SCHEMA IF NOT EXISTS rpt WITH MANAGED ACCESS;

-- Create roles using USERADMIN
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS bakery_full;
CREATE ROLE IF NOT EXISTS bakery_read;
CREATE ROLE IF NOT EXISTS data_engineer;
CREATE ROLE IF NOT EXISTS data_analyst;

-- Grant privileges using SECURITYADMIN
USE ROLE SECURITYADMIN;
-- Full access role
GRANT USAGE ON DATABASE bakery_db TO ROLE bakery_full;
GRANT USAGE ON ALL SCHEMAS IN DATABASE bakery_db TO ROLE bakery_full;
GRANT ALL ON SCHEMA bakery_db.raw TO ROLE bakery_full;
GRANT ALL ON SCHEMA bakery_db.rpt TO ROLE bakery_full;
-- Read-only access role
GRANT USAGE ON DATABASE bakery_db TO ROLE bakery_read;
GRANT USAGE ON SCHEMA bakery_db.rpt TO ROLE bakery_read;
GRANT SELECT ON ALL TABLES IN SCHEMA bakery_db.rpt TO ROLE bakery_read;
GRANT SELECT ON ALL VIEWS IN SCHEMA bakery_db.rpt TO ROLE bakery_read;
-- Future grants for read-only
GRANT SELECT ON FUTURE TABLES IN SCHEMA bakery_db.rpt TO ROLE bakery_read;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA bakery_db.rpt TO ROLE bakery_read;
-- Role hierarchy
GRANT ROLE bakery_full TO ROLE data_engineer;
GRANT ROLE bakery_read TO ROLE data_analyst;
GRANT ROLE data_engineer TO ROLE SYSADMIN;
GRANT ROLE data_analyst TO ROLE SYSADMIN;
-- Grant roles to current user for testing
SET my_current_user = CURRENT_USER();
GRANT ROLE data_engineer TO USER IDENTIFIER($my_current_user);
GRANT ROLE data_analyst TO USER IDENTIFIER($my_current_user);
-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE bakery_wh TO ROLE data_engineer;
GRANT USAGE ON WAREHOUSE bakery_wh TO ROLE data_analyst;

-- Test: data_engineer creates table and inserts data in raw schema
USE ROLE data_engineer;
USE WAREHOUSE bakery_wh;
USE DATABASE bakery_db;
USE SCHEMA raw;
CREATE OR REPLACE TABLE employee (
  id INTEGER,
  name VARCHAR,
  home_address VARCHAR,
  department VARCHAR,
  hire_date DATE
);
INSERT INTO employee VALUES
  (1001, 'William Jones', '5170 Arcu St.', 'Bread', '2020-02-01'),
  (1002, 'Alexander North', '261 Ipsum Rd.', 'Pastry', '2021-04-01'),
  (1003, 'Jennifer Navarro', '880 Dictum Ave.', 'Pastry', '2019-08-01'),
  (1004, 'Sandra Perkins', '55 Velo St.', 'Bread', '2022-05-01');

-- Test: data_analyst tries to select from raw.employee (should fail)
USE ROLE data_analyst;
SELECT * FROM raw.employee;
-- Expected: Permission denied

-- data_engineer creates a view in rpt schema
USE ROLE data_engineer;
USE SCHEMA rpt;
CREATE OR REPLACE VIEW employee AS 
SELECT id, name, home_address, department, hire_date
FROM raw.employee;

-- data_analyst selects from the view in rpt schema (should succeed)
USE ROLE data_analyst;
SELECT * FROM rpt.employee;
-- Expected: Returns values