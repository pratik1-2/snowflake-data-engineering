use role sysadmin;
create schema reviews:


SELECT
    value:"rating"::number as rating,
    value:"time_created"::timestamp as time_created,
    value:"text"::varchar as customer_review
FROM @bakery_reviews/yelp_reviews.json (FILE_FORMAT => my_json_format),
TABLE(FLATTEN(input => $1:reviews)) ;

-- create table CUSTOMER_REVIEWS
CREATE OR REPLACE TABLE CUSTOMER_REVIEWS(
    rating NUMBER,
    time_created TIMESTAMP,
    customer_review VARCHAR
);


-- add regex to customer_review in above select statement to remove any unprinted characters

insert into CUSTOMER_REVIEWS
SELECT
    value:"rating"::number as rating,
    value:"time_created"::timestamp as time_created,
    REGEXP_REPLACE(value:"text"::varchar, '[^a-zA-Z0-9 .,!?-]+') as customer_review
FROM @bakery_reviews/yelp_reviews.json (FILE_FORMAT => my_json_format),
TABLE(FLATTEN(input => $1:reviews)) ;


select * from CUSTOMER_REVIEWS;


-- give privileg to sysadmin to use CORTEX LLM
use role ACCOUNTADMIN;
grant database role SNOWFLAKE.CORTEXT_USER to role sysadmin;


use role sysadmin;
use database BAKERY_DB;
use schema REVIEWS;



-- get the sentiment score from different examples of text
select SNOWFLAKE.CORTEX.SENTIMENT('The service was excellent!');
select SNOWFLAKE.CORTEX.SENTIMENT('The bagel was stale.');
select SNOWFLAKE.CORTEX.SENTIMENT('I went to the bakery for lunch.');

SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT('One day I will see the world', ['travel', 'cooking']);