--- Snowflake Streams

--- CREATE TABLE FOR A FUTURE PLACEMENT OF THE TABLE
create table W2 ("C1" VARIANT);

--- CREATE AN INTERNAL STAGE @MY_INT_STAGE 

--- PUT THE FILE ON THE INTERNAL STAGE @my_int_stage
--- DID THIS PART THROUGH SNOWSQL PUT file://C:\Users\x\Documents\employees.parquet @my_int_stage


COPY INTO FROSTY_FRIDAYS.PUBLIC.W2 
FROM @my_int_stage file_format = (type = 'PARQUET') ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;

--- create a parced table
create
    or replace table w2_2 as
select
    w.$1:city::varchar as city,
    w.$1:country::varchar as country,
    w.$1:country_code::varchar as country_code,
    w.$1:dept::varchar as dept,
    w.$1:education::varchar as education,
    w.$1:email::varchar as email,
    w.$1:employee_id::varchar as employee_id,
    w.$1:first_name::varchar as first_name,
    w.$1:job_title::varchar as job_title,
    w.$1:last_name::varchar as last_name,
    w.$1:payroll_iban::varchar as payroll_iban,
    w.$1:postcode::varchar as postcode,
    w.$1:street_name::varchar as street_name,
    w.$1:street_num::varchar as street_num,
    w.$1:time_zone::varchar as time_zone,
    w.$1:title::varchar as title
from
    W2 w;
    
select *  from w2_2;

--- CREATE A VIEW WITH SELECTED COLUMNS
create or replace view w2_view as 
    select dept,
           employee_id,
           job_title
    from w2_2;

--- CREATE A STREAM BASED ON THE VIEW WITH SELECTED COLUMNS
create or replace stream stream_1 on view w2_view;


-- APPLY REQUIRED CHANGED
UPDATE w2_2 SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;
UPDATE w2_2 SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;
UPDATE w2_2 SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;
UPDATE w2_2 SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;
UPDATE w2_2 SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;

--- CHECK YOUR STREAM
SELECT * FROM stream_1;
