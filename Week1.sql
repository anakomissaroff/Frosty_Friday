-- create the new database and set it to use
CREATE DATABASE FROSTY_FRIDAYS;
USE DATABASE FROSTY_FRIDAYS;

-- create a file format for csv file
create or replace file format FROSTY_FRIDAYS.PUBLIC.FF_CSV
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_DELIMITER = ',';

-- create an external named stage
create or replace stage frosty_fridays.public.s3_bucket_external
 file_format = FF_CSV
 url = 's3://frostyfridaychallenges/challenge_1/';

-- list all the objects we have in this stage
list @s3_bucket_external;

-- check the metadata of the staged files
SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
FROM @s3_bucket_external;

-- create a table in which we will upload our file
CREATE OR REPLACE TABLE FROSTY_FRIDAYS.PUBLIC.FF1
  (value VARCHAR);


-- bulk load into targeted table
copy into FF1
from @s3_bucket_external
pattern='.*[1-3].csv',
on_error = 'skip_file';

-- check your result
SELECT * FROM FF1;
