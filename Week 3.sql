-- Use Frosty_friday database
USE DATABASE FROSTY_FRIDAYS;

-- create a file format for csv file
create or replace file format FROSTY_FRIDAYS.PUBLIC.FF_CSV
    TYPE = 'CSV'
    SKIP_HEADER = 1 
    FIELD_DELIMITER = ',';

-- create an external named stage
create or replace stage frosty_fridays.public.FF_W3
 url = 's3://frostyfridaychallenges/challenge_3';
 
--create an external stage just for KEYWORDS table
create or replace stage frosty_fridays.public.FF_W3_STAGE_keywords
 url = 's3://frostyfridaychallenges/challenge_3/keywords.csv';


--create the table with keywords that we're gonna use later as a filtering condition
create or replace table keywords_table 
    (keywords varchar);
    
--create a new table with search words
copy into keywords_table
from (select t.$1 from @FF_W3_STAGE_keywords t)
FILE_FORMAT = (format_name = FROSTY_FRIDAYS.PUBLIC.FF_CSV)
on_error = 'skip_file';

--create a table for all files in the folder
create or replace table FF3_TABLE
    (FILENAME varchar,
    FILE_ROW_NUMBER varchar);

--copy the metadata into the new table
copy into FF3_TABLE(FILENAME, FILE_ROW_NUMBER)
  from (
      select metadata$filename,metadata$file_row_number
      from @FF_W3 (file_format => FF_CSV)
        );

--create the final table
create or replace table FF3_FINAL_TABLE
    (FILENAME varchar,
    FILE_ROW_NUMBER number);

--populate the final table ONLY with files which contain KEYWORDS in their names
create or replace table FF3_FINAL_TABLE as 
    select FILENAME as FILENAME, MAX(TO_NUMBER(FILE_ROW_NUMBER)) AS NUMBER_OF_ROWS FROM ff3_table, keywords_table
    where contains(FILENAME, keywords_table.KEYWORDS)
    group by FILENAME;
    
--select and check the final reuslt
select * from FF3_FINAL_TABLE;
