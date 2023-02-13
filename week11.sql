-- Set the database and schema
use database frosty_fridays;
use schema public;

create or replace file format csv
    type = csv
    skip_header = 1;

-- Create the stage that points at the data.
create or replace stage week_11_frosty_stage
    url = 's3://frostyfridaychallenges/challenge_11/'
    file_format = csv;


select  from @week_11_frosty_stage;

truncate frosty_fridays.public.week11;

-- Create the table as a CTAS statement.
create or replace table frosty_fridays.public.week11 as
select m.$1 as milking_datetime,
        m.$2 as cow_number,
        m.$3 as fat_percentage,
        m.$4 as farm_code,
        m.$5 as centrifuge_start_time,
        m.$6 as centrifuge_end_time,
        m.$7 as centrifuge_kwph,
        m.$8 as centrifuge_electricity_used,
        m.$9 as centrifuge_processing_time,
        m.$10 as task_used
from @week_11_frosty_stage (pattern => '.*milk_data.*[.]csv') m;

select * from week11;

-- TASK 1: Remove all the centrifuge dates and centrifuge kwph and replace them with NULLs WHERE fat = 3. 
-- Add note to task_used.
create or replace task whole_milk_updates
    schedule = '1 minute'
as
    update frosty_fridays.public.week11
    SET centrifuge_start_time = NULL, 
        centrifuge_end_time = NULL, 
        centrifuge_kwph = NULL, 
        centrifuge_electricity_used = NULL, 
        centrifuge_processing_time = NULL,
        task_used = system$current_user_task_name() || ' at ' || current_timestamp
    where fat_percentage = 3;


-- TASK 2: Calculate centrifuge processing time (difference between start and end time) WHERE fat != 3. 
-- Add note to task_used.
create or replace task skim_milk_updates
    after frosty_fridays.public.whole_milk_updates
as
    update week11
    set centrifuge_processing_time = (DATEDIFF('minutes',centrifuge_start_time, centrifuge_end_time)),
        centrifuge_electricity_used = (datediff(minute, centrifuge_start_time, centrifuge_end_time) / 60) * centrifuge_kwph,
        task_used = system$current_user_task_name() || ' at ' || current_timestamp
    where fat_percentage !=3;

-- alter task 
alter task skim_milk_updates resume;
    
-- Manually execute the task.
execute task whole_milk_updates;

-- Check that the data looks as it should.
select * from week11;

-- Check that the numbers are correct.
select task_used, count(*) as row_count from week11 group by task_used;
