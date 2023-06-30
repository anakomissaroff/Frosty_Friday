--- JSON parsing in snowflake

--- create stage
create or replace stage spanish_monarchs
    url = 's3://frostyfridaychallenges/challenge_4/';

--- check what we have in the stage
list @spanish_monarchs;

--- create a file format
create or replace file format json_ff
    type = json
    strip_outer_array = TRUE;

--- create a new table for the file from the stage
create or replace table monarchs ("raw" VARIANT);

--- copy into the table the file we have in the stage
copy into monarchs
from @spanish_monarchs
file_format = json_ff; 


--- create the final table
create or replace table week4_solution as (
select 
        row_number()
            over(order by m.value:"Birth"::string) as ID,
        row_number() 
            over(partition by houses.value:"House"::string 
                order by m.value:"Birth"::string) as INTER_HOUSE_ID,
        "raw":Era::string as Era,
        houses.value:"House"::string as HOUSE,
        m.value:"Name"::string as NAME,
        m.value:"Nickname"[0]::string as NICKNAME_1,
        m.value:"Nickname"[1]::string as NICKNAME_2,
        m.value:"Nickname"[2]::string as NICKNAME_3,
        m.value:"Birth"::date as BIRTH,
        m.value:"Place of Birth"::string as PLACE_OF_BIRTH,
        m.value:"Start of Reign"::date as START_OF_REIGN,
        m.value:"Consort\/Queen Consort"[0]::string as QUEEN_OR_QUEEN_CONSORT_1,
        m.value:"Consort\/Queen Consort"[1]::string as QUEEN_OR_QUEEN_CONSORT_2,
        m.value:"Consort\/Queen Consort"[2]::string as QUEEN_OR_QUEEN_CONSORT_3,
        m.value:"End of Reign"::date as END_OF_REIGN,
        m.value:"Duration"::string as DURATION,
        m.value:"Death"::date as DEATH,
        m.value:"Age at Time of Death"::string as AGE_AT_TIME_OF_DEATH_YEARS,
        m.value:"Place of Death"::string as PLACE_OF_DEATH,
        m.value:"Place of Death"::string as BURIAL_PLACE
        from monarchs,
lateral flatten (input => "raw":Houses ) houses,
lateral flatten (input => houses.value:Monarchs) m
);

--- check your results
SELECT * FROM WEEK4_SOLUTION;
