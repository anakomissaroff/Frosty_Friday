--- CREATE A FILE FORMAT FOR A FUTURE JSON FILE
create or replace file format json_ff
    type = json
    strip_outer_array = TRUE;
    
--- CREATE A STAGE
create or replace stage week_16_frosty_stage
    url = 's3://frostyfridaychallenges/challenge_16/'
    file_format = json_ff;

--- CREATE A TABLE INTO WHICH YOU'LL UPLOAD THE FILES FROM THE STAGE
create or replace table week16 as
select t.$1:word::text word, t.$1:url::text url, t.$1:definition::variant definition  
from @week_16_frosty_stage (file_format => 'json_ff', pattern=>'.*week16.*') t;

-- EXPLORE WHAT YOU HAVE IN THE TABLE
select * from week16;

-- CREATE A SOLUTION TABLE
CREATE OR REPLACE TABLE W16_SOLUTION AS 
 select 
    WORD,
    URL,
    meanings.value['partOfSpeech']::string as PART_OF_SPEECH,
    meanings.value['synonyms']::string as GENERAL_SYNONYMS,
    meanings.value['antonyms']::string as GENERAL_ANTONYMS,
    definitions.value['definition']::string as DEFINITION,
    definitions.value['example']::string as EXAMPLE_IF_APPLICABLE,
    definitions.value['antonyms']::string as DEFINITIONAL_SYNONYMS,
    definitions.value['synonyms']::string as DEFINITIONAL_ANTONYMS
from week16,
    LATERAL FLATTEN(DEFINITION, OUTER => TRUE, MODE => 'ARRAY') as list_member,
    LATERAL flatten(input=>list_member.value:meanings, OUTER => TRUE, MODE => 'ARRAY') meanings,
    LATERAL flatten (input=>meanings.value:definitions, OUTER => TRUE, MODE => 'ARRAY') definitions;
    

-- TEST YOUR SOLUTION
select
    count(word)
  , count(distinct word)
from W16_SOLUTION;
