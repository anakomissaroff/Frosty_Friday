--- Geospatial application of a Snowflake

CREATE OR REPLACE TABLE constituency_points
	(
    constituency varchar,
    sequence_num integer,
    longitude float,
    latitude float,
    part integer
    );


CREATE OR REPLACE TABLE nations_regions
	(
    nation_or_region_name STRING,
    type STRING,
    sequence_num INTEGER,
    longitude float,
    latitude float,
    part INTEGER
    );

---create internal stages, the files were uploaded with SNOWSql
create or replace stage my_int_stage_2;


CREATE or replace FILE FORMAT CSV_SKIP
	TYPE = CSV
	FIELD_DELIMITER = ','
	RECORD_DELIMITER = '\n'
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '"';
   

COPY INTO constituency_points
	FROM @my_csv_stage_2
    FILE_FORMAT = 'CSV_SKIP';


COPY INTO nations_regions
	FROM @my_csv_stage
    FILE_FORMAT = 'CSV';


--- validate in case you have some erroes while uploading the file
SELECT * FROM TABLE(VALIDATE(CONSTITUENCY_POINTS, JOB_ID => '01aa582c-3200-a6a2-0001-f01e000ca346'));


SELECT * FROM nations_regions;
SELECT * FROM constituency_points;


ALTER SESSION SET geography_output_format = 'WKT';


CREATE OR REPLACE TABLE nations_regions_pols AS (
WITH pts AS (
  SELECT 
NATION_OR_REGION_NAME, type, sequence_num, longitude, latitude, part,
    ST_MAKEPOINT(longitude, latitude) as geo_pts
  FROM nations_regions
),
pts_0 AS (
    SELECT NATION_OR_REGION_NAME, type, sequence_num, longitude, latitude, part, 
        ST_MAKEPOINT(longitude, latitude) as geo_pts_0
    FROM nations_regions
    WHERE sequence_num = 0
    ),
collect_pts AS (
    SELECT NATION_OR_REGION_NAME, type, part, 
        ARRAY_AGG(sequence_num) as seq,
        ST_COLLECT(geo_pts) as collection_pts
    FROM pts
    WHERE sequence_num != 0
    GROUP BY nation_or_region_name, type, part
    ),
lines_tbl AS (
    SELECT cp.nation_or_region_name, cp.type, cp.part, 
        cp.seq, cp.collection_pts,
        pz.geo_pts_0
    FROM collect_pts cp
    LEFT JOIN pts_0 pz ON 
    		cp.nation_or_region_name = pz.nation_or_region_name
        AND cp.type = pz.type
        AND cp.part = pz.part 
    ),
lines AS (
    SELECT nation_or_region_name, type, part, seq,
        ST_MAKELINE(geo_pts_0, collection_pts) as geo_lines
    FROM lines_tbl
    ),
pols AS (
	SELECT nation_or_region_name, type, part, seq, 
    	ST_MAKEPOLYGON(geo_lines) as part_pols
FROM lines
    )
SELECT nation_or_region_name, type, ST_COLLECT(part_pols) as all_pols
FROM pols
GROUP BY nation_or_region_name, type
    );



CREATE OR REPLACE TABLE west_const_pols AS (
WITH pts AS (
    SELECT constituency, sequence_num, longitude, latitude, part,
      ST_MAKEPOINT(longitude, latitude) as geo_pts
    FROM constituency_points
),
pts_0 AS (
    SELECT constituency, sequence_num, longitude, latitude, part, 
      ST_MAKEPOINT(longitude, latitude) as geo_pts_0
    FROM constituency_points
    WHERE sequence_num = 0
    ),
collect_pts AS (
    SELECT constituency, part, 
        ARRAY_AGG(sequence_num) as seq,
        ST_COLLECT(geo_pts) as collection_pts
    FROM pts
    WHERE sequence_num != 0
    GROUP BY constituency, part
    ),
lines_tbl AS (
    SELECT cp.constituency, cp.part, 
        cp.seq, cp.collection_pts,
        pz.geo_pts_0
    FROM collect_pts cp
    LEFT JOIN pts_0 pz ON cp.constituency = pz.constituency
        AND cp.part = pz.part 
    ),
lines AS (
    SELECT constituency, part, seq,
        ST_MAKELINE(geo_pts_0, collection_pts) as geo_lines
    FROM lines_tbl
    ),
pols AS (
SELECT constituency, part, seq, 
    ST_MAKEPOLYGON(geo_lines) as part_pols
FROM lines
    )

SELECT constituency, ST_COLLECT(part_pols) as all_pols
FROM pols
GROUP BY constituency
    );

SELECT * FROM WEST_CONST_POLS;
SELECT * FROM nations_regions_pols;


CREATE OR REPLACE VIEW product AS
	SELECT np.nation_or_region_name, COUNT(wp.constituency) as cnt_const
	FROM nations_regions_pols np
	LEFT JOIN west_const_pols wp ON ST_INTERSECTS(np.all_pols, wp.all_pols)
	GROUP BY 1
	ORDER BY 2 DESC;

// Checking the results
SELECT * FROM product; 
