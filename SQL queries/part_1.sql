-- Part 1: Data Ingestion

-- Q1. Download files

-- Q2. Upload dataset in Azure

-- Q3. Create database and stage on snowflake
-- 3.1 Create database "assignment_1"
CREATE DATABASE assignment_1;

-- switch to database created
USE DATABASE assignment_1;

-- 3.2 Create a stage “stage_assignment”, pointing to azure storage
CREATE OR REPLACE STAGE stage_assignment
URL='azure://rootx.blob.core.windows.net/bde-assignment'
CREDENTIALS=(AZURE_SAS_TOKEN='?sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2024-12-30T16:57:48Z&st=2024-08-20T09:57:48Z&spr=https&sig=X8z10u5MexpraHr9jRil2EMrIADN9fvqc2pz%2FWwnXNM%3D')
;

-- check files under the stage
list @stage_assignment;


-- 4. Ingest the data as external tables on Snowflake

-- create a file format, ignoring header for CSV file
CREATE OR REPLACE FILE FORMAT file_format_csv
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('\\N', 'NULL', 'NUL', '')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;

-- create external trending table with any file endswith .csv, usin`g the file format above
CREATE OR REPLACE EXTERNAL TABLE ex_table_youtube_trending
WITH LOCATION = @stage_assignment
FILE_FORMAT = file_format_csv
PATTERN = '.*[.]csv'
;

-- create external category table with any file endswith .json
CREATE OR REPLACE EXTERNAL TABLE ex_table_youtube_category
WITH LOCATION = @stage_assignment
FILE_FORMAT = (TYPE=JSON)
PATTERN = '.*[.]json'
;


-- Q5. Transfer external tables into tables, assigning appropriate data type
CREATE OR REPLACE TABLE table_youtube_trending AS
SELECT 
t.value:c1::char(11) as video_id,
t.value:c2::varchar(500) as title,
t.value:c3::date as publishedat,
t.value:c4::char(24) as channelid,
t.value:c5::varchar(500) as channeltitle,
t.value:c6::varchar(2) as categoryid,
t.value:c7::date as trending_date,
t.value:c8::bigint as view_count,
t.value:c9::int as likes,
t.value:c10::int as dislikes,
t.value:c11::int as comment_count,
Split_Part(Split_Part(Metadata$Filename,'/',-1),'_',0)::varchar(2) as country
From ex_table_youtube_trending as t
;

-- check the schema of TABLE_YOUTUBE_TRENDING
DESCRIBE TABLE TABLE_YOUTUBE_TRENDING;


-- Create category table with semi-struture json format files
-- have a look at the struture of json file using jsonviewer (https://codebeautify.org/jsonviewer)
-- handle this nested json using Lateral Flatten
CREATE OR REPLACE TABLE table_youtube_category AS
SELECT 
    Split_Part(Split_Part(Metadata$Filename,'/',-1),'_',0)::varchar(2) as country,
-- access target fields in items_data
    items_data.value:id::varchar(2) as categoryid,
    items_data.value:snippet.title::varchar(50) as category_title
From 
    ex_table_youtube_category,
-- locate target json array: value:items
LATERAL FLATTEN(input => value:items) as items_data  
;

-- check the schema of TABLE_YOUTUBE_CATEGORY
DESCRIBE TABLE TABLE_YOUTUBE_CATEGORY;



-- Q6.combining them as final table using join, adding a new field id using “UUID_STRING()” function
CREATE OR REPLACE TABLE table_youtube_final AS
SELECT 
UUID_STRING()::varchar(36) as ID ,
t.VIDEO_ID,
t.TITLE,
t.PUBLISHEDAT,
t.CHANNELID,
t.CHANNELTITLE,
t.CATEGORYID,
c.CATEGORY_TITLE,
t.TRENDING_DATE,
t.VIEW_COUNT,
t.LIKES,
t.DISLIKES,
t.COMMENT_COUNT,
t.COUNTRY
FROM 
    table_youtube_trending as t
LEFT JOIN 
    table_youtube_category as c 
    ON t.CATEGORYID=c.CATEGORYID and t.COUNTRY=c.COUNTRY
;

-- set ID as PRIMARY KEY
ALTER TABLE table_youtube_final ADD PRIMARY KEY (ID);

-- check the schema of TABLE_YOUTUBE_FINAL
DESC TABLE TABLE_YOUTUBE_FINAL;


-- check number of columns of TABLE_YOUTUBE_FINAL
SELECT 
    count(*) 
FROM 
    TABLE_YOUTUBE_FINAL
;