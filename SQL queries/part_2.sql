-- Part 2: Data Cleaning

-- Q1 In “table_youtube_category” which category_title has duplicates if we don’t take into account the categoryid (return only a single row)?

WITH result as (
SELECT 
    country, category_title, 
    row_number() over(partition by country,category_title order by country) as title_dup
FROM 
    table_youtube_category
)
 SELECT distinct 
    category_title
FROM 
    result
WHERE
    title_dup>1
;


-- Q2 In “table_youtube_category” which category_title only appears in one country?

WITH result as (
SELECT 
    country,category_title,
    count(*) over(partition by category_title) as title_cnts
FROM 
    table_youtube_category
)
SELECT 
    category_title
FROM 
    result
WHERE title_cnts=1
;



-- Q3 In “table_youtube_final”, what is the categoryid of the missing category_titles?

SELECT distinct categoryid
FROM table_youtube_final
WHERE category_title is null
;


-- Q4 Update the table_youtube_final to replace the NULL values in category_title with the answer from the previous question.

UPDATE table_youtube_final
SET category_title = 'Nonprofits & Activism'
WHERE category_title is null
;

-- Q5 In “table_youtube_final”, which video doesn’t have a channeltitle (return only the title)?
SELECT title
FROM table_youtube_final
WHERE channeltitle is null
;


-- Q6 Delete from “table_youtube_final“, any record with video_id = “#NAME?”

DELETE FROM table_youtube_final
WHERE video_id='#NAME?'
;

-- Q7 Create a new table called “table_youtube_duplicates” containing only the “bad” duplicates by using the row_number() function.

CREATE OR REPLACE TABLE table_youtube_duplicates
AS
SELECT *
FROM (SELECT 
        *,
        row_number() over(partition by video_id, country, trending_date order by view_count DESC) as dup_flag
        FROM 
            table_youtube_final
)
WHERE dup_flag>1
;

-- Q8 Delete the duplicates in “table_youtube_final“ by using “table_youtube_duplicates”.

DELETE FROM table_youtube_final as f
WHERE f.id in (
    SELECT id
    FROM table_youtube_duplicates
);

-- Q9 Count the number of rows in “table_youtube_final“ and check that it is equal to 2,597,494 rows.

SELECT 
    count(*) as records
FROM 
    table_youtube_final
;