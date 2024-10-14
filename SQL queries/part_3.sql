-- Part 3 Data Analysis

-- Q1 What are the 3 most viewed videos for each country in the Gaming category for the trending_date = '2024-04-01'. Order the result by country and the rank.

WITH result as (
SELECT 
    country, title, channeltitle, view_count,
    row_number() over(partition by country order by view_count DESC) as RK
FROM
    table_youtube_final
WHERE  
    category_title='Gaming' AND trending_date='2024-04-01'
)
SELECT *
FROM 
    result
WHERE 
    RK<4
ORDER BY 
    country, RK
;


-- Q2 For each country, count the number of distinct video with a title containing the word “BTS” (case insensitive) and order the result by count in a descending order,

SELECT 
    country, count(distinct video_id) as CT
FROM
    table_youtube_final
WHERE  
    title like '%BTS%'
  GROUP BY 
    country
ORDER BY 
    CT DESC
;

-- Q3 For each country, year and month (in a single column) and only for the yeare for the 2024, which video is the most viewed and what is its likes_ratio (defined as the percentage of likes against view_count) truncated to 2 decimals. Order the result by year_month and country.

WITH Ranked_data as (
SELECT 
    *,
    CASE WHEN view_count = 0 THEN 0
        ELSE ((likes/view_count)*100)::decimal(32,2) 
        END as likes_ratio,
    row_number() over(partition by country, DATE(CONCAT(LEFT(trending_date::Varchar,7),'-01')) order by view_count DESC) as RK
FROM
    table_youtube_final
WHERE
    LEFT(trending_date::Varchar,4)='2024'
)
SELECT
    country, DATE(CONCAT(LEFT(trending_date::Varchar,7),'-01')) as year_month, title, channeltitle,
    category_title, view_count, likes_ratio
FROM
    Ranked_data
WHERE
    RK=1
ORDER BY 
    year_month, country
;



-- Q4 For each country, which category_title has the most distinct videos and what is its percentage (2 decimals) out of the total distinct number of videos of that country? Only look at the data from 2022. Order the result by category_title and country.

WITH cnt_result as (
SELECT distinct
    country, category_title,
    count(distinct video_id) over(partition by country, categoryid) as total_category_video,
    count(distinct video_id) over(partition by country) as total_country_video
FROM
    table_youtube_final
WHERE 
    LEFT(trending_date::varchar,4)>='2022' 
), ranked_result as (
SELECT
    *,
    row_number() over(partition by country order by total_category_video DESC) as rn
FROM
    cnt_result
)
SELECT 
    country, category_title, total_category_video, total_country_video,
    ((total_category_video / total_country_video) * 100)::decimal(32,2) as percentage
FROM
    ranked_result
WHERE
    rn=1
ORDER BY
    category_title, country
;


-- Q5 Which channeltitle has produced the most distinct videos and what is this number? 

SELECT 
    channeltitle,
    count(distinct video_id) as total_video
FROM
    table_youtube_final
GROUP BY 
    channeltitle
ORDER BY 
    total_video DESC
LIMIT 1
;