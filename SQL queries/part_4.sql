-- Part4 If you were to launch a new Youtube channel tomorrow, which category (excluding “Music” and “Entertainment”) of video will you be trying to create to have them appear in the top trend of Youtube? Will this strategy work in every country?  -> chance to be top trending

-- Data retrieved: starting in 2023; Each trending video was counted only once, on the first trending day it appeared YouTube top trending videos 
-- Key Metrics: audience interest and reach (total_video) / channel competitiveness (total_channel) / engagement (like_ratio)

-- overall 
With rn_video_data as (
Select 
    *,
    row_number() over(partition by video_id order by trending_date) as rn
from 
    table_youtube_final
Where  
    LEFT(TRENDING_DATE,4) > '2022'
)
Select
    category_title,
    count(distinct video_id) as total_viedo,
    count(distinct channelid) as total_channel,
    round(100 * sum(likes) / sum(view_count), 2) as ratio_like
from 
    rn_video_data
Where
    rn = 1
GROUP BY 
    1
;


-- considering different countries
WITH rn_video_data as (
Select 
    *,
    row_number() over(partition by video_id,country order by trending_date) as rn
from table_youtube_final
Where  
    LEFT(TRENDING_DATE,4) > '2022'
), rn_count_data as(
Select
    category_title, country,
    row_number() over(partition by country order by count(distinct video_id) desc) as rn_video,
    row_number() over(partition by country order by count(distinct channelid) desc) as rn_channel,
    row_number() over(partition by country order by (sum(likes) / sum(view_count)) desc) as rn_like_ratio,
    count(distinct video_id) as total_video,
    count(distinct channelid) as total_channel,
    round(100 * sum(likes) / sum(view_count), 2) as ratio_like,
from 
    rn_video_data
Where
    rn = 1 
GROUP BY 
    1,2
)
Select
    *
From
    rn_count_data
;