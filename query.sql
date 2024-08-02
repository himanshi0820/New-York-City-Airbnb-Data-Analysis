create database portfolioProject;

use portfolioProject;

create table airbnb_listings(
id int,
name varchar(255),
host_id int,
host_name varchar(255),
neighbourhood_group varchar(255),
neighbourhood varchar(255),
latitude decimal(10,8),
longitude decimal(11,8),
room_type varchar(50),
price int,
minimum_nights int,
number_of_reviews int,
last_review date,
reviews_per_month decimal(5,2),
calculated_host_listings_count int,
availability_365 int
);

#drop table airbnb_listings;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\AB_NYC_2019.csv'
INTO TABLE airbnb_listings
FIELDS TERMINATED BY ','
ENCLOSED by '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@id, @name, @host_id, @host_name, @neighbourhood_group, @neighbourhood, @latitude, @longitude, @room_type, @price, @minimum_nights, @number_of_reviews, @last_review, @reviews_per_month, @calculated_host_listings_count, @availability_365)
SET
	id = nullif(@id, ''),
	name = nullif(@name, ''),
    host_id = nullif(@host_id, ''),
    host_name = nullif(@host_name, ''),
    neighbourhood_group = nullif(@neighbourhood_group, ''),
    neighbourhood = NULLIF(@neighbourhood, ''),
    latitude = NULLIF(@latitude, ''),
    longitude = NULLIF(@longitude, ''),
    room_type = NULLIF(@room_type, ''),
	price =NULLIF(@price, ''),
    minimum_nights = NULLIF(@minimum_nights, ''),
	number_of_reviews = NULLIF(@number_of_reviews, ''),
    last_review = CASE
		WHEN @last_review = '' THEN NULL
        WHEN @last_review REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(@last_review, '%d-%m-%Y')
		WHEN @last_review REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(@last_review, '%Y-%m-%d')
		ELSE NULL
	END,
    reviews_per_month = NULLIF(@reviews_per_month, ''),
    calculated_host_listings_count = NULLIF(@calculated_host_listings_count, ''),
	availability_365 = NULLIF(@availability_365, '');
    
#SHOW VARIABLES LIKE 'secure_file_priv';
    
select * from airbnb_listings;
  
Delete from airbnb_listings
where id in (
	SELECT id FROM (
		SELECT
			id,
			ROW_NUMBER() OVER (PARTITION BY id, name, host_id, host_name, neighbourhood_group, neighbourhood, latitude, longitude, room_type, price, minimum_nights, number_of_reviews, last_review, reviews_per_month, calculated_host_listings_count, availability_365) as row_num
        FROM airbnb_listings
) as subquery
where row_num > 1
);

delete from airbnb_listings where host_name is null;

update airbnb_listings set last_review = '2000-01-01' where last_review is null;

create temporary table mean_reviews as select avg(reviews_per_month) as mean_reviews_per_month from airbnb_listings
where reviews_per_month is not null;

update airbnb_listings set reviews_per_month = (select mean_reviews_per_month from mean_reviews)
where reviews_per_month is null;

alter table airbnb_listings
add column last_review_null int default 0,
add column reviews_per_month_null int default 0;

update airbnb_listings set last_review_null = 1
where last_review = '2000-01-01';

update airbnb_listings set reviews_per_month_null = 1
where reviews_per_month = (select mean_reviews_per_month from mean_reviews);

# What can we learn about different hosts and areas?

# about hosts
select host_name,
count(id) as listings_count,
round(avg(number_of_reviews),2) as avg_reviews,
round(avg(reviews_per_month),2) as avg_reviews_monthly
from airbnb_listings group by host_name order by count(*) desc;

# about areas
select neighbourhood_group, neighbourhood,
count(id) as listings_count,
round(avg(price),3) as avg_price,
round(avg(number_of_reviews),3) as avg_reviews
from airbnb_listings 
group by neighbourhood_group, neighbourhood 
order by count(*) desc, neighbourhood_group desc;

# What is the distribution of the rooms?
select room_type, count(*) as room_count
from airbnb_listings group by room_type
order by room_count desc;

# Which hosts are the busiest and why?
select host_name, round(avg(reviews_per_month),2) as avg_reviews_per_month
from airbnb_listings group by host_name
order by avg_reviews_per_month desc;

# Is there any noticeable difference of traffic among different areas and what could be the reason for it?
select neighbourhood, round(avg(reviews_per_month),2) as avg_reviews
from airbnb_listings group by neighbourhood
order by avg_reviews desc;