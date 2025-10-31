-- DATA CLEANING
-- Checking NULLS and duplicate counts
-- Data inconsistencies
-- Unwanted spaces
-- Out of Range dates
USE DatawarehouseNew;

-- Table 1
SELECT * FROM  bronze.crm_users;

-- Check for Nulls and Duplicate for primary keys
SELECT user_id, COUNT(*) FROM bronze.crm_users
GROUP BY user_id
HAVING COUNT(*)>1 OR user_id IS NULL;

-- Deleting duplicate rows
WITH duplicate_rows AS(
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY user_id) AS row_num
FROM bronze.crm_users
)
DELETE FROM duplicate_rows
WHERE row_num > 1;

-- Data Inconsistencies
SELECT DISTINCT country FROM bronze.crm_users;
SELECT DISTINCT subscription_plan FROM bronze.crm_users;
SELECT DISTINCT primary_device FROM bronze.crm_users;

-- Unwanted Spaces
SELECT first_name, last_name, age, gender FROM bronze.crm_users;

SELECT * FROM silver.crm_users;

-- ============================================================================================================
-- Table 2
SELECT * FROM  bronze.crm_movies;

-- Check for Nulls and Duplicate for primary keys
SELECT movie_id, COUNT(*) FROM bronze.crm_movies
GROUP BY movie_id
HAVING COUNT(*)>1 OR movie_id IS NULL;

-- Deleting duplicate rows
WITH duplicate_rows AS(
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY movie_id ORDER BY movie_id) AS row_num
FROM bronze.crm_movies
)
DELETE FROM duplicate_rows
WHERE row_num > 1;

-- Data Inconsistencies
SELECT DISTINCT country_of_origin FROM bronze.crm_movies;
SELECT DISTINCT genre_primary FROM bronze.crm_movies;
SELECT DISTINCT language FROM bronze.crm_movies;

-- Unwanted Spaces
SELECT title, genre_primary, language, country_of_origin  FROM bronze.crm_movies;

SELECT * FROM silver.crm_movies;

-- ============================================================================================================
-- Table 3
SELECT * FROM  bronze.erp_recommendation_logs;

-- Check for Nulls and Duplicate for primary keys
SELECT user_id , movie_id, COUNT(*) FROM bronze.erp_recommendation_logs
GROUP BY user_id, movie_id
HAVING COUNT(*)>1 OR movie_id IS NULL OR user_id IS NULL;

-- Deleting duplicate rows
WITH duplicate_rows AS(
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY user_id, movie_id ORDER BY user_id, movie_id) AS row_num
FROM bronze.erp_recommendation_logs
)
DELETE FROM duplicate_rows
WHERE row_num > 1;

-- Data Inconsistencies
SELECT DISTINCT recommendation_type FROM bronze.erp_recommendation_logs;

-- Unwanted Spaces
SELECT position_in_list FROM bronze.erp_recommendation_logs;

-- Out of range Date
SELECT CONVERT(DATE, recommendation_date) AS rec_date FROM bronze.erp_recommendation_logs;

SELECT * FROM silver.erp_recommendation_logs;

-- ============================================================================================================
-- Table 4
SELECT * FROM  bronze.erp_reviews;

-- Check for Nulls and Duplicate for primary keys
SELECT user_id, movie_id, COUNT(*) FROM bronze.erp_reviews
GROUP BY user_id, movie_id
HAVING COUNT(*)>1 OR movie_id IS NULL OR user_id IS NULL;

-- Deleting duplicate rows
WITH duplicate_rows AS(
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY user_id, movie_id ORDER BY user_id, movie_id) AS row_num
FROM bronze.erp_reviews
)
DELETE FROM duplicate_rows
WHERE row_num > 1;

-- Data Inconsistencies
SELECT DISTINCT device_type FROM bronze.erp_reviews;

-- Unwanted Spaces
SELECT sentiment_score FROM bronze.erp_reviews;

-- Out of range Date
SELECT CONVERT(DATE, review_date) AS rec_date FROM bronze.erp_reviews;

SELECT * FROM silver.erp_reviews;

-- ============================================================================================================
-- Table 5
SELECT * FROM  bronze.erp_search_logs;

-- Data Inconsistencies
SELECT DISTINCT search_query FROM bronze.erp_search_logs;

-- Out of range Date
SELECT search_date, COUNT(*) FROM bronze.erp_search_logs 
GROUP BY search_date
HAVING search_date < 1900-01-01 OR search_date > GETDATE() OR COUNT(*) > 1;

WITH duplicate_rows AS(
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY search_date ORDER BY search_date) AS row_num
FROM bronze.erp_search_logs
)
DELETE FROM duplicate_rows
WHERE search_date < 1900-01-01 OR search_date > GETDATE();

SELECT * FROM silver.erp_reviews;

-- ============================================================================================================
-- Table 6
-- Check for Nulls and Duplicate for primary keys
SELECT user_id, movie_id, COUNT(*) FROM bronze.erp_watch_history
GROUP BY user_id, movie_id
HAVING COUNT(*)>1 OR movie_id IS NULL OR user_id IS NULL;

-- Deleting duplicate rows
WITH duplicate_rows AS(
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY user_id, movie_id ORDER BY user_id, movie_id) AS row_num
FROM bronze.erp_watch_history
)
DELETE FROM duplicate_rows
WHERE row_num > 1;

-- Data Inconsistencies
SELECT DISTINCT quality FROM bronze.erp_watch_history;
SELECT DISTINCT action FROM bronze.erp_watch_history;

-- Unwanted Spaces
SELECT progress_percentage FROM bronze.erp_watch_history;

-- Out of range Date
SELECT CONVERT(DATE, watch_date) AS watch_date FROM bronze.erp_reviews;

SELECT watch_date FROM bronze.erp_watch_history
WHERE watch_date < 1900-01-01 OR watch_date > GETDATE();

WITH duplicate_rows AS(
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY watch_date ORDER BY watch_date) AS row_num
FROM bronze.erp_watch_history
)
DELETE FROM duplicate_rows
WHERE watch_date < 1900-01-01 OR watch_date > GETDATE();

SELECT * FROM silver.erp_reviews;