/*==============================================================
QUALITY CHECKS SILVER LAYER
================================================================
This script  performs various quality checks like consistency, standardization and
accuracy on silver layer tables
- NUll or Duplicate primary key
- Check for out of range dates 
- Data Consistency

Usage 
- Run theses queries after loading silver layer
- Investigate and resolve any data discrepancies
*/
USE DatawarehouseNew;
-- ===============================================
-- silver.crm_users
-- ===============================================
-- Check for Nulls
SELECT user_id, COUNT(*) FROM silver.crm_users
GROUP BY user_id
HAVING COUNT(*) > 1 OR user_id IS NULL; 

-- Unwanted Spaces
SELECT user_id FROM silver.crm_users
WHERE user_id != TRIM(user_id);

-- Data Standardization and Consistency
SELECT DISTINCT gender FROM silver.crm_users;

-- ===============================================
-- silver.crm_movies
-- ===============================================
-- Check for Nulls
SELECT movie_id, COUNT(*) FROM silver.crm_movies
GROUP BY movie_id
HAVING COUNT(*) > 1 OR movie_id IS NULL; 

-- Unwanted Spaces
SELECT movie_id FROM silver.crm_movies
WHERE movie_id != TRIM(movie_id);

-- Data Standardization and Consistency
SELECT imdb_rating FROM silver.crm_movies;

-- Check for date inconsistencies
SELECT date_added FROM silver.crm_movies
WHERE date_added < '1900-01-01' OR date_added > GETDATE();

-- ===============================================
-- silver.erp_recommendation_logs
-- ===============================================
-- Check for Nulls
SELECT user_id, movie_id, COUNT(*) FROM silver.erp_recommendation_logs
GROUP BY user_id, movie_id
HAVING COUNT(*) > 1 OR user_id IS NULL OR movie_id IS NULL; 

-- Unwanted Spaces
SELECT user_id, movie_id FROM silver.erp_recommendation_logs
WHERE user_id != TRIM(user_id) OR movie_id != TRIM(movie_id);

-- Data Standardization and Consistency
SELECT recommendation, recommendation_type FROM silver.erp_recommendation_logs;

-- Check for date inconsistencies
SELECT recommendation_date FROM silver.erp_recommendation_logs
WHERE recommendation_date < '1900-01-01' OR recommendation_date > GETDATE();

WITH duplicate_date AS
(
SELECT *, ROW_NUMBER() OVER(PARTITION BY recommendation_date ORDER BY recommendation_date) as row_num
FROM silver.erp_recommendation_logs
)
DELETE FROM duplicate_date
WHERE recommendation_date < '1900-01-01' OR recommendation_date > GETDATE();

-- ===============================================
-- silver.erp_reviews
-- ===============================================
-- Check for Nulls
SELECT user_id, movie_id, COUNT(*) FROM silver.erp_reviews
GROUP BY user_id, movie_id
HAVING COUNT(*) > 1 OR user_id IS NULL OR movie_id IS NULL; 

-- Unwanted Spaces
SELECT user_id, movie_id FROM silver.erp_reviews
WHERE user_id != TRIM(user_id) OR movie_id != TRIM(movie_id);

-- Data Standardization and Consistency
SELECT sentiment FROM silver.erp_reviews;

-- Check for date inconsistencies
SELECT review_date FROM silver.erp_reviews
WHERE review_date < '1900-01-01' OR review_date > GETDATE();

WITH duplicate_date AS
(
SELECT *, ROW_NUMBER() OVER(PARTITION BY review_date ORDER BY review_date) as row_num
FROM silver.erp_reviews
)
DELETE FROM duplicate_date
WHERE review_date < '1900-01-01' OR review_date > GETDATE();

-- ===============================================
-- silver.erp_search_logs
-- ===============================================
-- Check for Nulls
SELECT user_id FROM silver.erp_search_logs
GROUP BY user_id
HAVING user_id IS NULL; 

-- Unwanted Spaces
SELECT user_id FROM silver.erp_search_logs
WHERE user_id != TRIM(user_id);

-- Data Standardization and Consistency
SELECT search_query FROM silver.erp_search_logs;

-- Check for date inconsistencies
SELECT search_date FROM silver.erp_search_logs
WHERE search_date < '1900-01-01' OR search_date > GETDATE();

-- ===============================================
-- silver.erp_watch_history
-- ===============================================
-- Check for Nulls
SELECT user_id, movie_id, COUNT(*) FROM silver.erp_watch_history
GROUP BY user_id, movie_id
HAVING COUNT(*) > 1 OR user_id IS NULL; 

-- Unwanted Spaces
SELECT user_id FROM silver.erp_watch_history
WHERE user_id != TRIM(user_id);

-- Data Standardization and Consistency
SELECT watch_hours, action, quality FROM silver.erp_watch_history;

-- Check for date inconsistencies
SELECT watch_date FROM silver.erp_watch_history
WHERE watch_date < '1900-01-01' OR watch_date > GETDATE();

