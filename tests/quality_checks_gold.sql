/*===============================================================
QUALITY CHECK of GOLD LAYER

- This layer checks the consistency and integrity of data
- Check for uniqueness of surrogate key in dimension tables
- Check for referential uniquness between fact and dimension table
- Validation of relationship for data analytics and reporting

=================================================================*/
USE DatawarehouseNew;
-- ================================================================
-- Checking gold.dim_users
-- ================================================================
SELECT user_key, COUNT(*) FROM gold.dim_users
GROUP BY user_key
HAVING COUNT(*) > 1 OR user_key IS NULL;

-- ================================================================
-- Checking gold.dim_movies
-- ================================================================
SELECT movie_key, COUNT(*) FROM gold.dim_movies
GROUP BY movie_key
HAVING COUNT(*) > 1 OR movie_key IS NULL;

-- ================================================================
-- Checking gold.fact_subscription
-- ================================================================
SELECT * FROM gold.dim_users d
LEFT JOIN gold.fact_subscription s
ON d.user_key = s.user_key

-- ================================================================
-- Checking gold.fact_search_watch_history
-- ================================================================
SELECT * FROM gold.dim_users d
JOIN gold.fact_search_watch_history w
ON d.user_id = w.user_id
JOIN gold.dim_movies m
ON w.movie_id = w.movie_id;

-- ================================================================
-- Checking gold.fact_movie_review
-- ================================================================
SELECT * FROM gold.dim_users d
JOIN gold.fact_movie_review r
ON r.user_key = d.user_key
JOIN gold.dim_movies m
ON m.title = r.movie_title;


