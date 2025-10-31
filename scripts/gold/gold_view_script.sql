/*
DDL OF GOLD LAYER 
------------------
This script contains the script for gold views which can be used for 
analysis.
Each view performs transformation using silver layer, the data is refined.
enriched and combined for tables for analysis.
-- These queries can be directly utilized for analytics and reporting
*/

-- ===================================================================
-- CREATE DIMENSION : gold.dim_user 
-- ===================================================================
USE DatawarehouseNew;

IF OBJECT_ID(gold.dim_user, 'V') IS NOT NULL
	DROP TABLE gold.dim_user;

CREATE VIEW gold.dim_users AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY user_id) AS user_key,
	user_id,
	first_name,
	last_name,
	age,
	gender, 
	country
FROM silver.crm_users;

-- ===================================================================
-- CREATE DIMENSION : gold.dim_movies
-- ===================================================================
IF OBJECT_ID(gold.dim_movies, 'V') IS NOT NULL
	DROP TABLE gold.dim_movies;

CREATE VIEW gold.dim_movies AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY movie_id) AS movie_key,
	movie_id,
	title,
	genre,
	country_of_origin,
	release_year,
	duration,
	language,
	date_added
FROM silver.crm_movies;

-- ===================================================================
-- CREATE DIMENSION : gold.fact_subscription
-- ===================================================================
IF OBJECT_ID(gold.fact_subscription, 'V') IS NOT NULL
	DROP TABLE gold.fact_subscription;

CREATE VIEW gold.fact_subscription AS
SELECT
	d.user_key,
	c.subscription,
	c.subscription_date,
	c.Expense,
	c.device
FROM silver.crm_users c
JOIN gold.dim_users d
ON c.user_id = d.user_id;

-- ===================================================================
-- CREATE DIMENSION : gold.fact_movie_review
-- ===================================================================
IF OBJECT_ID(gold.fact_movie_review, 'V') IS NOT NULL
	DROP TABLE gold.fact_movie_review;

CREATE VIEW gold.fact_movie_review AS
SELECT
	c.user_key,
	c.first_name +' '+ c.last_name AS user_name,
	m.title AS movie_title,
	m.imdb_rating,
	r.recommendation AS customer_recommendation,
	e.rating AS customer_rating,
	e.sentiment,
	e.verified_watch,
	e.review_date
FROM gold.dim_users c
LEFT JOIN silver.erp_recommendation_logs r
ON c.user_id = r.user_id
LEFT JOIN silver.crm_movies m
ON m.movie_id = r.movie_id
LEFT JOIN silver.erp_reviews e
ON e.user_id = r.user_id;

-- ===================================================================
-- CREATE DIMENSION : gold.fact_search
-- ===================================================================
IF OBJECT_ID(gold.fact_search, 'V') IS NOT NULL
	DROP TABLE gold.fact_search;

CREATE VIEW gold.fact_search_watch_history AS
SELECT 
	s.user_id,
	w.movie_id,
	m.title AS movie_title,
	w.watch_hours,
	w.progress_percent,
	w.watch_date,
	s.search_query,
	s.results_returned,
	s.search_date,
	s.used_filters
FROM silver.erp_search_logs s
LEFT JOIN silver.erp_watch_history w
ON s.user_id = w.user_id
LEFT JOIN silver.crm_movies m
ON w.movie_id = m.movie_id;

 
