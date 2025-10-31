/*----------------------------------------
CREATE DDL SCRIPT BRONZE TABLE
------------------------------------------
Script Purpose : This script creates Tables for Bronze layer.
The data is inserted into tables using bulk insert query.

Warning : Please keep backup of data as the script will truncate data before inserting 

*/

USE DataWarehouseNew;

IF OBJECT_ID('bronze.crm_users', 'U') IS NOT NULL
	DROP TABLE bronze.crm_users;
GO

CREATE TABLE bronze.crm_users (
	user_id NVARCHAR(50),
	first_name NVARCHAR(50),
	last_name NVARCHAR(50),
	age INT,
	gender NVARCHAR(50),
	country NVARCHAR(50),
	subscription_plan NVARCHAR(50),
	subscription_start_date DATETIME,
	is_active BIT,
	monthly_spend FLOAT,
	primary_device NVARCHAR(50)
);

IF OBJECT_ID('bronze.crm_movies', 'U') IS NOT NULL
	DROP TABLE bronze.crm_movies;
GO

CREATE TABLE bronze.crm_movies (
	movie_id NVARCHAR(50),
	title NVARCHAR(50),
	genre_primary NVARCHAR(50),
	release_year INT,
	duration_minutes INT,
	language NVARCHAR(50),
	country_of_origin NVARCHAR(50),
	imdb_rating FLOAT,
	number_of_episodes INT,
	added_to_platform DATETIME
);

IF OBJECT_ID('bronze.erp_recommendation_logs', 'U') IS NOT NULL
	DROP TABLE bronze.erp_recommendation_logs;
GO

CREATE TABLE bronze.erp_recommendation_logs(
	user_id NVARCHAR(50),
	movie_id  NVARCHAR(50),
	recommendation_date DATETIME,
	recommendation_type NVARCHAR(50),
	recommendation_score FLOAT,
	position_in_list INT
);

IF OBJECT_ID('bronze.erp_reviews', 'U') IS NOT NULL
	DROP TABLE bronze.erp_reviews;
GO

CREATE TABLE bronze.erp_reviews (
	user_id NVARCHAR(50),
	movie_id NVARCHAR(50),
	rating FLOAT,
	review_date DATETIME,
	device_type NVARCHAR(50),
	is_verified_watch BIT,
	sentiment_score FLOAT
);

IF OBJECT_ID('bronze.erp_search_logs', 'U') IS NOT NULL
	DROP TABLE bronze.erp_search_logs;
GO

CREATE TABLE bronze.erp_search_logs (
	user_id NVARCHAR(10),
	search_query NVARCHAR(50),
	search_date DATETIME,
	results_returned INT,
	used_filters BIT
);

IF OBJECT_ID('bronze.erp_watch_history','U') IS NOT NULL
	DROP TABLE bronze.erp_watch_history;
GO

CREATE TABLE bronze.erp_watch_history
(
	user_id NVARCHAR(10),
	movie_id NVARCHAR(10),
	watch_date DATETIME,
	watch_duration_minutes FLOAT,
	progress_percentage FLOAT,
	action NVARCHAR(50),
	quality NVARCHAR(10)
);

BULK INSERT bronze.crm_users
			FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\crm\crm_users.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);

BULK INSERT bronze.crm_movies
			FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\crm\crm_movies.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);

BULK INSERT bronze.erp_recommendation_logs
			FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\erp\erp_recommendation_logs.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);

BULK INSERT bronze.erp_reviews
			FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\erp\erp_reviews.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);

BULK INSERT bronze.erp_search_logs
		FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\erp\erp_search_logs.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);

BULK INSERT bronze.erp_watch_history
		FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\erp\erp_watch_history.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);
