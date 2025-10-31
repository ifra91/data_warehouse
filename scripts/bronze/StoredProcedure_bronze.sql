/*-----------------------------------------------------------------------------
STORED PROCEDURE : Load Bronze Layer
-------------------------------------------------------------------------------
-This script inserts data into Bronze schema tables from external CSV files
-The tables are truncated first when script is loaded, it is recommended to
  keep data backup
-Uses BULK INSERT to insert data from external file
-This script does not accept any parameters

USAGE : EXEC bronze.load_bronze
-------------------------------------------------------------------------------*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET	@batch_start_time = GETDATE();
		PRINT '---------------------';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '---------------------';

		PRINT '-----------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '-----------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE bronze.crm_users';
		TRUNCATE TABLE bronze.crm_users;
		BULK INSERT bronze.crm_users
			FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\crm\crm_users.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duartion :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '>> --------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE bronze.crm_movies';
		TRUNCATE TABLE bronze.crm_movies;
		BULK INSERT bronze.crm_movies
			FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\crm\crm_movies.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duartion :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '>> --------------------';

		SET	@batch_start_time = GETDATE();
		
		PRINT '-----------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '-----------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE bronze.erp_recommendation_logs';
		TRUNCATE TABLE bronze.erp_recommendation_logs;
		BULK INSERT bronze.erp_recommendation_logs
			FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\erp\erp_recommendation_logs.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duartion :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '>> --------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE bronze.erp_reviews';
		TRUNCATE TABLE bronze.erp_reviews;
		BULK INSERT bronze.erp_reviews
			FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\erp\erp_reviews.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duartion :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '>> --------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE bronze.erp_search_logs';
		TRUNCATE TABLE bronze.erp_search_logs;
		BULK INSERT bronze.erp_search_logs
		FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\erp\erp_search_logs.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duartion :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '>> --------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE bronze.erp_watch_history';
		TRUNCATE TABLE bronze.erp_watch_history;
		BULK INSERT bronze.erp_watch_history
		FROM 'I:\DA\SQLfiles\dw_Project1\DatesetNetflix\erp\erp_watch_history.csv'
			WITH 
			(
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',' ,
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duartion :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '>> --------------------';

		SET @batch_end_time = GETDATE();
		PRINT '-------------------------------';
		PRINT ' LOADING BRONZE LAYER COMPLETED';
		PRINT '-------------------------------';
		PRINT '>> Total Load Duartion :' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR(50)) + 'seconds';
		PRINT '>> --------------------';
	END TRY
	BEGIN CATCH
		PRINT '----------------------------------------------------';
		PRINT 'ERROR DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR(50));
	END CATCH
END
