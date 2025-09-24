/*
========================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
========================================================================
Script Purpose:
	This stored procedure loads data into the 'Bronze' schema from external CSV files.
It perfoms the following action:
  - Truncate the bronze table before loading data.
  - Uses the bulk insert command to load data from the csv files to bronze table.

Parameters:
  None.
this stored procedure does not accept any parameters or return any value.

Usage Example:
  EXEC bronze.load_bronze;
========================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY
		PRINT '=====================================';
		PRINT 'Loading bronze layer';
		PRINT '=====================================';

		PRINT '-------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Triuncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting data into Table: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\sbu_2\Documents\dwh_project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '--------------------------------';

		PRINT '-------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Triuncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting data into Table: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\sbu_2\Documents\dwh_project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '--------------------------------';

		PRINT '-------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Triuncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting data into Table: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\sbu_2\Documents\dwh_project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '--------------------------------';

		PRINT '-------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Triuncating Table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting data into Table: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\sbu_2\Documents\dwh_project\datasets\source_erp\cust_az12.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '--------------------------------';

		PRINT '-------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Triuncating Table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting data into Table: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\sbu_2\Documents\dwh_project\datasets\source_erp\loc_a101.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '--------------------------------';

		PRINT '-------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Triuncating Table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting data into Table: bronze.crm_cust_info'

		PRINT '>> Inserting data into Table: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\sbu_2\Documents\dwh_project\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '--------------------------------';

			SET @batch_end_time = GETDATE();
			PRINT '=================================='
			PRINT 'Loading Bronze is complete'
			PRINT '>> Total Load duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '==================================';
	END TRY
	BEGIN CATCH
	PRINT '=================================='
	PRINT 'ERROR DURING BRONZE LAYER LOAD'
	PRINT 'ERROR msg' + ERROR_MESSAGE();
	PRINT 'ERROR msg' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR msg' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT '=================================='
	END CATCH
END
