/*
========================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
========================================================================
Script Purpose:
	This stored procedure performs ETL process to populate the 'Silver' 
  schema tables from the 'bronze' schema tables.
It perfoms the following action:
  - Truncate the Silver table before loading data.
  - Inserts transformed and clean data from 'Bronze' into 'Silver' layer.

Parameters:
  None.
this stored procedure does not accept any parameters or return any value.

Usage Example:
  EXEC silver.load_bronze;
========================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '====================================================';
		PRINT 'Loading Silver Layer';
		PRINT '====================================================';

		PRINT '----------------------------------------------------';
		PRINT 'Loading CRM Table'
		PRINT '----------------------------------------------------';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating table silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting data into silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)


		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_firstname,
		CASE  UPPER(TRIM(cst_marital_status))
			  WHEN 'M' THEN 'married'
			  WHEN 'S' THEN 'single'
			  ELSE 'n/a'
		END  cst_marital_status, --- NORMALISE MARITAL STATUS VALUES TO READABLE FORMAT
		CASE  UPPER(TRIM(cst_gndr))
			  WHEN 'F' THEN 'female'
			  WHEN 'M' THEN 'male'
			  ELSE 'n/a'
		END cst_gndr, --- NORMALISE GENDER VALUES TO READABLE FORMAT
		cst_create_date
		FROM (SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1 --- SELECT THE MOST RECENT RECORD PER CUSTOMER
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------'

				-- Loading silver.crm_prd_info 
		SET @start_time = GETDATE();
		PRINT '>> Truncating table silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting data into silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, --- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, --- Extract product key
			prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'n/a'
			END prd_line, --- Change codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE
			) AS prd_end_dt --- Calculating end date as 1 day b4 the next start date
		From bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------'

		-- Loading silver.crm_sales_details 
		SET @start_time = GETDATE();
		PRINT '>> Truncating silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting data into silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE WHEN sls_sales is NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				 THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				 THEN sls_sales / NULLIF(sls_quantity, 0)
				 ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------'

		-- Loading silver.erp_cust_az12s 
		SET @start_time = GETDATE();
		PRINT '>> Truncating silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting data into silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			CID,
			BDATE,
			GEN)
		SELECT
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
				 ELSE CID
			END AS CID,
			CASE WHEN BDATE > GETDATE() THEN NULL
				 ELSE BDATE
			END AS BDATE,
			CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'FEMALE'
				 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE')   THEN 'MALE'
				 ELSE 'N/A'
			END AS GEN
		FROM bronze.erp_cust_az12 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------'

		-- Loading silver.erp_loc_a101 
		SET @start_time = GETDATE();
		PRINT '>> Truncating silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting data into silver.erp_loc_a10';
		INSERT INTO silver.erp_loc_a101(
			 CID,
			 CNTRY)
		SELECT
			REPLACE(CID, '-', ''),
			CASE WHEN TRIM(CNTRY) = 'DE'           THEN 'GERMANY'
				 WHEN TRIM(CNTRY) IN ('US', 'USA')      THEN 'UNITED STATES'
				 WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'N/A'
				 ELSE TRIM(CNTRY)
			END AS CNTRY
		FROM bronze.erp_loc_a101 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------'

		-- Loading silver.erp_px_cat_g1v2 
		SET @start_time = GETDATE();
		PRINT '>> Truncating silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting data into silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE)
		SELECT
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------'

		SET @batch_end_time = GETDATE();
		PRINT '=================================='
		PRINT 'Loading Silver Layer';
		PRINT '  - Total loading Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '=================================='

	END TRY
	BEGIN CATCH
		PRINT '==================================='
		PRINT 'Error Occured During Loading Silver Layer'
		PRINT 'Error Massage' + ERROR_MESSAGE();
		PRINT 'Error Massage' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Massage' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==================================='
	END CATCH
END
