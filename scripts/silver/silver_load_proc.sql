/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from bronze schema. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Insert transformed data into silver layer tables from bronze.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	 DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	 BEGIN TRY
		SET @batch_start_time = GETDATE()


    	PRINT '************************************************';
		PRINT 'LOADING SILVER LAYER';
		PRINT '************************************************';

		PRINT '================================================';
		PRINT 'LOADING CRM TABLES';
		PRINT '================================================';

	SET @start_time = GETDATE(); 
	PRINT '>> TRUNCATING TABLE SILVER.CRM_CUST_INFO <<'
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> INSERTING INTO SILVER.CRM_CUST_INFO FROM BRONZE.CRM_CUST_INFO <<'
	INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname,cst_gndr,cst_marital_status,cst_create_date)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,  --Remove unwanted spaces to maintain data consistency & uniformity.
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 ELSE 'n/a'
		END cst_gndr, --Normalize gender data to readable format.

		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_marital_status, --Normalize marital data to readable format.

		cst_create_date
	FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	) t 
	WHERE t.flag_last = 1; --Select the most recent record per customer
	SET @end_time = GETDATE();
	PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '<<-------------->>';

		

	SET @start_time = GETDATE();
	PRINT '>> TRUNCATING TABLE SILVER.CRM_PROD_INFO <<'
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> INSERTING INTO SILVER.CRM_PROD_INFO FROM BRONZE.CRM_PROD_INFO <<'
	INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  --Extracting Category ID
		SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key,         --Extracting Product ID
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,                                        --Map Product line codes to descriptive values
		CAST (prd_start_dt AS DATE) as prd_start_dt,            --Casting it to only DATE instead of DATETIME
		CAST(
			LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
			AS DATE
		) AS prd_end_dt                --Calcualte end date as one day less than start date
	FROM bronze.crm_prd_info;
	SET @end_time = GETDATE();
	PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '<<-------------->>';


	SET @start_time = GETDATE();
	PRINT '>> TRUNCATING TABLE SILVER.CRM_SALES_DETAILS <<'
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> INSERTING INTO SILVER.CRM_SALES_DETAILS FROM BRONZE.CRM_SALES_DETAILS <<'
	INSERT INTO silver.crm_sales_details (
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
	CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,

	CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,

	CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,

	CASE WHEN sls_sales IS NULL or sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,  --Re-calcualte sales if original values is missing or incorrect

	sls_quantity,

	CASE WHEN sls_price IS NULL or sls_price <= 0
		 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END AS sls_price   --Derive price if available price is invalid
	FROM bronze.crm_sales_details;
	SET @end_time = GETDATE();
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '<<-------------->>';

		PRINT '================================================';
		PRINT 'LOADING ERP TABLES';
		PRINT '================================================';

		
	SET @start_time = GETDATE();
	PRINT '>> TRUNCATING TABLE SILVER.ERP_CUST_AZ12 <<'
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> INSERTING INTO SILVER.ERP_CUST_AZ12 FROM BRONZE.ERP_CUST_AZ12 <<'
	INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen)
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,    --Remove 'NAS' prefix if present

	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS bdate,  --Set future dates to NULL

	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen     --Normalize gender values and handle unknown values
	FROM bronze.erp_cust_az12;
	SET @end_time = GETDATE();
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '<<-------------->>';


	SET @start_time = GETDATE();
	PRINT '>> TRUNCATING TABLE SILVER.ERP_LOC_A101 <<'
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> INSERTING INTO SILVER.ERP_LOC_A101 FROM BRONZE.ERP_LOC_A101 <<'
	INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry)
	SELECT 
	REPLACE(cid, '-', '') cid, 

	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry   --Normalize and handle missing or blank countries
	FROM bronze.erp_loc_a101;
	SET @end_time = GETDATE();
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '<<-------------->>';


	SET @start_time = GETDATE();
	PRINT '>> TRUNCATING TABLE SILVER.ERP_PX_CAT_G1V2 <<'
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> INSERTING INTO SILVER.ERP_PX_CAT_G1V2 FROM BRONZE.ERP_PX_CAT_G1V2 <<'
	INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE();
	PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '<<-------------->>';
		SET @batch_end_time = GETDATE();
		PRINT '================================================';
		PRINT 'Silver Layer load is completed';
		PRINT ' -TOTAL LOAD TIME: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '================================================';
	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'ERROR OCCURED DURING SILVER LAYER EXECUTION';
		PRINT 'ERROR MESSAGE :'+ ERROR_MESSAGE();
		PRINT 'ERROR_MESSAGE :'+ CAST (ERROR_NUMBER () AS NVARCHAR);
		PRINT 'ERROR_MESSAGE :'+ CAST (ERROR_STATE () AS NVARCHAR);
		PRINT '================================================';
	END CATCH
END
