
/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from the bronze layer. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - inserts clean and transformed data into the silver schema tables. 

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
    */



CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @silver_start_time DATETIME, @silver_end_time DATETIME
    BEGIN TRY
        SET @silver_start_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Silver Layer';
        PRINT '==========================================';

        PRINT '------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------';
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) cst_firstname,
            TRIM(cst_lastname) cst_lastname,
            CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'n\a'
            END cst_marital_status,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                ELSE 'n\a'
            END cst_gndr,
            cst_create_date
        FROM (
            SELECT
            *,
            ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
            ) t
        WHERE flag_last = 1
        ;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        PRINT '----------------------------------------------------------'

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting Data into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt 
        )
        --Data Cleaning & Transformation Query
        SELECT
            prd_id, 
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Exctract category ID
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract product key
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
                END AS prd_line, -- Map product lines to descriptive values 
            CAST(prd_start_dt AS DATE) prd_start_date,
            CAST(LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) prd_end_dt --dervied column
        FROM bronze.crm_prd_info
        ;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        PRINT '----------------------------------------------------------'

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_sales_details_info';
        TRUNCATE TABLE silver.crm_sales_details_info;
        PRINT '>> Inserting Data into: silver.crm_sales_details_info';
        INSERT INTO silver.crm_sales_details_info (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <=0
                THEN sls_sales/NULLIF(sls_quantity,0)
                ELSE sls_price
            END AS sls_pricesls_price
        FROM bronze.crm_sales_details_info
            --WHERE sls_ord_num != TRIM(sls_ord_num) | to check that there are no hidden spaces
            --WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info) | to check that the FK(prd key) is working as expected with the prd_key in the product info table
            --WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
        ;
        
        PRINT '------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------';
                
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        PRINT '----------------------------------------------------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting Data into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid, 
            bdate, 
            gen
        )
        SELECT 
            CASE WHEN cid LIKE 'NAS%'  --Remove 'NAS' prefix if present
                THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE WHEN bdate > GETDATE() THEN NULL -- Set future bdates to NULL
                ELSE bdate
            END AS bdate,
            CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
                ELSE 'n/a'
            END AS gen -- Normalize gender values and handle unknown cases
        FROM bronze.erp_cust_az12
        ;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        PRINT '----------------------------------------------------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Inserting Data into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(
            cid, 
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') cid, -- Removes the dash from the ID so we can Join with the cust table from CRM
            CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry -- Standardized the country data
        FROM bronze.erp_loc_a101
        ;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        PRINT '----------------------------------------------------------'

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )

        SELECT
            TRIM(id) id,
            TRIM(cat) cat,
            TRIM(subcat) subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2
        ;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        PRINT '----------------------------------------------------------'

        --printing the duration for the whole silver load procedure
        SET @silver_end_time = GETDATE()
        PRINT '================================================'
        PRINT 'LOADING SILVER LAYER COMPLETE!'
        PRINT '>> Silver Layer Whole Batch Loading Duration: ' + CAST(DATEDIFF(second, @silver_start_time, @silver_end_time) AS NVARCHAR)
        PRINT '================================================'
    END TRY

    BEGIN CATCH
        PRINT '===============================================';
        PRINT 'ERROR OCCURED DURING SILVER LAYER LOADING';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '===============================================';
    END CATCH
END

