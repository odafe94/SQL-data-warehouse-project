
/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

!NOTE!: To use this on your machine, update the csv file path (C:\Users\....) as required 

Usage Example:
    EXEC bronze.load_bronze;
    */

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @bronze_start_time DATETIME, @bronze_end_time DATETIME
    BEGIN TRY
        SET @bronze_start_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Bronze Layer';
        PRINT '==========================================';

        PRINT '------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------';

        --Clean all the data in the table
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        -- Bulk insert using csv path
        PRINT '>> Inserting Data into Table: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Gamabunta\Documents\Baara SQL\sql_dwh_project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2, --skip the header
            FIELDTERMINATOR = ',', --define delimeter
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        PRINT '----------------------------------------------------------'

        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info; 
        -- Bulk insert using csv path
        PRINT '>> Inserting Data into Table: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Gamabunta\Documents\Baara SQL\sql_dwh_project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        PRINT '----------------------------------------------------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details_info';
        TRUNCATE TABLE bronze.crm_sales_details_info;
        -- Bulk insert using csv path
        PRINT '>> Inserting Data into Table: bronze.crm_sales_details_info';
        BULK INSERT bronze.crm_sales_details_info
        FROM 'C:\Users\Gamabunta\Documents\Baara SQL\sql_dwh_project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        PRINT '----------------------------------------------------------'

        --moving to the ERP source system Tables
        PRINT '------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data into Table: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\Gamabunta\Documents\Baara SQL\sql_dwh_project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        PRINT '----------------------------------------------------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101; 

        PRINT '>> Inserting Data into Table: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\Gamabunta\Documents\Baara SQL\sql_dwh_project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
        PRINT '----------------------------------------------------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data into Table: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\Gamabunta\Documents\Baara SQL\sql_dwh_project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '----------------------------------------------------------'
        
        SET @bronze_end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
       
        -- printing the duration for the whole procedure
        PRINT '================================================'
        PRINT 'LOADING BRONZE LAYER COMPLETE!'
        PRINT '>> Bronze Layer Whole Batch Loading Duration: ' + CAST(DATEDIFF(second, @bronze_start_time, @bronze_end_time) AS NVARCHAR)
        PRINT '================================================'
    END TRY

    BEGIN CATCH
        PRINT '===============================================';
        PRINT 'ERROR OCCURED DURING BRONZE LAYER LOADING';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '===============================================';
    END CATCH
END
