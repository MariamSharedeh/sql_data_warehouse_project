/*
============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
============================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema 
    from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files 
      to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters 
    or return any values.

Usage Example:
    EXEC bronze.load_bronze;

============================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
		DECLARE @start_time DATETIME = GETDATE();
		DECLARE @end_time DATETIME;
		DECLARE @rowcount INT;
		PRINT '==========================================================';
		PRINT '  DÉMARRAGE DU CHARGEMENT DES DONNÉES BRONZE  ';
		PRINT '==========================================================';
		PRINT 'Start Time: ' + CONVERT(NVARCHAR(30), @start_time, 120);
		PRINT '';
		BEGIN TRY
			PRINT 'Chargement CRM_CUST_INFO...';
			TRUNCATE TABLE bronze.crm_cust_info;
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\maria\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH
			(FirstROW = 2,FIELDTERMINATOR=',',tablock);
			PRINT '';

		
			PRINT 'Chargement CRM_PRD_INFO...';
			TRUNCATE TABLE bronze.crm_prd_info;
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\maria\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH
			(FirstROW = 2,FIELDTERMINATOR=',',tablock);
			SET @rowcount = (SELECT COUNT(*) FROM bronze.crm_prd_info);
			PRINT '' + CAST(@rowcount AS NVARCHAR) + ' rows loaded.';
			PRINT '';
		 
			PRINT 'Chargement CRM_SALES_DETAILS...';
			TRUNCATE TABLE bronze.crm_sales_details;
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\maria\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH
			(FirstROW = 2,FIELDTERMINATOR=',',tablock);


			PRINT 'Chargement ERP_CUST_AZ12...';
			TRUNCATE TABLE bronze.erp_cust_az12;
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Users\maria\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH
			(FirstROW = 2,FIELDTERMINATOR=',',tablock);
		

			PRINT 'Chargement ERP_LOC_A101...';
			TRUNCATE TABLE bronze.erp_loc_a101;
			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Users\maria\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH
			(FirstROW = 2,FIELDTERMINATOR=',',tablock);
		
			PRINT 'Chargement ERP_PAX_CAT_G1V2...';
			TRUNCATE TABLE bronze.erp_pax_cat_g1v2;
			BULK INSERT bronze.erp_pax_cat_g1v2
			FROM 'C:\Users\maria\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH
			(FirstROW = 2,FIELDTERMINATOR=',',tablock);
			
		END TRY
		BEGIN CATCH
			PRINT 'ERROR OCCURRED: ' + ERROR_MESSAGE();
		END CATCH;
		SET @end_time = GETDATE();
		PRINT '';
		PRINT '==========================================================';
		PRINT 'BRONZE DATA LOAD COMPLETED';
		PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT 'End Time: ' + CONVERT(NVARCHAR(30), @end_time, 120);
		PRINT '==========================================================';
		
 END;

/* 
then we can excute :  EXEC bronze.load_bronze
*/
