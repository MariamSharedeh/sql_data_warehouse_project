/*------------------------------------------------------------
   DDL scripts : Create Bronze Tables
   script purpose :
		this script create tables in the 'bronze' schema, dropping existing tables
		if already exist.
		Run this script to re-difine the DDL structure of 'bronze' tables
------------------------------------------------------------
*/
if OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR (50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);


if OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost  INT,  
	prd_line  NVARCHAR(10), 
	prd_start_dt DATE,
	prd_end_dt DATE
);



if OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    INT,
    sls_ship_dt     INT,
    sls_due_dt      INT,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT
);

if OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
    cid   NVARCHAR(50),
    bdate  DATE,
	gen   NVARCHAR(10)
);


if OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
    cid  NVARCHAR(50),
	cntry NVARCHAR(50)
);



if OBJECT_ID('bronze.erp_pax_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_pax_cat_g1v2;
CREATE TABLE bronze.erp_pax_cat_g1v2(
    id  NVARCHAR(50),
	cat  NVARCHAR(60),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(10)
);


------------------------------------------------------------
--   Bronze Load Procedure
------------------------------------------------------------
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
