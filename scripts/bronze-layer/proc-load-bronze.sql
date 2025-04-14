/*
===============================================================================
STORED PROCEDURE: LOAD BRONZE LAYER (Source -> Bronze)
SCRIPT PURPOSE:
      This procedure loads data into the Bronze Layer from a CSV file 
        -> Truncate the bronze tables before loading data 
        -> Use 'BULK INSERT' to load the data to the Bronze Layer from the 
            CSV file 

EXECUTION CODE:
EXEC Bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE Bronze.load_bronze AS

 BEGIN 
 
	DECLARE @start_time DATETIME, @end_time DATETIME, @layer_start_time DATETIME, @layer_end_time DATETIME;
	BEGIN TRY 

		PRINT '==================================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '==================================================';

		SET @layer_start_time  = GETDATE();
		PRINT '--------------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '--------------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Ttuncate table: Bronze.crm_cust_info';
		TRUNCATE TABLE Bronze.crm_cust_info;
		PRINT '>> INSERT Table: Bronze.crm_cust_info';
		BULK INSERT Bronze.crm_cust_info
		FROM 'C:\SQL PROJECT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' , 
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '--------------------'


		SET @start_time = GETDATE();
		PRINT 'Truncate Table: Bronze.crm_product_info';
		TRUNCATE TABLE Bronze.crm_product_info;
		PRINT 'INSERT TABLE : Bronze.crm_product_info '
		BULK INSERT Bronze.crm_product_info
		FROM 'C:\SQL PROJECT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '--------------------'



		SET @start_time = GETDATE();
		PRINT 'Truncate Table: Bronze.crm_sales_details';
		TRUNCATE TABLE Bronze.crm_sales_details;
		PRINT 'Insert Table: Bronze.crm_sales_details';
		BULK INSERT Bronze.crm_sales_details
		FROM 'C:\SQL PROJECT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '--------------------'


		PRINT '--------------------------------------------------'
		PRINT 'LOADING ERP TABLES'
		PRINT '--------------------------------------------------'


		SET @start_time = GETDATE();
		PRINT 'Truncate Table: Bronze.erp_cust_az12';
		TRUNCATE TABLE Bronze.erp_cust_az12;
		PRINT 'Insert Table: Bronze.erp_cust_az12';
		BULK INSERT Bronze.erp_cust_az12
		FROM 'C:\SQL PROJECT\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------';


		SET @start_time = GETDATE();
		PRINT 'Truncate Table: Bronze.erp_loc_a101';
		TRUNCATE TABLE Bronze.erp_loc_a101;
		PRINT ' INSERT Table: Bronze.erp_loc_a101';
		BULK INSERT Bronze.erp_loc_a101
		FROM 'C:\SQL PROJECT\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: Bronze.erp_px_cat_gv12';
		TRUNCATE TABLE Bronze.erp_px_cat_gv12;
		PRINT '>> Insert Table: Bronze.erp_px_cat_gv12';
		BULK INSERT Bronze.erp_px_cat_gv12
		FROM 'C:\SQL PROJECT\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------';


		SET @layer_end_time  = GETDATE();
		PRINT 'TOTAL BRONZE LAYER DURATION: ' + CAST(DATEDIFF(second, @layer_start_time, @layer_end_time) AS NVARCHAR) + ' seconds';
	END TRY 

	BEGIN CATCH
	PRINT '=================================================='
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '=================================================='

	END CATCH
END
