/*
=========================================================================================
STORED PROCEDURE: Load Silver Layer (Bronze -> Silver)
=========================================================================================
Script Purpose : 
            This stored procedure performs the ETL(Extract,Transform,Load) Process to 
            populate the 'Sliver' schema from the 'Bronze' schema.
Actions performed:
            Truncate Silver Tables.
            Insert transformed and cleaned data from 'Bronze' into 'Sliver' Schema.
*/
CREATE OR ALTER PROCEDURE Silver.load_silver AS
	
	BEGIN
		
		DECLARE @start_time DATETIME, @end_time DATETIME, @load_start DATETIME, @load_end DATETIME
		BEGIN TRY 

			SET @load_start = GETDATE();
			PRINT '==============================================='
			PRINT 'CUSTOMER INFO TABLE'
			PRINT '==============================================='

			SET @start_time = GETDATE();
			PRINT'TRUNCATING THE DATA: Silver.crm_cust_info'
			TRUNCATE TABLE Silver.crm_cust_info
			PRINT'>> INSERTING THE DATA INTO: Silver.crm_cust_info'
			INSERT INTO Silver.crm_cust_info(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_date 
			)


			SELECT 
				cst_id,
				cst_key,
				TRIM(cst_firstname) AS cst_firstname,
				TRIM(cst_lastname) AS cst_lastname,
				CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					ELSE 'N/A'
				END AS cst_marital_status,
				CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
					ELSE 'N/A'
				END AS cst_gndr,
				cst_date 

				FROM(
					   SELECT *,
							  ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_date) AS flag_last
					   FROM Bronze.crm_cust_info
					   WHERE cst_id IS NOT NULL
					) t
					WHERE flag_last = 1;

				SET @end_time = GETDATE();
				PRINT 'Cust_info DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';




			PRINT '==============================================='
			PRINT 'PRODUCTS TABLE '
			PRINT '==============================================='

			SET @start_time = GETDATE()

			PRINT'TRUNCATING THE DATA: Silver.crm_product_info'
			TRUNCATE TABLE Silver.crm_product_info
			PRINT'>> INSERTING THE DATA INTO: Silver.crm_product_info'

			INSERT INTO Silver.crm_product_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)

			SELECT prd_id,
			REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id, --Extract Catergory Id 
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- Extract Product Key 
			prd_nm,
			COALESCE(prd_cost,0) AS prd_cost,
			CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountian'
				 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				 ELSE 'N/A' 
			END prd_line, -- Map prodcut line code to Decription values 
			CAST(prd_start_dt AS DATE) ,
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE
			) AS prd_end_dt --Calculate end date as One day before the next Start date. 
			FROM Bronze.crm_product_info;

			SET @end_time = GETDATE();
			PRINT 'Prud_info DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';


			PRINT '==============================================='
			PRINT 'SALES DETAILS TABLE'
			PRINT '==============================================='

			SET @start_time = GetDATE();

			PRINT'TRUNCATING THE DATA: Silver.crm_sales_details'
			TRUNCATE TABLE Silver.crm_sales_details
			PRINT'>> INSERTING THE DATA INTO: Silver.crm_sales_details'

			INSERT INTO Silver.crm_sales_details(
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
			END  sls_order_dt, --SALE_ORDER_DATE 

			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS  sls_ship_dt, --SALE_SHIP_DATE 

			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS  sls_due_dt, -- SALE_DUE_DATE

			CASE WHEN  sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END sls_sales, --SALE_SALES

			sls_quantity, --SALE_QUANTITY

			CASE WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / sls_quantity
				ELSE sls_price 
			END sls_price --SALE_PRICE

			FROM Bronze.crm_sales_details;

			SET @end_time = GETDATE();
			PRINT 'sale_info DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';


			PRINT '==============================================='
			PRINT 'ERP BIRTHDAY AND GENDER TABLE'
			PRINT '==============================================='

			SET @start_time = GETDATE()

			PRINT'TRUNCATING THE DATA: Silver.erp_cust_az12'
			TRUNCATE TABLE Silver.erp_cust_az12
			PRINT'>> INSERTING THE DATA INTO: Silver.erp_cust_az12'

			INSERT INTO Silver.erp_cust_az12(
					cid,
					bdate,
					gen
			)
			SELECT 
				CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
					 ELSE cid 
				END cid,

				CASE WHEN bdate > GETDATE() THEN NULL
					 ELSE bdate
				END AS bdate,

				CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
					 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
					 ELSE 'N/A'	
				END AS gen
			FROM Bronze.erp_cust_az12

			SET @end_time = GETDATE();
			PRINT 'bday_info DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';


			PRINT '==============================================='
			PRINT 'ERP CITY AND COUNTRY TABLE'
			PRINT '==============================================='

			SET @start_time = GETDATE()

			PRINT'TRUNCATING THE DATA: Silver.erp_loc_a101'
			TRUNCATE TABLE Silver.erp_loc_a101
			PRINT'>> INSERTING THE DATA INTO: Silver.erp_loc_a101'

			INSERT INTO Silver.erp_loc_a101(
				cid,
				cntry
			)

			SELECT REPLACE(cid,'-','') cid,
				CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
					 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
					 ELSE cntry
					 END cntry
			FROM Bronze.erp_loc_a101

			SET @end_time = GETDATE();
			PRINT 'City_info DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';

			PRINT '==============================================='
			PRINT 'ERP Accessoceries TABLE'
			PRINT '==============================================='

			SET @start_time = GETDATE()

			PRINT'TRUNCATING THE DATA: Silver.erp_px_cat_gv12'
			TRUNCATE TABLE Silver.erp_px_cat_gv12
			PRINT'>> INSERTING THE DATA INTO: Silver.erp_px_cat_gv12'

			INSERT INTO Silver.erp_px_cat_gv12(
				id,
				cat,
				subcat,
				maintenance
			)

			SELECT id,
				cat,
				subcat,
				maintenance
			FROM Bronze.erp_px_cat_gv12

			SET @end_time = GETDATE();
			PRINT 'accs_info DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
			PRINT '-----------------------------';

			SET @load_end = GETDATE();
			PRINT 'Silver_layer Duration: ' + CAST(DATEDIFF(second,@load_start,@load_end) AS NVARCHAR) + ' Seconds';
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
