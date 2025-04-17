/*
=============================================================
DDL SCRIPT: CREATE SILVER LAYER 
=============================================================
Script purpose:
			This script creates 6 tables in silver schema,
			Dropping existing tables, if they already exist.

			Run this script to re-define the DDL structure 
*/

IF OBJECT_ID ('Silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE Silver.crm_cust_info;

CREATE TABLE Silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID  ('Silver.crm_product_info','U') IS NOT NULL
	DROP TABLE Silver.crm_product_info;

CREATE TABLE Silver.crm_product_info(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID  ('Silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE Silver.crm_sales_details;

CREATE TABLE Silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID  ('Silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE Silver.erp_loc_a101;

CREATE TABLE Silver.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID  ('Silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE Silver.erp_cust_az12;

CREATE TABLE Silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()

);

IF OBJECT_ID  ('Silver.erp_px_cat_gv12','U') IS NOT NULL
	DROP TABLE Silver.erp_px_cat_gv12;

CREATE TABLE Silver.erp_px_cat_gv12(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

