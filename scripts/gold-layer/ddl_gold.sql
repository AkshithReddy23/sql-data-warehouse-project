/*
=======================================================================================
DDL SCRIPT: Create Gold Views 
=======================================================================================
Script Purpose:
        This script creates views for the gold layer in the data warehouse.
        The Gold Layer represents the final dimension and the fact table(Star Schema).

        Each views perfoms transformations and combines the data from silver layer to 
      produce a clean, enriched, and business-ready dataset.
Usage:
  -These views can be queried directly for analytics and reporting 
=======================================================================================
*/

-- ======================================
--  Create Dimension: gold.dim_customers 
-- ======================================

CREATE VIEW gold.dim_customer AS 
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr 
		 ELSE COALESCE(ca.gen,'N/A')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_date AS create_date
	
FROM Silver.crm_cust_info AS ci  --ci is ilias

LEFT JOIN Silver.erp_cust_az12 AS ca
ON        ci.cst_key = ca.cid

LEFT JOIN Silver.erp_loc_a101 AS la
ON		  ci.cst_key = la.cid

-- ======================================
--  Create Dimension: gold.dim_products 
-- ======================================

CREATE VIEW gold.dim_product AS 
SELECT 
	ROW_NUMBER() OVER(ORDER BY cat_id) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS Product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance AS maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date,
	pn.prd_end_dt AS end_date
	
FROM Silver.crm_product_info AS pn 

LEFT JOIN Silver.erp_px_cat_gv12 AS pc
ON		  pn.cat_id = pc.id



-- ======================================
--  Create Facts: gold.facts_sales 
-- ======================================
CREATE VIEW gold.facts_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_id,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM Silver.crm_sales_details AS sd 

LEFT JOIN gold.dim_product AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer AS cu
ON sd.sls_cust_id = cu.customer_id

