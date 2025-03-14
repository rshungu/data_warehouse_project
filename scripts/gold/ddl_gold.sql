/*
===============================================================================================================
Create Gold Views 

This script creates views for the Gold layer in the data warehouse. This layer represents the final dimensions
and facts tables.
  With each view, transforms and combines data from the silver layer to produce a clean and business-ready data.

Usage: Views can be queried directly for analytics and reporting
===============================================================================================================
*/
-- Understanding the 'business' objects our datasets presents
--> Customers; Products and Sales

-->>>> Customers objects: crm_cust_info, erp_cust_az12, erp_loc_a101
-- Checking if after joining for any duplicates
SELECT cst_id, COUNT(*)
FROM (
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	co.country
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.goldcid
LEFT JOIN silver.erp_loc_a101 AS co
ON ci.cst_key = co.cid)
GROUP BY cst_id
HAVING COUNT(*) > 1; --There are no duplicates
 
-- After the joining, there are two columns of gender,so data integration
SELECT DISTINCT -- ensures that only unique combinations of the selected columns appear in the output
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr --CRM is the master source for gender info if known
		ELSE COALESCE(ca.gen, 'n/a') -- Otherwise, use ERP gender or fallback to 'n/a' if ERP data is null
	END AS new_gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS co
ON ci.cst_key = co.cid
ORDER BY 1,2;

-- Create a dimesnion table and its corresponding new surrogate key that will have no meaning in the model
CREATE VIEW gold.dim_customers AS -- Dimension object
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	co.country AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr --CRM is the master source for gender info if known
		ELSE COALESCE(ca.gen, 'n/a') -- Otherwise, use ERP gender or fallback to 'n/a' if ERP data is null
	END AS gender,
	ca.bdate AS birth_date,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS co
ON ci.cst_key = co.cid;

-->>>> Products objects: crm_pd_info, erp_px_cat_g1v2
-- Checking if the prd_key is unique
SELECT cat_id, COUNT(*)
FROM (
SELECT
	pi.prd_id,
	pi.prd_key,
	pi.cat_id,
	pi.prd_nm,
	pi.prd_cost,
	pi.prd_line,
	pi.prd_start_dt,
	pi.prd_end_dt,
	pc.id,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pi.prd_key = pc.id
WHERE pi.prd_end_dt IS NULL) --Filter out all historical data
GROUP BY cat_id
HAVING COUNT(*) > 1;

CREATE VIEW gold.dim_products AS -- Product object
SELECT
	ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt,pi.cat_id) AS product_key,
	pi.prd_id AS product_id,
	pi.cat_id AS product_number,
	pi.prd_nm AS product_name,
	pi.prd_cost AS product_cost,
	pi.prd_line AS product_line,
	pc.maintenance,
	pi.prd_start_dt AS start_date,
	pi.prd_key AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pi.prd_key = pc.id
WHERE pi.prd_end_dt IS NULL;

-->>>> Sales objects: crm_sales_details
-- Since the data shows the transcation and measures data and connects to dimension tables, we use as FACT table

CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key, -- Using surrogate key from the dimension tables instead
	dc.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id = dc.customer_id;

-- Foreign Key integrity
SELECT *
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS dc
ON dc.customer_key = f.customer_key
LEFT JOIN gold.dim_products AS dp
ON dp.product_key = f.product_key;
