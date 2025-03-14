/*
=================================================================================================
Quality Checks: 
  
To validate the integrity, consistency and accuracy of the Gold layer.
    - Ensures the uniqueness of surrogate keys in dimension tables
    - Referential integrity between fact and dimension tables are created
    - Validation of relationships in the data model for analytical reasons is done
=================================================================================================
*/

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
