/*
========================================================================================================================================
Quality Checks
========================================================================================================================================
Script Purpose: 
  The script performs various qaulity checks for data consistency, accuracy and standardization across the 'silver' schema.
    - Null or Duplicate Primary Keys
    - Unwanted spaces in all string fields
    - Data standardization and consistency
    - Invalid date ranges and orders
    - Data consistency between related fields

Usage: Run these after loading data in silver layer
========================================================================================================================================
*/

-- Table: bronze.crm_cust_info

-- Detecting any quality issues, any nulls or duplicates by aggregating the column of interest
SELECT 
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id is NULL;

-- There are atleast 6cst_id that have been duplicated it, thus need of cleansing 
SELECT *
FROM (
	SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	)
WHERE flag_last != 1;

-- Check for duplicates in the primary key or if there are nulls
SELECT cst_id,count(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id is NULL;

-- Now check the differences between duplicates to know which one to keep
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- Now, rank and select the complete one
SELECT *
FROM
	(SELECT 
		*,
		ROW_NUMBER()OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL)
WHERE flag_last != 1;

-- Proving that there are no duplicates
SELECT *
FROM
	(SELECT 
		*,
		ROW_NUMBER()OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL)
WHERE flag_last = 1;

-- In the string value, checking for unwanted spaces
SELECT cst_firstname, cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)OR cst_lastname != TRIM(cst_lastname);


-- Since we know they are unwanted spaces, we can deal with them now
-- Also remove the duplicates in the cst_id and ensure data consistency
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'Unknown'
	END AS cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'Unknown'
	END AS cst_gndr,
	cst_create_date
FROM
	(SELECT 
		*,
		ROW_NUMBER()OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL)
WHERE flag_last = 1;

-- Check on the silver layer data
-- Check for duplicates in the primary key or if there are nulls
SELECT cst_id,count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id is NULL;

-- Now check the differences between duplicates to know which one to keep
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

-- Table: bronze.crm_prd_info

-- Check if there any duplicates in the prd_id
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Checking for unwanted spaces
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Checking for nulls or negative numbers
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL

-- prd_key column needs to be divide
SELECT 
	prd_id,
	prd_key,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,-- Selecting from the left, 1st to fifth letter
		SUBSTRING(prd_key,7, LENGTH(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost,0) as prd_cost, --replacing the null value with a zero
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'Unknown'
	END AS prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

--
SELECT 
	prd_id,
	prd_key,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,-- Selecting from the left, 1st to fifth letter
		SUBSTRING(prd_key,7, LENGTH(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) as prd_cost, --replacing the null value with a zero
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN
	(SELECT DISTINCT id,
	 FROM bronze.erp_px_cat_g1v2) --Checking in another table column

-- Table: bronze.crm_sales_details

SELECT 
	prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,-- Selecting from the left, 1st to fifth letter
		SUBSTRING(prd_key,7, LENGTH(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key,7, LENGTH(prd_key)) NOT IN
	(SELECT DISTINCT sls_prd_key
	 FROM bronze.crm_sales_details);--Checking in another table column

-- Check for invalid dates
SELECT
	NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
	OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 
	OR sls_order_dt > 20500101
	OR sls_order_dt < 19000101;

-- Check for invalide date order
-- There is none
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
	OR sls_ship_dt > sls_due_dt
	OR sls_order_dt > sls_due_dt;

-- Check Data consistency between sales, quantity and price
-->> Sales = Quantity * Price
-->> Values must not be NULL, Zero or Negative

SELECT 
    sls_sales AS old_sls_sales,
    sls_quantity, 
    sls_price AS old_sls_price,
    -- Corrected CASE statement to return a value
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
        	THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
	
	CASE WHEN sls_price IS NULL OR sls_price <= 0  
        	THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL 
    OR sls_quantity IS NULL 
    OR sls_price IS NULL
    OR sls_sales <= 0 
    OR sls_quantity <= 0 
    OR sls_price <= 0
    OR sls_sales != sls_quantity * sls_price
ORDER BY sls_sales, sls_quantity, sls_price;


SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	-- Convert sls_order_dt (integer) to DATE format if valid
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
        ELSE TO_DATE(CAST(sls_order_dt AS TEXT), 'YYYYMMDD')
    END AS sls_order_dt,
	-- Convert sls_ship_dt (integer) to DATE format if valid
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
        ELSE TO_DATE(CAST(sls_ship_dt AS TEXT), 'YYYYMMDD')
    END AS sls_ship_dt,
	-- Convert sls_due_dt (integer) to DATE format if valid
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
        ELSE TO_DATE(CAST(sls_due_dt AS TEXT), 'YYYYMMDD')
    END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
        	THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
	COALESCE(sls_quantity,0) AS sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0  
        	THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

