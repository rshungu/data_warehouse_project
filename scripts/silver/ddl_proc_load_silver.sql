/*
==================================================================================================================================================
DDL Script: Create Silver Tables

Stored Procedure: transform_bronze_to_silver

Description:
  This stored procedure transforms raw customer, product, sales, and ERP data from the bronze layer into the structured silver layer in the data warehouse.

==================================================================================================================================================
Objectives:
 - Standardize and clean customer records (CRM).
 - Normalize product information and categorize product lines.
 - Convert sales data into structured format with correct date and amount formats.
 - Refine and validate ERP customer and location data.
 - Log execution progress and performance metrics.

Logs:
  - Execution time and transformation status are logged using RAISE NOTICE.  

Usage: CALL transform_bronze_to_silver();
==================================================================================================================================================
*/

CREATE OR REPLACE PROCEDURE transform_bronze_to_silver()
LANGUAGE plpgsql
AS $$
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
BEGIN
	-- Record the start time of the entire process
	start_time := clock_timestamp();
	RAISE NOTICE 'Starting transformation process...';

	RAISE NOTICE 'Loading CRM TABLES';
	
	-- Table: silver.crm_cust_info
	RAISE NOTICE 'Creating silver.crm_cust_info table...';

	DROP TABLE IF EXISTS silver.crm_cust_info;
	
	CREATE TABLE silver.crm_cust_info
	(
	    cst_id integer,
	    cst_key character varying(50) COLLATE pg_catalog."default",
	    cst_firstname character varying(50) COLLATE pg_catalog."default",
	    cst_lastname character varying(50) COLLATE pg_catalog."default",
	    cst_marital_status character varying(50) COLLATE pg_catalog."default",
	    cst_gndr character varying(50) COLLATE pg_catalog."default",
	    cst_create_date date
	);
	RAISE NOTICE 'Table silver.crm_cust_info created successfully.';

	-- Now insert this data to the silver layer
	RAISE NOTICE 'Inserting data into silver.crm_cust_info...';
	start_time := clock_timestamp(); -- Record start time for the insert
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
	SELECT 
		cst_id,
		cst_key,
	
	    -- Trim leading and trailing spaces from customer first name and lastname
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
	
		-- Standardize marital status and gender values:
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
		(-- Select all records and create a row number ranking based on latest create date
		SELECT 
			*,
			ROW_NUMBER()OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL)
	WHERE flag_last = 1; -- Select the most recent record
	
	end_time := clock_timestamp(); -- Record end time for the insert
    RAISE NOTICE 'Inserting data into silver.crm_cust_info took %.2f seconds', EXTRACT(EPOCH FROM end_time - start_time);
    
    -- Step 2: Create silver.crm_prd_info table
    RAISE NOTICE 'Creating silver.crm_prd_info table...';
	DROP TABLE IF EXISTS silver.crm_prd_info;
	
	CREATE TABLE silver.crm_prd_info
	(
	    prd_id integer,
	    prd_key character varying(50) COLLATE pg_catalog."default",
		cat_id character varying (50) COLLATE pg_catalog."default",
	    prd_nm character varying(50) COLLATE pg_catalog."default",
	    prd_cost integer,
	    prd_line character varying(50) COLLATE pg_catalog."default",
	    prd_start_dt date,
	    prd_end_dt date
	);
	RAISE NOTICE 'Table silver.crm_prd_info created successfully.';
    
    -- Insert data into silver.crm_prd_info
    RAISE NOTICE 'Inserting data into silver.crm_prd_info...';
    start_time := clock_timestamp();
	
	-- Insert to the editted TABLE
	INSERT INTO silver.crm_prd_info(
	    prd_id,
	    prd_key,
	    cat_id,
	    prd_nm,
	    prd_cost,
	    prd_line,
	    prd_start_dt,
	    prd_end_dt
	)
	SELECT 
	    prd_id,
	
		-- Extract category ID from the first 5 characters of prd_key and replace '-' with '_'
		REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id, 
	
		-- Extract the product key from position 7 onwards (excluding the first 6 characters)
	    SUBSTRING(prd_key FROM 7) AS prd_key, 
	    prd_nm,
	
		-- Ensure product cost is not NULL; replace NULLs with 0
	    COALESCE(prd_cost, 0) AS prd_cost,
	
		-- Categorize product line based on specific letter codes
	    CASE 
	        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	        ELSE 'Unknown'
	    END AS prd_line,
	    prd_start_dt,
		
		-- Calculate product end date: next product's start date - 1 day
	    -- Uses LEAD() to get the next row's start date within the same product category
	    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS prd_end_dt
	FROM bronze.crm_prd_info;

	end_time := clock_timestamp();
    RAISE NOTICE 'Inserting data into silver.crm_prd_info took %.2f seconds', EXTRACT(EPOCH FROM end_time - start_time);
    
    -- Step 3: Create silver.crm_sales_details table
    RAISE NOTICE 'Creating silver.crm_sales_details table...';	
	DROP TABLE IF EXISTS silver.crm_sales_details;
	
	CREATE TABLE silver.crm_sales_details(
	    sls_ord_num character varying(50) COLLATE pg_catalog."default",
	    sls_prd_key character varying(50) COLLATE pg_catalog."default",
	    sls_cust_id integer,
	    sls_order_dt date,
	    sls_ship_dt date,
	    sls_due_dt date,
	    sls_sales integer,
	    sls_quantity integer,
	    sls_price integer);
		
	-- Insert to the editted TABLE
	INSERT INTO silver.crm_sales_details(
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
		-- Convert sls_order_dt (integer) to DATE format if valid
	    CASE 
	        WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL -- If the value is 0 or not in YYYYMMDD format, return NULL
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
	        ELSE TO_DATE(CAST(sls_due_dt AS TEXT), 'YYYYMMDD') -- Convert the integer date to DATE format
	    END AS sls_due_dt,
	
		-- validate and correct the sales amount
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
	        	THEN sls_quantity * ABS(sls_price) -- If sales is missing, zero, or inconsistent, calculate it as quantity * absolute price
	        ELSE sls_sales
	    END AS sls_sales,
	
		-- Ensure quantity is not NULL, replace with 0 if missing
		COALESCE(sls_quantity,0) AS sls_quantity,
	
		-- Vaidate and correct the price per unit
		CASE WHEN sls_price IS NULL OR sls_price <= 0  
	        	THEN sls_sales / NULLIF(sls_quantity,0)  -- Calculate price if missing or zero (avoid division by zero)
	        ELSE sls_price  -- Otherwise, keep the existing price
	    END AS sls_price
	FROM bronze.crm_sales_details;
	
	end_time := clock_timestamp();
    RAISE NOTICE 'Inserting data into silver.crm_sales_details took %.2f seconds', EXTRACT(EPOCH FROM end_time - start_time);

	RAISE NOTICE 'Loading ERP TABLES';

	-- Step 4: Create silver.erp_cust_az12
    RAISE NOTICE 'Creating silver.erp_cust_az12 table..';	
	DROP TABLE IF EXISTS bronze.erp_cust_az12;

	CREATE TABLE bronze.erp_cust_az12
	(
    cid character varying(50) COLLATE pg_catalog."default",
    bdate date,
    gen character varying(6) COLLATE pg_catalog."default"
	);
	
	RAISE NOTICE 'Table silver.erp_cust_az12 created successfully.';
		
	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen)
		
	SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(UPPER(TRIM(cid)),4,LENGTH(cid))
			ELSE cid
		END AS cid,
		CASE WHEN bdate > CURRENT_DATE THEN NULL
			ELSE bdate
		END AS bdate,
		CASE
			WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'N/A'
		END AS gen
	FROM bronze.erp_cust_az12;
	
	end_time := clock_timestamp();
    RAISE NOTICE 'Inserting data into silver.erp_cust_az12 took %.2f seconds', EXTRACT(EPOCH FROM end_time - start_time);
	
	-- Step 5: Create silver.erp_loc_a101	
    RAISE NOTICE 'Creating silver.erp_loc_a101 table..';	
	DROP TABLE IF EXISTS bronze.erp_loc_a101;

	CREATE TABLE bronze.erp_loc_a101
	(
    cid character varying(50) COLLATE pg_catalog."default",
    country character varying(50) COLLATE pg_catalog."default"
	);

	RAISE NOTICE 'Table silver.erp_loc_a101 created successfully.';
	
	INSERT INTO silver.erp_loc_a101(
		cid,
		country)
	SELECT 
		REPLACE(cid,'-','') AS cid,-- Remove the '-' in cid
		CASE 
			WHEN TRIM(country) = 'DE' THEN 'Germany'
			WHEN TRIM(country) IN ('US','USA') THEN 'United States'
			WHEN TRIM(country) = '' OR country IS NULL THEN 'N/A'
			ELSE TRIM(country)
		END AS country
	FROM bronze.erp_loc_a101;
	
	end_time := clock_timestamp();
    RAISE NOTICE 'Inserting data into silver.erp_loc_a101 took %.2f seconds', EXTRACT(EPOCH FROM end_time - start_time);
	
	-- Step 6: Create silver.erp_px_cat_g1v2	
    RAISE NOTICE 'Creating silver.erp_px_cat_g1v2 table..';		
	DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

	CREATE TABLE bronze.erp_px_cat_g1v2
	(
    id character varying(50) COLLATE pg_catalog."default",
    cat character varying(50) COLLATE pg_catalog."default",
    subcat character varying(50) COLLATE pg_catalog."default",
    maintenance character varying(50) COLLATE pg_catalog."default"
	);

	RAISE NOTICE 'Table silver.erp_px_cat_g1v2 created successfully.';

	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
		id,
		cat, 
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	
	end_time := clock_timestamp();
    RAISE NOTICE 'Inserting data into silver.erp_px_cat_g1v2 took %.2f seconds', EXTRACT(EPOCH FROM end_time - start_time);
    
    -- Error Handling Example
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'An error occurred during the process: %', SQLERRM;
        ROLLBACK; -- Optionally rollback if required
        RETURN;
    
    -- Record the end time of the entire process
    end_time := clock_timestamp();
    RAISE NOTICE 'Total transformation process took %.2f seconds', EXTRACT(EPOCH FROM end_time - start_time);

END;
$$;

CALL transform_bronze_to_silver()
