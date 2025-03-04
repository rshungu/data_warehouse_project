/*
========================================================================================================================================
DDL Script: Create Bronze Tables
Stored Procedure: Load Bronze layer (Source -> Bronze)
========================================================================================================================================
Purpose of the script:
  This script creates tables in the bronze schema, dropping existing tables if they already exist.
  The stored procedure loads data into the bronze schema from external csv files and does not take any parameters nor return any values, 
    - By truncating the bronze tables before the data is loaded
    - Using the "bulk" inserting command
    - Usage: 'CALL load_bronze_data()'
  
  Run this in POSTGRESQL to re-define the DDL structure of 'bronze' Tables
=================================================================================================
*/

-- Table: bronze.crm_cust_info
DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
	cst_id int,
	cst_key "varchar"(50),
	cst_firstname "varchar"(50),
	cst_lastname "varchar"(50),
	cst_marital_status "varchar"(50),
	cst_gndr "varchar"(50),
	cst_create_date date
);

-- Table: bronze.crm_prd_info
DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
	prd_id int,
	prd_key "varchar"(50),
	prd_nm "varchar"(50),
	prd_cost int,
	prd_line "char",
	prd_start_dt date,
	prd_end_dt date
);

-- Table: bronze.crm_sales_details
DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
	sls_ord_num "varchar"(50),
	sls_prd_key "varchar"(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);

-- Table: bronze.erp_cust_az12
DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
	cid "varchar"(50),
	bdate date,
	gen "varchar"(6)
);

-- Table: bronze.erp_loc_a101
DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101 (
	cid "varchar"(50),
	country "varchar"(50)
);

-- Table: bronze.erp_px_cat_g1v2
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2 (
 id "varchar"(50),
 cat "varchar"(50),
 subcat "varchar"(50),
 maintenance "varchar"(50)
);

-- STORED PROCEDURE
CREATE OR REPLACE PROCEDURE load_bronze_data()
LANGUAGE plpgsql
AS $$
-- Loading data from CSV files to their corresponding tables
DECLARE 
	start_time TIMESTAMP; 
	end_time TIMESTAMP;
	duration INTERVAL;
	load_duration INTERVAL;
BEGIN
    -- CRM Data
	start_time := clock_timestamp();
	
    RAISE NOTICE 'Loading data into bronze.crm_cust_info %',now();
    TRUNCATE TABLE bronze.crm_cust_info;
    EXECUTE 'COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
             FROM ''C:\\Program Files\\PostgreSQL\\17\\data\\Datasets\\datasets\\source_crm\\cust_info.csv''
             WITH (FORMAT csv, HEADER true, DELIMITER '','')';
			 
	-- Track end time
    end_time = clock_timestamp();
	duration = age(end_time,start_time);
	
	RAISE NOTICE '>>>Loading data into bronze.crm_cust_info % in seconds', duration;
	RAISE NOTICE '>> --------------';
	
	start_time := clock_timestamp();
    RAISE NOTICE 'Loading data into bronze.crm_prd_info %', now();
    TRUNCATE TABLE bronze.crm_prd_info;
    EXECUTE 'COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
             FROM ''C:\\Program Files\\PostgreSQL\\17\\data\\Datasets\\datasets\\source_crm\\prd_info.csv''
             WITH (FORMAT csv, HEADER true, DELIMITER '','')';
	end_time := clock_timestamp();
	duration = age(end_time,start_time);
	RAISE NOTICE '>>>Loading data into bronze.crm_prd_info % in seconds', duration;
	RAISE NOTICE '>> --------------';

	start_time := clock_timestamp();
    RAISE NOTICE 'Loading data into bronze.crm_sales_details %', now();
    TRUNCATE TABLE bronze.crm_sales_details;
    EXECUTE 'COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
             FROM ''C:\\Program Files\\PostgreSQL\\17\\data\\Datasets\\datasets\\source_crm\\sales_details.csv''
             WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    end_time := clock_timestamp();
	duration = age(end_time,start_time);
	RAISE NOTICE '>>>Loading data into bronze.crm_sales_details % in seconds', duration;
	RAISE NOTICE '>> --------------';

    -- ERP Data
	start_time := clock_timestamp();
    RAISE NOTICE 'Loading data into bronze.erp_cust_az12 %',now();
    TRUNCATE TABLE bronze.erp_cust_az12;
    EXECUTE 'COPY bronze.erp_cust_az12 (cid, bdate, gen)
             FROM ''C:\\Program Files\\PostgreSQL\\17\\data\\Datasets\\datasets\\source_erp\\CUST_AZ12.csv''
             WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    end_time := clock_timestamp();
	duration = age(end_time,start_time);
	RAISE NOTICE '>>>Loading data into bronze.erp_cust_az12 % in seconds', duration;
	RAISE NOTICE '>> --------------';

	start_time := clock_timestamp();
    RAISE NOTICE 'Loading data into bronze.erp_px_cat_g1v2 %', now();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    EXECUTE 'COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
             FROM ''C:\\Program Files\\PostgreSQL\\17\\data\\Datasets\\datasets\\source_erp\\PX_CAT_G1V2.csv''
             WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    end_time := clock_timestamp();
	duration = age(end_time,start_time);
	RAISE NOTICE '>>>Loading data into bronze.erp_px_cat_g1v2 % in seconds', duration;
	RAISE NOTICE '>> --------------';

	start_time := clock_timestamp();
    RAISE NOTICE 'Loading data into bronze.erp_loc_a101 %', now();
    TRUNCATE TABLE bronze.erp_loc_a101;
    EXECUTE 'COPY bronze.erp_loc_a101 (cid, country)
             FROM ''C:\\Program Files\\PostgreSQL\\17\\data\\Datasets\\datasets\\source_erp\\LOC_A101.csv''
             WITH (FORMAT csv, HEADER true, DELIMITER '','')';
    end_time := clock_timestamp();
	duration = age(end_time,start_time);
	RAISE NOTICE '>>>Loading data into bronze.erp_loc_a101 % in seconds', duration;
	RAISE NOTICE '>> --------------';

	-- Calculate the duration
	load_duration = age(end_time,start_time);

    RAISE NOTICE 'Data loading in bronze layer %',load_duration;
END;
$$;
