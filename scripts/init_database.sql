/*
=================================================================================
Create Database and Schemas
=================================================================================
Script Purpose: 
  This script is used to drop and recreate the "DataWarehouse" database along with the creation of necessary schemas for data processing layers:
  Bronze (raw data), Silver (cleaned/processed data), and Gold (aggregated/final data)

WARNING:
  This script will DROP the "DataWarehouse" database if it already exists, causing permanent deletion of all data within it. Use this script with caution.
*/

-- Drop and recreate the "DataWarehouse" database
DROP DATABASE IF EXISTS "DataWarehouse";

-- Create the "DataWarehouse" database with specified configurations
CREATE DATABASE "DataWarehouse"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

USE DataWarehouse;


-- Create Schemas for various layers of data processing
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS gold;

CREATE SCHEMA IF NOT EXISTS silver;


