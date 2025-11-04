/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- Checking the bronze.crm_cust_info
-- Check for Nulls or Duplicates in Primary key
SELECT cst_id, count(*) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL

-- check for Unwanted Spaces
SELECT cst_key 
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key) 

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) 

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname) 

-- Data Standardization & Consistency 
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info


-- Checking the table bronze.crm_prd_info

-- Check for Nulls or Duplicates in Primary key
SELECT prd_id, count(*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL

-- Check for Nulls or Negativve Numbers
SELECT prd_nm 
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency 
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info


-- Checking the table bronze.crm_sales_details
-- Check for Invalid Dates 
SELECT 
NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) !=8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

-- Check for INvalid Date Orders
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quantity, and Price 
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative

SELECT 
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_price IS NULL OR sls_quantity IS NULL
OR sls_sales <= 0 OR sls_price <= 0 OR sls_quantity <= 0

-- Checking the table bronze.erp_cust_az12
-- IDentify Out Of Range Dates
SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT gen 
FROM bronze.erp_cust_az12

-- Data Standardization & Consistency
SELECT DISTINCT cntry 
FROM bronze.erp_loc_a101

-- Checking the table bronze.erp_px_cat_g1v2
-- Check for unwanted Spaces
SELECT * FROM bronze.erp_px_cat_g1v2 
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2 

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2 

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2 
