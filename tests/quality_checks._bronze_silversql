/*This script performs various quality checks before and after transformations across the bronze and silver schema tables. 
Ctrl +H for changing the keyword*/ 


-------------------------------------------
----------crm_cust_info--------------------
-------------------------------------------

--Quality check of the table crm_cust_info 
--Change names using ctrl+h
--Check for nulls and duplicates in the primary key
-- Whether duplicates exist or not, if yes, what is the count*/ 
SELECT COUNT(cst_id) AS total_rows,
COUNT(DISTINCT cst_id) AS distinct_rows,
COUNT(cst_id) - COUNT(DISTINCT cst_id) AS duplicate_count
FROM Bronze.crm_cust_info; 
--Which keys have duplicates and nulls 
SELECT cst_id, COUNT(*) AS duplicate_count
FROM Bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL; 
--checking for unwanted spaces
--expectation : no results 
SELECT cst_key FROM Bronze.crm_cust_info WHERE cst_key != TRIM(cst_key) 
SELECT cst_firstname FROM Bronze.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname)
SELECT cst_lastname FROM Bronze.crm_cust_info WHERE cst_lastname != TRIM(cst_lastname)
SELECT cst_gndr FROM Bronze.crm_cust_info WHERE cst_gndr != TRIM(cst_gndr) 
--data standardization and consistency
SELECT DISTINCT cst_gndr FROM Bronze.crm_cust_info
----------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------
-----------crm_prd_info--------------------
-------------------------------------------
--Quality check of the table crm_prd_info
--Change names using ctrl+h
--Check for nulls and duplicates in the primary key
SELECT COUNT(prd_id) AS total_rows,
COUNT(DISTINCT prd_id) AS distinct_rows,
COUNT(prd_id) - COUNT(DISTINCT prd_id) AS duplicate_count
FROM Silver. crm_prd_info; 
--Which keys have duplicates and nulls 
SELECT prd_id, COUNT(*) AS duplicate_count
FROM Silver. crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL; 
--filtering out unmatched data after transformation
--SELECT prd_id, prd_key FROM Silver.crm_prd_info WHERE REPLACE (SUBSTRING (prd_key, 1,5 ), '-', '_') NOT IN (SELECT DISTINCT id FROM Silver.erp_px_cat_g1v2);
--checking for unwanted spaces 
SELECT prd_nm FROM Silver. crm_prd_info WHERE prd_nm != TRIM(prd_nm) 
--checking for nulls and negatives
SELECT prd_cost FROM Silver.crm_prd_info
WHERE prd_cost<0 OR prd_cost IS NULL 
--data standardization and consistency
SELECT DISTINCT prd_line FROM Silver. crm_prd_info
--check for invalid date orders
SELECT * FROM Silver.crm_prd_info WHERE prd_end_dt < prd_start_dt
--start date is after the end date in this file 
--there is overlapping, the end date of first record has to be younger than the start date of the next record but this is not the case
--solution: I am discarding the end dates and using the start date - 1 of the next record as the end date of the previous record

-----------------------------------------------------------------------------------------------------------------------------------


-----------------------------------------------
---------------crm_sales_details---------------
-----------------------------------------------
--Quality check crm_sales_details 
--sls_ord_num is a string so checking for unwated spaces
SELECT sls_ord_num FROM Bronze.crm_sales_details WHERE sls_ord_num != TRIM (sls_ord_num) 
--as we have to join sales_details table with prd-info table (sls_prd_key in sales_details and prd_key in prd_info)
SELECT sls_prd_key FROM Bronze.crm_sales_details WHERE sls_prd_key NOT IN (SELECT prd_key FROM Silver.crm_prd_info) 
--as we have to join sales_details table with cust_info table (sls_cust_id in sales_details and cst_id in cust_info)
SELECT sls_cust_id FROM Bronze.crm_sales_details WHERE sls_cust_id NOT IN (SELECT cst_id FROM Silver.crm_cust_info)
--check for invalid dates
--dates are integers in this file so checking the lenght of the integer first. Has to be 8
SELECT sls_order_dt From Bronze.crm_sales_details WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8; 
--zeros exist so replacing them with nulls 
SELECT sls_ship_dt From Bronze.crm_sales_details WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8; 
SELECT sls_due_dt From Bronze.crm_sales_details WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8; 
SELECT * FROM Bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;
-- sales = quantity * price 
--checking for nulls, negatives and zeros 
SELECT DISTINCT sls_sales, sls_quantity, sls_price From  Bronze.crm_sales_details WHERE sls_sales != sls_price * sls_quantity OR sls_sales IS NULL OR sls_price IS NULL OR 
sls_quantity IS NULL OR sls_sales <0 OR sls_price <0 OR sls_quantity <0 ORDER BY sls_sales, sls_quantity, sls_price;
-----------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------
--------------erp_cust_az12------------------------
---------------------------------------------------

SELECT CID,
BDATE,
GEN 
FROM Silver.erp_cust_az12; 
--checking for invalid birthdates 
SELECT DISTINCT BDATE FROM Silver.erp_cust_az12 WHERE BDATE < '1924-01-01' OR BDATE > GETDATE()
SELECT DISTINCT GEN FROM Silver.erp_cust_az12; 
SELECT * FROM Silver.crm_cust_info; --have to join crm_cust_info and erp_cust_az12(CID and cst_key)

--------------------------------------------------------------------------------------------------------------


-----------------------------------------------------
----------------erp_loc_a101-------------------------
-----------------------------------------------------
SELECT CID, CNTRY FROM Silver.erp_loc_a101
--after replacing - with space checking if there is any unmatched value
SELECT 
REPLACE(CID, '-', '') AS CID FROM Silver.erp_loc_a101 WHERE REPLACE (CID, '-', '') NOT IN (SELECT cst_key FROM Silver.crm_cust_info) 
--standardization and consistency
SELECT DISTINCT CNTRY FROM Silver.erp_loc_a101 ORDER BY CNTRY 
--checking after transformation
SELECT DISTINCT 
CASE 
WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
WHEN TRIM(CNTRY) IN ('US', 'USA')  THEN 'United States'
WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
ELSE TRIM(CNTRY) 
END AS CNTRY 
FROM Silver.erp_loc_a101 ORDER BY CNTRY 
SELECT cst_key FROM Silver.crm_cust_info; --have to join location table with cust_info table, cid and cst_key
-------------------------------------------------------------------------------------------------------------------

----------------------------------------
--------erp_px_cat_g1v2-----------------
----------------------------------------
--going to join this categories table(erp_px_cat_g1v2) with products table (crm_prd_info) 
--derived a new column cat_id in prd_info 
--going to use cat_id from prd_info and id from px_cat for joining 
SELECT 
ID, 
CAT, 
SUBCAT,
MAINTENANCE
FROM Bronze.erp_px_cat_g1v2 
--checking for unwanted spaces
SELECT * FROM Bronze.erp_px_cat_g1v2 WHERE cat!= TRIM(CAT) OR SUBCAT!= TRIM(SUBCAT) OR MAINTENANCE!= TRIM(MAINTENANCE)
-standardization and consistency
SELECT DISTINCT CAT FROM Bronze.erp_px_cat_g1v2
SELECT DISTINCT SUBCAT FROM Bronze.erp_px_cat_g1v2
SELECT DISTINCT MAINTENANCE FROM Bronze.erp_px_cat_g1v2
--------------------------------------------------------------
