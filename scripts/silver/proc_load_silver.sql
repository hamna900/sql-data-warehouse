/* This stored procedure performs the ETL process, loading tables in the Silver schema from the Bronze schema. */

--EXEC Silver.load_silver

CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN 

DECLARE @start_time DATETIME, @end_time DATETIME; 
BEGIN TRY 

PRINT '.......................';
PRINT 'LOADING SILVER LAYER';
PRINT '.......................'; 

SET @start_time = GETDATE(); 
TRUNCATE TABLE Silver.crm_cust_info; 
INSERT INTO Silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)
SELECT
cst_id,
cst_key,
UPPER(TRIM(cst_firstname)) AS cst_firstname,
UPPER(TRIM(cst_lastname)) AS cst_lastname, 
CASE 
WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
ELSE 'n/a'
END AS cst_marital_status,
CASE 
WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
ELSE 'N/A'
END AS cst_gndr,
cst_create_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
    FROM Bronze.crm_cust_info
    WHERE cst_id IS NOT NULL 
) t 
WHERE rn = 1;
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 



SET @start_time = GETDATE(); 
TRUNCATE TABLE Silver.crm_prd_info;
INSERT INTO Silver.crm_prd_info (
prd_id, 
cat_id, 
prd_key, 
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt,
dwh_create_date
) 
SELECT 
prd_id, 
REPLACE (SUBSTRING (prd_key, 1,5 ), '-', '_') AS cat_id,  --we are going to join this table to erp_px_cat_g1v2 
SUBSTRING (prd_key, 7, LEN (prd_key)) AS prd_key,
prd_nm, 
ISNULL (prd_cost, 0) AS prd_cost,
CASE 
WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
ELSE 'n/a' 
END AS prd_line, 
prd_start_dt,
DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt, 
GETDATE() AS dwh_create_dt
FROM Bronze.crm_prd_info; 
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 



SET @start_time = GETDATE(); 
TRUNCATE TABLE Silver.crm_sales_details; 
INSERT INTO Silver.crm_sales_details (
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
CASE 
WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL 
ELSE CAST(CAST (sls_order_dt AS VARCHAR) AS DATE) 
END AS sls_order_dt, 
CASE 
WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
ELSE CAST(CAST (sls_ship_dt AS VARCHAR) AS DATE) 
END AS sls_ship_dt, 
CASE 
WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL 
ELSE CAST(CAST (sls_due_dt AS VARCHAR) AS DATE) 
END AS sls_due_dt, 
CASE 
WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_price * sls_quantity 
THEN sls_quantity * ABS (sls_price) 
ELSE sls_sales
END AS sls_sales, 
sls_quantity, 
CASE
WHEN sls_price IS NULL OR sls_price <= 0 
THEN sls_sales / NULLIF(sls_quantity, 0) --so the denominator is never null
ELSE sls_price 
END AS sls_price
FROM Bronze.crm_sales_details;
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 



SET @start_time = GETDATE(); 
TRUNCATE TABLE Silver.erp_cust_az12;
INSERT INTO Silver.erp_cust_az12
( 
 CID,
 BDATE,
 GEN)
SELECT 
CASE WHEN CID LIKE 'NAS%'
THEN SUBSTRING (CID, 4, LEN (CID)) 
ELSE CID
END AS CID, 
CASE WHEN BDATE > GETDATE() THEN NULL
ELSE BDATE
END AS BDATE, 
CASE
WHEN Upper(TRIM(GEN)) IN ( 'F', 'FEMALE') THEN 'Female' 
WHEN Upper(TRIM(GEN)) IN ( 'M', 'MALE') THEN 'Male' 
ELSE 'N/A'
END AS GEN 
FROM Bronze.erp_cust_az12 
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 



SET @start_time = GETDATE(); 
TRUNCATE TABLE Silver.erp_loc_a101; 
INSERT INTO Silver.erp_loc_a101
(
CID,
CNTRY)
SELECT 
REPLACE(CID, '-', '') AS CID,
CASE 
WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
WHEN TRIM(CNTRY) IN ('US', 'USA')  THEN 'United States'
WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
ELSE TRIM(CNTRY) 
END AS CNTRY 
FROM Bronze.erp_loc_a101
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 




SET @start_time = GETDATE(); 
TRUNCATE TABLE Silver.erp_px_cat_g1v2;
INSERT INTO Silver.erp_px_cat_g1v2
(ID, 
CAT, 
SUBCAT,
MAINTENANCE)
SELECT 
ID, 
CAT, 
SUBCAT,
MAINTENANCE
FROM Bronze.erp_px_cat_g1v2
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 

END TRY 

BEGIN CATCH
PRINT 'ERROR OCCURED WHILE LOADING SILVER LAYER' 
END CATCH 

END

