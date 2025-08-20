-- This stored procedure (Bronze.load_bronze) loads data into the Bronze layer tables.  
-- Steps performed:  
--   1. For each table, it first clears old data using TRUNCATE.  
--   2. Loads fresh data from CSV files into the tables using BULK INSERT.  
--   3. Prints the load duration (time taken) for each table.  
--   4. If an error occurs during loading, it shows an error message.   

CREATE OR ALTER PROCEDURE Bronze.load_bronze AS 
BEGIN 

DECLARE @start_time DATETIME, @end_time DATETIME; 
BEGIN TRY 
PRINT '...............................';
PRINT 'Loading Bronze Layer'; 
Print '...............................'; 

SET @start_time = GETDATE(); 
TRUNCATE TABLE Bronze.crm_cust_info; 
BULK INSERT Bronze.crm_cust_info 
FROM 'C:\Users\hamna\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' 
WITH (
      FIRSTROW = 2, 
	  FIELDTERMINATOR = ',', 
	  TABLOCK
	  ); 
SELECT * FROM Bronze.crm_cust_info; 
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 

SET @start_time = GETDATE(); 
TRUNCATE TABLE Bronze.crm_prd_info;
BULK INSERT Bronze.crm_prd_info
FROM 'C:\Users\hamna\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
      FIRSTROW = 2, 
	  FIELDTERMINATOR = ',',
	  TABLOCK
	  ); 
SELECT * FROM Bronze.crm_prd_info; 
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 

SET @start_time = GETDATE(); 
TRUNCATE TABLE Bronze.crm_sales_details;
BULK INSERT Bronze.crm_sales_details
FROM 'C:\Users\hamna\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
       FIRSTROW = 2, 
	   FIELDTERMINATOR = ',',
	   TABLOCK
	   ); 
SELECT * FROM Bronze.crm_sales_details; 
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 

SET @start_time = GETDATE(); 
TRUNCATE TABLE Bronze.erp_cust_az12; 
BULK INSERT Bronze.erp_cust_az12
FROM 'C:\Users\hamna\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
      FIRSTROW = 2, 
	  FIELDTERMINATOR = ',', 
	  TABLOCK
	  ); 
SELECT * FROM Bronze.erp_cust_az12; 
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 

SET @start_time = GETDATE(); 
TRUNCATE TABLE Bronze.erp_loc_a101; 
BULK INSERT Bronze.erp_loc_a101
FROM 'C:\Users\hamna\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
      FIRSTROW = 2, 
	  FIELDTERMINATOR = ',', 
	  TABLOCK
	  ); 
SELECT * FROM Bronze.erp_loc_a101; 
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 

SET @start_time = GETDATE(); 
TRUNCATE TABLE Bronze.erp_px_cat_g1v2; 
BULK INSERT Bronze.erp_px_cat_g1v2
FROM 'C:\Users\hamna\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
      FIRSTROW = 2, 
	  FIELDTERMINATOR = ',', 
	  TABLOCK
	  ); 
SELECT * FROM Bronze.erp_px_cat_g1v2; 
SET @end_time = GETDATE(); 
PRINT ' LOAD DURATON : ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 

END TRY 

BEGIN CATCH
PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER' 
END CATCH 
END
  
EXEC Bronze.load_bronze;
