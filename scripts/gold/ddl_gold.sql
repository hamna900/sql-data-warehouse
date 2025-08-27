/*This code defines the Gold layer of the data warehouse using a star schema design. It includes one central fact table (fact_sales) 
that captures transactional sales data and two supporting dimension tables (dimension_customers and dimension_products) that provide
descriptive details for analysis*/ 

-------------Gold.fact_sales-----------------------
CREATE VIEW Gold.fact_sales AS 
SELECT 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key, 
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity, 
sd.sls_price AS price
FROM Silver.crm_sales_details AS sd
LEFT JOIN Gold.dimension_products AS pr
ON sd.sls_prd_key = pr.product_number 
LEFT JOIN Gold.dimension_customers AS cu 
ON sd.sls_cust_id = cu.customer_id 
-----------------------------------------------------

  
---------------Gold.dimension_customers------------------
CREATE VIEW  Gold.dimension_customers AS 
SELECT 
ROW_NUMBER () OVER (ORDER BY cst_id) AS customer_key, 
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.CNTRY AS country, 
ca.BDATE AS birth_date, 
ci.cst_marital_status AS marital_status,
CASE
WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
ELSE COALESCE ( ca.GEN, 'n/a') 
END AS gender, 
ci.cst_create_date AS create_date
FROM Silver.crm_cust_info AS ci
LEFT JOIN Silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.CID
LEFT JOIN Silver.erp_loc_a101 AS la
ON ci.cst_key = la.CID; 
--------------------------------------------------------


------------------Gold.dimension_products----------------
CREATE VIEW Gold.dimension_products AS
SELECT 
ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key ) AS product_key,
pi.prd_id AS product_id,
pi.prd_key AS product_number,
pi.prd_nm AS product_name,
pi.cat_id AS category_id,
pc.CAT AS category,
pc.SUBCAT AS subcategory,
pc.MAINTENANCE AS maintenance, 
pi.prd_cost AS product_cost,
pi.prd_line AS product_line,
pi.prd_start_dt AS start_date
FROM Silver.crm_prd_info AS pi
LEFT JOIN Silver.erp_px_cat_g1v2 AS pc
ON pi.cat_id = pc.id 
Where prd_end_dt IS NULL   --using only current data
---------------------------------------------------------------------------
