--quality checks for gold layer

---------------------------------------
-----Dimension_customers---------------
---------------------------------------
--assuming master source is CRM 
--checking if any duplicates were generated after joining
SELECT customer_id, COUNT(*) AS duplicate_count
FROM (
    SELECT 
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name,
        ci.cst_lastname AS last_name,
        la.CNTRY AS country, 
        ca.BDATE AS birth_date, 
        ci.cst_marital_status AS marital_status,
        ci.cst_gndr AS gender, 
        ci.cst_create_date AS create_date
		FROM Silver.crm_cust_info AS ci
		LEFT JOIN Silver.erp_cust_az12 AS ca
		ON ci.cst_key = ca.CID
		LEFT JOIN Silver.erp_loc_a101 AS la
		ON ci.cst_key = la.CID
) sub
GROUP BY customer_id HAVING COUNT(*) > 1;

--two gender columns so
SELECT DISTINCT 
ci.cst_gndr,
ca.GEN 
FROM Silver.crm_cust_info AS ci
LEFT JOIN Silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.CID;
----------------------------------------------------------------------------------------------------------


----------------------------------------------
-----------Dimension_products-----------------
----------------------------------------------
--duplicates:
SELECT product_id, COUNT(*) AS duplicate_count
FROM (
    SELECT 
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
		WHERE pi.prd_end_dt IS NULL
) sub
GROUP BY product_id HAVING COUNT(*) > 1;
-------------------------------------------------------------------------------------------------------------------------

--foreign key integrity

SELECT * FROM Gold.fact_sales AS f
LEFT JOIN Gold.dimension_customers AS c
ON c.customer_key = f.customer_key 
LEFT JOIN Gold.dimension_products AS p
ON	p.product_key = f.product_key 
WHERE c.customer_key IS NULL
OR p.product_key IS NULL;
