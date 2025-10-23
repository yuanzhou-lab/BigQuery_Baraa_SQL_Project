---------------------------------------------------------------///----------------------------------------------------------------
/*  ---Data with Baraa SQL Full Course Practice Project---
            ---Script: Gold Layer Load View---

>>>Purpose:
This script serves as the SQL query to create Views for business analysis in Gold Layer.

>>>Scope:
After analysing business objective by looking at the source data and their relationships, three views are defined for the gold layer.
1.  Fact: Sales View, which connect itself with dimensions by surrogate keys.
2.  Dimension: Product View, which was built on crm_prd_info and erp_px_cat_g1v2 in silver layer.
3.  Dimension: Customer View, which was built on crm_cust_info and erp_cust_az12 and erp_loc_a101 in silver layer.

>>>Contents:
1.  Create and Define Gold Layer Schema
2.  Drop and Load Sales View
3.  Drop and Load Product View
4.  Drop and Load Customer View

>>>Note:
About naming convention, snake_case is adopted here according to Baraa's instruction.

>>>Special Thanks:
To Baraa and his team for leading me into the data world.*/
---------------------------------------------------------------///----------------------------------------------------------------

#Create Gold Layer Schema
CREATE SCHEMA IF NOT EXISTS `data-with-baraa-sql-projects.gold`;

------///-------
#fact.sales view
------///-------
DROP VIEW IF EXISTS `data-with-baraa-sql-projects.gold.fact_sales`;
CREATE VIEW `data-with-baraa-sql-projects.gold.fact_sales` AS
SELECT
  sls_ord_num AS order_number,
  C.customer_key,
  P.product_key,
  sls_order_dt AS order_date,
  sls_ship_dt AS shipping_date,
  sls_due_dt AS due_date,
  sls_price AS price,
  sls_quantity AS quantity,
  sls_sales AS sales,
FROM
  `data-with-baraa-sql-projects.silver.crm_sales_details` S
LEFT JOIN 
  `data-with-baraa-sql-projects.gold.dim_product` P
ON
  sls_prd_key = product_number
LEFT JOIN
  `data-with-baraa-sql-projects.gold.dim_customer` C
ON
  sls_cust_id = customer_id
;

-------///-------
#dim.product view
-------///-------
DROP VIEW IF EXISTS `data-with-baraa-sql-projects.gold.dim_product`;
CREATE VIEW `data-with-baraa-sql-projects.gold.dim_product` AS
SELECT
  ROW_NUMBER() OVER(ORDER BY prd_start_dt, prd_id) AS product_key,
  prd_id AS product_id,
  prd_key AS product_number,
  prd_nm AS product_name,
  CAST(prd_cost AS FLOAT64) AS product_cost, #The product cost ranges from 1 to hundreds, thus should be float than integer.
  CASE 
    WHEN cat_id IS NULL THEN id
    ELSE cat_id
  END AS category_id,
  COALESCE(cat, 'n.a.') AS category,
  COALESCE(subcat, 'n.a.') AS sub_category,
  COALESCE(CAST(maintenance AS STRING), 'n.a.') AS maintenance, #the three Coalesces here are counter measures against the NULLs found in the initial results. CAST() function is needed for maintenance since 'n.a.' does not fit boolean type column.
  prd_line AS product_line,
  prd_start_dt AS product_start_date,
  --FORMAT_DATE('%d.%m.%Y', prd_start_dt) AS product_start_date, #Format the product start date into string and dd.mm.yyyy to keep data consistency with customer view. #This measure was not validated due to future analysis needs.
  COALESCE(CAST(prd_end_dt AS STRING), 'Current Product') AS product_end_date
FROM
  `data-with-baraa-sql-projects.silver.crm_prd_info`
LEFT OUTER JOIN 
  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2` #there is 1 row difference between the 'LEFT' and 'FULL' joins here. This came from the source table difference, which we studied in the silver layer quality check.
ON
  cat_id = id
WHERE
  prd_end_dt IS NULL
;

-------///--------
#dim.customer view
-------///--------
DROP VIEW IF EXISTS `data-with-baraa-sql-projects.gold.dim_customer`;
CREATE VIEW `data-with-baraa-sql-projects.gold.dim_customer` AS
SELECT
  ROW_NUMBER() OVER(ORDER BY cst_create_date, cst_id) AS customer_key, #surrogate key
  cst_id AS customer_id,
  cst_key AS customer_number,
  cst_firstname AS first_name,
  cst_lastname AS last_name,
  CASE 
    WHEN COALESCE(cst_gndr, 'n.a.') != 'n.a.' THEN cst_gndr
    ELSE COALESCE(gen, 'n.a.')
  END AS gender,
  bdate AS birthday,
  --COALESCE(FORMAT_DATE('%d.%m.%Y', bdate), 'n.a.') AS birthday, #to eliminate NULLs in the initial results, cast bdate to STRING and Coalesce, this is to keep integrity of same data type in one column, which is a rule in BigQuery system. #This measure was not validated due to future analysis needs.
  cst_marital_status AS marital_status,
  cntry AS country,
  cst_create_date AS creation_date
  --FORMAT_DATE('%d.%m.%Y', cst_create_date) AS creation_date, #This measure was not validated due to future analysis needs.
FROM
  `data-with-baraa-sql-projects.silver.crm_cust_info` AS c1
FULL OUTER JOIN
  `data-with-baraa-sql-projects.silver.erp_cust_az12` AS c2
ON
  cst_key = cid 
FULL OUTER JOIN
  `data-with-baraa-sql-projects.silver.erp_loc_a101` AS c3
ON
  cst_key = c3.cid
;
