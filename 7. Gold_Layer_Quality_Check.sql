---------------------------------------------------------------///----------------------------------------------------------------
/*  ---Data with Baraa SQL Full Course Practice Project---
---Script: Gold Layer View Creation Support and Quality Check---

>>>Purpose:
This query serves as an working draft before the view creation script for golden layer. This also serves as an quality check for the created views.

>>>System:
Google Cloud BigQuery

>>>Contents:
1. Browsing source table in Silver Layer.
2. Check possible duplicates introduced by JOINs.
3. Check the results from different JOINs and choose the right JOIN in the final query.
4. Further test data integrity after joining information from different tables.
5. Quality check for created views such as duplicates, NULLs, etc.

>>>Coverage:
All three views in the Gold Layer.

>>>Notes:
1. My approach strictly follows Baraa's instruction on the one hand, also try to extend the practise such as playing with different JOINs, going one step ahead in the quality check etc.
2. During Data transformation, I casted all DATE into Strings and formated valid dates in dd.MM.YYYY form. However after second thought on future analysis, I reverted this action and kept all DATE and accepted the NULLs in DATE columns.


>>>Special Thanks:
To Baraa and his team for leading me into the data world.*/
---------------------------------------------------------------///----------------------------------------------------------------

------///-------
#fact.sales view
------///-------
#Check if there is any NULLs in fact.sales view
SELECT
  SUM(CASE
      WHEN order_number IS NULL THEN 1
      ELSE 0
  END
    ) AS null_order_number,
  SUM(CASE
      WHEN customer_key IS NULL THEN 1
      ELSE 0
  END
    ) AS null_customer_key,
  SUM(CASE
      WHEN product_key IS NULL THEN 1
      ELSE 0
  END
    ) AS null_product_key,
  SUM(CASE
      WHEN order_date IS NULL THEN 1
      ELSE 0
  END
    ) AS null_order_date,
  SUM(CASE
      WHEN shipping_date IS NULL THEN 1
      ELSE 0
  END
    ) AS null_shipping_date,
  SUM(CASE
      WHEN due_date IS NULL THEN 1
      ELSE 0
  END
    ) AS null_due_date,
  SUM(CASE
      WHEN price IS NULL THEN 1
      ELSE 0
  END
    ) AS null_price,
  SUM(CASE
      WHEN quantity IS NULL THEN 1
      ELSE 0
  END
    ) AS null_quantity,
  SUM(CASE
      WHEN sales IS NULL THEN 1
      ELSE 0
  END
    ) AS null_sales
FROM
  `data-with-baraa-sql-projects`.`gold`.`fact_sales`;
#Returns 19 NULL order dates, which was validated in silver layer.

#Check any duplicates in gold.fact_sales view introduced by joining
SELECT
  order_number,
  customer_key,
  product_key,
  order_date,
  shipping_date,
  due_date,
  price,
  quantity,
  sales,
  COUNT(*) AS duplicate_count
FROM
  `data-with-baraa-sql-projects`.`gold`.`fact_sales`
GROUP BY
  order_number,
  customer_key,
  product_key,
  order_date,
  shipping_date,
  due_date,
  price,
  quantity,
  sales
HAVING
  COUNT(*) > 1
ORDER BY
  COUNT(*) DESC;
#Returns 0 rows.

#Check Integrity of Foreign Keys
SELECT
  *
FROM
  `data-with-baraa-sql-projects`.`gold`.`fact_sales`
LEFT JOIN
  `data-with-baraa-sql-projects`.`gold`.`dim_customer`
ON
  fact_sales.customer_key = dim_customer.customer_key
WHERE
  fact_sales.customer_key IS NULL;
#Returns 0 rows.

SELECT
  *
FROM
 `data-with-baraa-sql-projects`.`gold`.`fact_sales`
LEFT JOIN
  `data-with-baraa-sql-projects`.`gold`.`dim_product`
ON
  fact_sales.product_key = dim_product.product_key
WHERE
  fact_sales.product_key IS NULL;
#Returns 0 rows.

-------///-------
#dim.product view
-------///-------
#Browse related tables.
SELECT
  *
FROM
  `data-with-baraa-sql-projects.silver.crm_prd_info`;
SELECT
  *
FROM
  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`;

#Try different JOINs and check possible duplicates after JOIN.
SELECT
  prd_id,
  count(1)
FROM
  (
    SELECT
      prd_id,
      cat_id,
      id,
      cat,
      subcat,
      maintenance,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
    FROM
      `data-with-baraa-sql-projects.silver.crm_prd_info`
    LEFT OUTER JOIN 
      `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2` #There is 1 row difference between the 'LEFT' and 'FULL' joins here. This came from the source table difference, which we studied in the silver layer quality check. Because the 1 row from the erp table lacks primary key such as product ID, I use LEFT join here to avoid confusion in the Product View of Gold Layer.
    ON
      cat_id = id
    ORDER BY
      #cat_id #Returns 398 rows, 1 row missing crm table value.
      id #Returns 398 rows, 7 rows missing erp table value.
  )
GROUP BY 
  prd_id
HAVING count(1) > 1
;
#Returns 0 rows.

#Check any nulls in the view dim.products
SELECT
  product_key,
  product_id,
  product_number,
  product_name,
  product_cost,
  category_id,
  category,
  sub_category,
  maintenance,
  product_line,
  product_start_date,
  product_end_date
FROM
  `data-with-baraa-sql-projects`.`gold`.`dim_product`
WHERE
  product_key IS NULL
  OR product_id IS NULL
  OR product_number IS NULL
  OR product_name IS NULL
  OR product_cost IS NULL
  OR category_id IS NULL
  OR category IS NULL
  OR sub_category IS NULL
  OR maintenance IS NULL
  OR product_line IS NULL
  OR product_start_date IS NULL
  OR product_end_date IS NULL
;
#Returns 7 rows from initial results because the erp product table lacks corresponding record. 
#Returns 0 rows after reloading the view with updated query.

#Check for duplicates in dim.products.
SELECT
  product_number,
  count(1)
FROM
  `data-with-baraa-sql-projects`.`gold`.`dim_product`
GROUP BY
  product_number
HAVING
  count(1) > 1
;
#Returns 0 rows.

-------///--------
#dim.customer view
-------///--------
#Browse column information in all three customer tables.
SELECT
  *
FROM
  `data-with-baraa-sql-projects.silver.crm_cust_info`;

SELECT
  *
FROM
  `data-with-baraa-sql-projects.silver.erp_cust_az12`;

SELECT
  *
FROM
  `data-with-baraa-sql-projects.silver.erp_loc_a101`;

#Check any duplicates introduced by JOIN.
SELECT
  cst_id,
  count(1)
FROM
  (
    SELECT
      cst_id,
      cst_key,
      c2.cid,  
      c3.cid,
      cst_firstname,
      cst_lastname,
      cst_marital_status,
      cst_gndr,
      cst_create_date,
      bdate,
      gen,
      cntry
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
  )
GROUP BY
  cst_id
HAVING
  count(1) > 1
; #0 rows returned.

#Test the integrity of gender column in the customer tables.
SELECT DISTINCT
  cst_gndr,
  gen
FROM
  (
    SELECT
      cst_id,
      cst_key,
      c2.cid,  
      c3.cid,
      cst_firstname,
      cst_lastname,
      cst_marital_status,
      cst_gndr,
      cst_create_date,
      bdate,
      gen,
      cntry
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
#In this subquery, I used both LEFT JOIN & FULL JOIN on this clause and both returned 18,484 values, meaning full mutual inclusion of these three tables.
  );
#9 unique combination returned.

SELECT DISTINCT
  cst_firstname,
  cst_lastname,
  cst_marital_status,
  cst_gndr,
  gen,
FROM
  (
    SELECT
      cst_id,
      cst_key,
      c2.cid,  
      c3.cid,
      cst_firstname,
      cst_lastname,
      cst_marital_status,
      cst_gndr,
      cst_create_date,
      bdate,
      gen,
      cntry
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
  )
WHERE
  #cst_gndr != gen #6,072 rows returned.
  #cst_gndr = 'Male' AND gen = 'Female' #17 rows returned, First Names all male.
  #cst_gndr = 'Female' AND gen = 'Male' #40 rows returned, First Names all female.
  #cst_gndr = 'n.a.' #4,569 rows returned, this is a big gap in CRM sourced data.
  #gen = 'n.a.' #4,476 rows returned.
  cst_gndr = 'n.a.' AND gen = 'n.a.' #15 records returned.
;

#check any NULLs in the gold.dim_customer
SELECT
  customer_key,
  customer_id,
  customer_number,
  first_name,
  last_name,
  gender,
  birthday,
  marital_status,
  country,
  creation_date
FROM
  `data-with-baraa-sql-projects`.`gold`.`dim_customer`
WHERE
  customer_key IS NULL
  OR customer_id IS NULL
  OR customer_number IS NULL
  OR first_name IS NULL
  OR last_name IS NULL
  OR gender IS NULL
  OR birthday IS NULL
  OR marital_status IS NULL
  OR country IS NULL
  OR creation_date IS NULL;
#16 rows returned as initial results where the birthday is NULL. Cleared after restating the View. 
#Reverted back to DATE and kept the 16 NULLs after considering the analysing needs in the future.

# check integrity of gender in dim_customer.
SELECT DISTINCT
  gender
FROM
  `data-with-baraa-sql-projects`.`gold`.`dim_customer`;

# check any duplicates in dim_customer.
SELECT
  customer_number,
  count(1)
FROM
  `data-with-baraa-sql-projects`.`gold`.`dim_customer`
GROUP BY
  customer_number
HAVING
  count(1) > 1;

