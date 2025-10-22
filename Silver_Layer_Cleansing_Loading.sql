/*  ---Data with Baraa SQL Full Course Practice Project---
   ---Script: Silver Layer Data Cleansing & Loading Tables---

>>>Purpose:
Clean the data Extracted from the Bronze Layer, Transform them into analysis-ready form, Load them into the Silver Layer Tables.

>>>System:
Google Cloud BigQuery

>>>Contents:
1. Prepare data in proper form according to the analysis results from the Bronze Layer Quality Check Query.
2. Truncate table command to prevent possible duplicate loading to the target table in Silver Layer.
3. Insert the prepared data into the target tables in Silver Layer.

>>>Coverage:
All 6 tables in the Silver Layer.

>>>Notes:
1. During the data cleansing/preparation stage, target table definition might need to be changed. Thus, before loading this script for each table, a revisit of the Silver Layer DDL script was done.
2. After running this script, all tables in Silver Layer was loaded and ready for Business Analysis.

>>>Special Thanks:
To Baraa and his team for leading me into the data world.*/

----------------///---------------
--Query for Table 1 crm_cust_info.
----------------///---------------
TRUNCATE TABLE `data-with-baraa-sql-projects.silver.crm_cust_info`; --This is a safety check against possible duplicate inserting into target table.
INSERT INTO `data-with-baraa-sql-projects.silver.crm_cust_info`
  (
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_gndr,
  cst_marital_status,
  cst_create_date
  )
  SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n.a.' 
    END AS cst_gndr,  --In the Quality Check Queries, we already know the gender column only has three distinct values. There is no undercase or spaces around the values. However, as another caution measure taken by Baraa, we put the UPPER and TRIM function also to make sure there is no surprises.
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n.a.' 
    END AS cst_marital_status,
    cst_create_date
  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC)  Flag_Last
      FROM
        `data-with-baraa-sql-projects.bronze.crm_cust_info`
      WHERE
        cst_id IS NOT NULL
    )  --In other DBMS such as SQL Server, the intermediate table from the subquery such as this one need ALIAS name to be defined before running. In BigQuery, this is obviously not the case.
  WHERE 
    Flag_Last = 1 
  ORDER BY
    cst_id
;

--------------///----------------
--Query for Table 2 crm_prd_info.
--------------///----------------
TRUNCATE TABLE `data-with-baraa-sql-projects.silver.crm_prd_info`;
INSERT INTO `data-with-baraa-sql-projects.silver.crm_prd_info`
  (
    prd_id, 
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
  )
SELECT
  prd_id,
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
  prd_nm,
  IFNULL(prd_cost, 1) AS prd_cost,--following logic stated in Quality Check Bronze.
  CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Sport'
    WHEN 'T' THEN 'Touring'
    ELSE 'n.a.'
  END AS prd_line,
  CAST(prd_start_dt AS DATE) AS prd_start_dt,
  CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY CAST(prd_start_dt AS DATE)) - INTERVAL 1 MINUTE AS DATE) AS prd_end_dt --Here I stated 1 minute is because in the actual data format the data type is datetime. This means the old price is valid until the last minute of the previous day. Of course I can also use 1 second/hour/day but this is just symbolic.
FROM 
  `data-with-baraa-sql-projects.bronze.crm_prd_info`
;

----------------///-------------------
--Query for Table 3 crm_sales_details.
----------------///-------------------
TRUNCATE TABLE  `data-with-baraa-sql-projects.silver.crm_sales_details`;
INSERT INTO `data-with-baraa-sql-projects.silver.crm_sales_details`
  (
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
  CASE WHEN 
    sls_order_dt = 0 
    OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 
    THEN NULL
  ELSE 
    PARSE_DATE('%Y%m%d', CAST(sls_order_dt AS STRING)) 
  END 
  AS sls_order_dt,
  CASE WHEN 
    sls_ship_dt = 0 
    OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 
    THEN NULL
  ELSE 
    PARSE_DATE('%Y%m%d', CAST(sls_ship_dt AS STRING)) 
  END 
  AS sls_ship_dt,
  CASE WHEN 
    sls_due_dt = 0 
    OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 
    THEN NULL
  ELSE 
    PARSE_DATE('%Y%m%d', CAST(sls_due_dt AS STRING)) 
  END 
  AS sls_due_dt, --Although there is no issues found for sls_ship_dt and sls_due_dt in the quality check, the measures here is again a caution measure.
  CASE WHEN 
    sls_sales != sls_quantity * sls_price
    OR sls_sales <= 0 
    OR sls_sales IS NULL
    THEN sls_quantity * ABS(sls_price)
  ELSE sls_sales
  END 
  AS sls_sales, --If the Sales is negative, zero or NULL, derive it using Quantity and Price
  sls_quantity,
  CASE 
  WHEN 
    sls_price = 0 
    OR sls_price IS NULL
  THEN ABS(sls_sales) / NULLIF(sls_quantity,0) -- This means price is not INT64 anymore, but FLOAT64.
  WHEN 
    sls_price < 0 
  THEN ABS(sls_price)
  ELSE sls_price
  END 
  AS sls_price --If the Price is zero or NULL, derive it using Sales and Quantity. If the Price is negative, convert it to a positive value.
FROM
  `data-with-baraa-sql-projects.bronze.crm_sales_details`;

---------------///----------------
--Query for Table 4 erp_cust_az12.
---------------///----------------
TRUNCATE TABLE  `data-with-baraa-sql-projects.silver.erp_cust_az12`;
INSERT INTO `data-with-baraa-sql-projects.silver.erp_cust_az12`
 (
    cid,
    bdate,
    gen
  )
SELECT
  CASE WHEN cid LIKE 'NAS%'
    THEN substring(cid, 4, LENGTH(cid))
    ELSE cid
  END AS cid,
  CASE WHEN bdate > DATE_SUB(CURRENT_DATE(), INTERVAL 18 YEAR) --bdates younger than 18 years old or even future dates are not valid and thus should be NULL.
    THEN NULL
    ELSE bdate 
  END AS bdate,
  CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')
    THEN 'Male'
    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE')
    THEN 'Female'
    ELSE 'n.a.'
  END AS gen
FROM
  `data-with-baraa-sql-projects.bronze.erp_cust_az12`;

---------------///----------------
--Query for Table 5: erp_loc_a101.
---------------///----------------
TRUNCATE TABLE  `data-with-baraa-sql-projects.silver.erp_loc_a101`;
INSERT INTO `data-with-baraa-sql-projects.silver.erp_loc_a101`
  (
    cid, 
    cntry
  )
SELECT
  REPLACE(cid, '-', '') cid,
  CASE 
    WHEN TRIM(cntry) IN ('USA', 'US', 'United States') THEN 'USA'
    WHEN TRIM(cntry) IN ('DE', 'Germany') THEN 'DEU'
    WHEN TRIM(cntry) = 'France' THEN 'FRA'
    WHEN TRIM(cntry) = 'United Kingdom' THEN 'GBR'
    WHEN TRIM(cntry) = 'Canada' THEN 'CAN'
    WHEN TRIM(cntry) = 'Australia' THEN 'AUS'
    ELSE 'n.a.'
  END AS cntry,
FROM
  `data-with-baraa-sql-projects.bronze.erp_loc_a101`;

----------------///-----------------
--Query for Table 6: erp_px_cat_g1v2
----------------///-----------------
TRUNCATE TABLE  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`;
INSERT INTO `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`
  (
    id,
    cat,
    subcat,
    maintenance
  )
SELECT
  ID,
  cat,
  subcat,
  maintenance
FROM
 `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`;