/*  ---Data with Baraa SQL Full Course Practice Project---
          ---Script: Bronze Layer Quality Check---

>>>Purpose:
This Query supports the Data Cleansing Query, serving as a Quality Check Query in the source Bronze Layer.
Here in GitHub Reporsitory, I document the entire query and results in this script.

>>>System:
Google Cloud BigQuery

>>>Contents:
1. Check for data integrity in Primary Key and Foreign Keys.
2. Check for matching between Foreign Keys and Target Tables.
2. Check for duplicates, unwanted Spaces or NULLs.
3. Data Standardization & Consistency.

>>>Coverage:
All six tables in the Bronze Layer.

>>>Notes:
1. Although the project is under instruction from Baraa's data and guidance, the running system is different on my implemention: from SQL server to BigQuery. Thus, part of the language used is different from the original one.
2. During my implementation, I have also added more quality check queries into this script than the original Baraa version.
3. The notes in the script was acquired after running on the project's bronze layer. 
4. All results should be able to reproduce as the bronze layer will not update due to the nature of this practise project. 

>>>Special Thanks:
To Baraa and his team for leading me into the data world.*/

----------------///---------------
--Query for Table 1 crm_cust_info.
----------------///---------------
--Check for Nulls or Duplicates in Primary Key.
SELECT 
  cst_id,
  count(1)
FROM `data-with-baraa-sql-projects.bronze.crm_cust_info`
GROUP BY cst_id
HAVING COUNT(1) > 1 OR cst_id IS NULL; 

SELECT 
  *
FROM `data-with-baraa-sql-projects.bronze.crm_cust_info`
Where cst_id IS NULL; 

--Check for unwanted Spaces and NULLs.
SELECT
  cst_firstname
FROM
  `data-with-baraa-sql-projects.bronze.crm_cust_info`
WHERE
  cst_firstname != TRIM(cst_firstname)
  OR cst_firstname IS NULL;

SELECT
  cst_lastname
FROM
  `data-with-baraa-sql-projects.bronze.crm_cust_info`
WHERE
  cst_lastname != TRIM(cst_lastname)
  OR cst_lastname IS NULL;

SELECT
  cst_gndr
FROM
  `data-with-baraa-sql-projects.bronze.crm_cust_info`
WHERE
  cst_gndr != TRIM(cst_gndr)
  OR cst_gndr IS NULL;

SELECT
  cst_marital_status
FROM
  `data-with-baraa-sql-projects.bronze.crm_cust_info`
WHERE
  cst_marital_status != TRIM(cst_marital_status)
  OR cst_gndr IS NULL;

SELECT 
  cst_key
FROM 
  `data-with-baraa-sql-projects.bronze.crm_cust_info`
WHERE 
  cst_key != TRIM(cst_key)
  OR cst_key IS NULL;
  
--Data Standardization & Consistency
SELECT DISTINCT
  cst_gndr
FROM
  `data-with-baraa-sql-projects.bronze.crm_cust_info`;

SELECT DISTINCT
  cst_marital_status
FROM
  `data-with-baraa-sql-projects.bronze.crm_cust_info`;

--------------///----------------
--Query for Table 2 crm_prd_info.
--------------///----------------
--Check for Nulls or Duplicates in Primary Key.
SELECT 
  prd_id,
  count(1)
FROM `data-with-baraa-sql-projects.bronze.crm_prd_info`
GROUP BY prd_id
HAVING COUNT(1) > 1 OR prd_id IS NULL; 

SELECT 
  prd_key,
  count(1)
FROM `data-with-baraa-sql-projects.bronze.crm_prd_info`
GROUP BY prd_key
HAVING COUNT(1) > 1 OR prd_key IS NULL; 

SELECT * FROM `data-with-baraa-sql-projects.bronze.crm_prd_info`
;
--Check the matching between the foreign key cat_id and the primary key in target table erp_px_cat_g1v2.
SELECT
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
FROM 
  `data-with-baraa-sql-projects.bronze.crm_prd_info`
WHERE
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') 
  NOT IN
    ( 
    SELECT 
      ID
    FROM
      `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`
    );
--check the matching between the foreign key of modified 'prd_id' to the target table of 'crm_sales_details'
SELECT
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key
FROM 
  `data-with-baraa-sql-projects.bronze.crm_prd_info`
WHERE
  SUBSTRING(prd_key, 7, LENGTH(prd_key))
  IN
    (
      SELECT 
        sls_prd_key
      FROM 
        `data-with-baraa-sql-projects.bronze.crm_sales_details`
    );--There are 177 matches of the foreign key to the target.
SELECT
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key
FROM 
  `data-with-baraa-sql-projects.bronze.crm_prd_info`
WHERE
  SUBSTRING(prd_key, 7, LENGTH(prd_key))
  NOT IN
    (
      SELECT 
        sls_prd_key
      FROM 
        `data-with-baraa-sql-projects.bronze.crm_sales_details`
    );--There are 220 misses of the foreign key to the target.
SELECT DISTINCT
  sls_prd_key
FROM 
  `data-with-baraa-sql-projects.bronze.crm_sales_details`;--The target column has distinct value of 130.
SELECT DISTINCT
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key
FROM 
  `data-with-baraa-sql-projects.bronze.crm_prd_info`
WHERE
  SUBSTRING(prd_key, 7, LENGTH(prd_key))
  IN
    (
      SELECT 
        sls_prd_key
      FROM 
        `data-with-baraa-sql-projects.bronze.crm_sales_details`
    );-- There are also 130 uniqte value matches of the foreign key to the target column. Means a 'full inclusion' from the source column towards the target column. 

--Check for unwanted Spaces and NULLs.
SELECT
  prd_key
FROM
    `data-with-baraa-sql-projects.bronze.crm_prd_info`
WHERE
  prd_key != TRIM(prd_key)
  OR prd_key IS NULL;
SELECT
  prd_nm
FROM
    `data-with-baraa-sql-projects.bronze.crm_prd_info`
WHERE
  prd_nm != TRIM(prd_nm)
  OR prd_nm IS NULL;
SELECT
  prd_line
FROM
    `data-with-baraa-sql-projects.bronze.crm_prd_info`
WHERE
  prd_line != TRIM(prd_line)
  OR prd_line IS NULL;--Surprisingly, all values in this column have abundant spaces.

--check for NULLs and Negative Numbers.
SELECT
  prd_id,
  prd_cost
FROM
    `data-with-baraa-sql-projects.bronze.crm_prd_info`
WHERE
  prd_cost IS NULL
  OR prd_cost < 0; --We can find two NULLs and no negatives in the cost column. Though Baraa gave 0 value to NULL, I believe the cost cannot be 0, let's put 1 to replace these NULLs, in the cleansing and load query of course.

--Check for invalid Date data
SELECT
  prd_id,
  prd_key
  prd_nm,
  prd_start_dt,
  prd_end_dt
FROM
  `data-with-baraa-sql-projects.bronze.crm_prd_info`
WHERE
  prd_start_dt > prd_end_dt --returns 200 results.
  -- if we put 'prd_start_dt IS NULL' only here, there will be 0 results.
  -- if we put 'prd_end_dt IS NULL' only here, there will be 197 results. The whole table has 397 rows, thus the start or end date has either wrong historical data or current data. We can only assume the 'current' data is correct since we have no way validating them using SQL only.
ORDER BY
  prd_id;

----------------///-------------------
--Query for Table 3 crm_sales_details.
----------------///-------------------
--Check for Unwanted Spaces or NULLs
SELECT
  sls_ord_num 
FROM
  `data-with-baraa-sql-projects.bronze.crm_sales_details`
WHERE
  sls_ord_num != TRIM(sls_ord_num)
  OR sls_ord_num IS NULL;--returns 0 rows.
SELECT
  sls_prd_key
FROM
  `data-with-baraa-sql-projects.bronze.crm_sales_details`
WHERE
  sls_prd_key != TRIM(sls_prd_key)
  OR sls_prd_key IS NULL;--returns 0 rows.

--Check Integrity of the Foreign Keys.
SELECT
  sls_cust_id
FROM
  `data-with-baraa-sql-projects.bronze.crm_sales_details`
WHERE
  sls_cust_id NOT IN
    (
      SELECT
        cst_id
      FROM
        `data-with-baraa-sql-projects.silver.crm_cust_info`
    );--returns 0 rows.
SELECT
  sls_prd_key
FROM
  `data-with-baraa-sql-projects.bronze.crm_sales_details`
WHERE
  sls_prd_key NOT IN
    (
      SELECT
        prd_key
      FROM
        `data-with-baraa-sql-projects.silver.crm_prd_info`
    );--returns 0 rows.

--Check for invalid Dates.
SELECT
  NULLIF(sls_order_dt, 0) AS sls_order_dt,
FROM
  `data-with-baraa-sql-projects.bronze.crm_sales_details`
WHERE
  sls_order_dt <= 0 --17 rows of 'NULL' value returned.
  OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 -- 2 more rows of random numbers returned.
  OR sls_order_dt > 20251019 -- No extra row returned
  OR sls_order_dt < 20000101; -- No extra row returned
SELECT
  NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM
  `data-with-baraa-sql-projects.bronze.crm_sales_details`
WHERE
  sls_ship_dt <= 0 --0 row returned.
  OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 --0 row returned.
  OR sls_ship_dt > 20251019 --0 row returned.
  OR sls_ship_dt < 20000101; --0 row returned.
SELECT
  NULLIF(sls_due_dt,0) AS sls_due_dt
FROM
  `data-with-baraa-sql-projects.bronze.crm_sales_details`
WHERE
  sls_due_dt <= 0 --0 row returned.
  OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 --0 row returned.
  OR sls_due_dt > 20251019 --0 row returned.
  OR sls_due_dt < 20000101; --0 row returned.

SELECT
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt
FROM
  `data-with-baraa-sql-projects.bronze.crm_sales_details`
WHERE
  sls_order_dt > sls_ship_dt
  OR sls_order_dt > sls_due_dt; --returns 0 rows.

--Check Data Consistency for sales, quantity and price. Sales = Quantity*Price. No data can be negative, zero, or NULL. 
SELECT
  sls_sales,
  sls_quantity,
  sls_price
FROM
  `data-with-baraa-sql-projects.bronze.crm_sales_details`
WHERE
  sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 --Returns 10 rows.
  OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL --Returns additional 15 rows.
  OR sls_sales != sls_quantity * sls_price; --Returns additional 10 rows.

---------------///----------------
--Query for Table 4 erp_cust_az12.
---------------///----------------
--Browse the table and its target table in CRM.
SELECT *
FROM
  `data-with-baraa-sql-projects.bronze.erp_cust_az12`
LIMIT 100;
SELECT *
FROM
  `data-with-baraa-sql-projects.bronze.crm_cust_info`
LIMIT 100;

--Check integrity of Primary Key CID.
SELECT
  cid,
  count(1)
FROM
  `data-with-baraa-sql-projects.bronze.erp_cust_az12`
GROUP BY
  cid
HAVING
  COUNT(1) > 1; --returns 0 rows.
SELECT
  cid 
FROM
  `data-with-baraa-sql-projects.bronze.erp_cust_az12`
WHERE
  cid != TRIM(cid)
  OR cid IS NULL; --returns 0 rows.
SELECT
  cid
FROM
 `data-with-baraa-sql-projects.bronze.erp_cust_az12` --returns 18,484 rows.
WHERE
  cid LIKE 'NAS%' 
  OR cid LIKE 'AW%'; --returns 18,484 rows.
SELECT
  cid
FROM
  (
    SELECT
      CASE WHEN cid LIKE 'NAS%' 
        THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
      END AS cid
    FROM
      `data-with-baraa-sql-projects.bronze.erp_cust_az12`
  )
WHERE 
  cid IN
  (
    SELECT
      cst_key
    FROM
      `data-with-baraa-sql-projects.silver.crm_cust_info`
  ); --returns 18,484 rows.

--Check for data integrity of bdate
SELECT
  bdate
FROM 
  `data-with-baraa-sql-projects.bronze.erp_cust_az12`
WHERE
  bdate IS NULL --returns 0 rows.
  OR bdate < DATE_SUB(CURRENT_DATE(), INTERVAL 101 YEAR) --returns 16 rows.
  OR bdate > DATE_SUB(CURRENT_DATE(), INTERVAL 18 YEAR); --returns 16 more rows.

SELECT
  DISTINCT t1.cst_id,
  t1.cst_key,
  t1.cst_firstname,
  t1.cst_lastname,
  t3.bdate AS customer_birth_date,
  t2.sls_order_dt AS last_order_date
FROM
  `data-with-baraa-sql-projects`.`silver`.`crm_cust_info` AS t1
INNER JOIN
  `data-with-baraa-sql-projects`.`silver`.`crm_sales_details` AS t2
ON
  t1.cst_id = t2.sls_cust_id
INNER JOIN
  `data-with-baraa-sql-projects`.`bronze`.`erp_cust_az12` AS t3
ON
  t1.cst_key = t3.cid
WHERE
  DATE_ADD(t3.bdate, INTERVAL 18 YEAR) >= t2.sls_order_dt
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY t1.cst_id ORDER BY t2.sls_order_dt DESC) = 1
ORDER BY
  last_order_date DESC;--This is an Gemini Generated query on customers who is younger than 18 when they place their last order in sales_details. Look at its usage of QUALIFY clause. This query returns 2 rows.

--Data Standardization & Consistency.
SELECT DISTINCT
  gen
FROM
  `data-with-baraa-sql-projects.bronze.erp_cust_az12`;

---------------///----------------
--Query for Table 5: erp_loc_a101.
---------------///----------------
--Check integrity of Primary Key CID.
SELECT
  cid,
  count(1)
FROM
  `data-with-baraa-sql-projects.bronze.erp_loc_a101`
GROUP BY
  cid
HAVING
  COUNT(1) > 1; --returns 0 rows.
SELECT
  cid 
FROM
  `data-with-baraa-sql-projects.bronze.erp_loc_a101`
WHERE
  cid != TRIM(cid)
  OR cid IS NULL; --returns 0 rows.
SELECT DISTINCT
  cid LIKE 'AW-%'
FROM
 `data-with-baraa-sql-projects.bronze.erp_loc_a101`; --returns 'true' only, no 'false' value, meaning all cid is in the format of 'AW-xxxx'.
SELECT
  REPLACE(cid, '-', '') cid
FROM
 `data-with-baraa-sql-projects.bronze.erp_loc_a101` --returns 18.494 rows.
WHERE
  REPLACE(cid, '-', '') IN (
    SELECT
      cst_key
    FROM
      `data-with-baraa-sql-projects.silver.crm_cust_info`
  ); --returns 18,484 rows, meaning all cid in erp_loc_a101 is recorded also in crm_cust_info.

--Data Standardization & Consistency.
SELECT DISTINCT
  cntry
FROM
  `data-with-baraa-sql-projects.bronze.erp_loc_a101`; --shows NULL, BLANK, SPACE, and country names in different formats.

----------------///-----------------
--Query for Table 6: erp_px_cat_g1v2
----------------///-----------------
--check for Integrity of Foreign Key
SELECT
  ID,
  count(1)
FROM
  `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`
GROUP BY
  ID
HAVING
  COUNT(1) > 1; --returns 0 rows.
SELECT
  ID 
FROM
  `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`
WHERE
  ID != TRIM(ID)
  OR ID IS NULL; --returns 0 rows.
SELECT
  cat_id
FROM
  `data-with-baraa-sql-projects.silver.crm_prd_info`
WHERE 
  cat_id NOT IN
    (SELECT
      ID
     FROM
      `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`
    ); --returns 1 row 'CO_PE'.
SELECT
  ID
FROM
  `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`
WHERE
  ID NOT IN
   (SELECT
      cat_id 
    FROM
      `data-with-baraa-sql-projects.silver.crm_prd_info`
    ); --returns one row, 'CO_PD'.

--Check for unwanted spaces or NULLs.
SELECT
  CAT
FROM
  `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`
WHERE CAT != TRIM(CAT)
  OR CAT IS NULL; --returns 0 rows.
SELECT
  subcat 
FROM
  `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`
WHERE subcat != TRIM(subcat)
  OR subcat IS NULL; --returns 0 rows.
SELECT
  maintenance 
FROM
  `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`
WHERE
  maintenance IS NULL; --returns 0 rows.

--Data Standardization & Consistency.
SELECT DISTINCT
  cat
FROM
  `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`; --shows 'Accessories', 'Clothing', 'Components', 'Bikes', 4 values.
SELECT DISTINCT
  subcat
FROM
  `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`; --shows 37 unique values.
SELECT DISTINCT
  maintenance
FROM
  `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`; --shows 'false' and 'true' 2 values. Since it's Boolean data type, it can only be 'true' or 'false', and 'null' of course.