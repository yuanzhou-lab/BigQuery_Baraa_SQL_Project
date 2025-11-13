/*  ---Data with Baraa SQL Full Course Practice Project---
          ---Script: Silver Layer Quality Check---

>>>Purpose:
This Query does the quality check for the Silver Layer Tables, after loading data, to make sure everything is working as intended.
Here in GitHub Reporsitory, I document the entire query and results in this script.

>>>System:
Google Cloud BigQuery

>>>Contents:
1. Check for data integrity in Primary Key and Foreign Keys.
2. Check for matching between Foreign Keys and Target Tables.
2. Check for duplicates, unwanted Spaces or NULLs.
3. Data Standardization & Consistency.

>>>Coverage:
All six tables in the Silver Layer.

>>>Notes:
1. Although the project is under instruction from Baraa's data and guidance, the running system is different on my implemention: from SQL server to BigQuery. Thus, part of the language used is different from the original one.
2. During my implementation, I have also added more quality check queries into this script than the original Baraa version.
3. The notes in the script was acquired after test running the new Silver Tables. 

>>>Special Thanks:
To Baraa and his team for leading me into the data world.*/

----------------///---------------
--Query for Table 1 crm_cust_info.
----------------///---------------
--Check for Nulls or Duplicates in Primary Key.
SELECT 
  cst_id,
  count(1)
FROM `data-with-baraa-sql-projects.silver.crm_cust_info`
GROUP BY cst_id
HAVING COUNT(1) > 1 OR cst_id IS NULL; 

SELECT 
  *
FROM `data-with-baraa-sql-projects.silver.crm_cust_info`
Where cst_id IS NULL; 

--Check for unwanted Spaces and NULLs.
SELECT
  cst_firstname
FROM
  `data-with-baraa-sql-projects.silver.crm_cust_info`
WHERE
  cst_firstname != TRIM(cst_firstname)
  OR cst_firstname IS NULL;

SELECT
  cst_lastname
FROM
  `data-with-baraa-sql-projects.silver.crm_cust_info`
WHERE
  cst_lastname != TRIM(cst_lastname)
  OR cst_lastname IS NULL;

SELECT
  cst_gndr
FROM
  `data-with-baraa-sql-projects.silver.crm_cust_info`
WHERE
  cst_gndr != TRIM(cst_gndr)
  OR cst_gndr IS NULL;

SELECT
  cst_marital_status
FROM
  `data-with-baraa-sql-projects.silver.crm_cust_info`
WHERE
  cst_marital_status != TRIM(cst_marital_status)
  OR cst_gndr IS NULL;

SELECT 
  cst_key
FROM 
  `data-with-baraa-sql-projects.silver.crm_cust_info`
WHERE 
  cst_key != TRIM(cst_key)
  OR cst_key IS NULL;
  
--Data Standardization & Consistency
SELECT DISTINCT
  cst_gndr
FROM
  `data-with-baraa-sql-projects.silver.crm_cust_info`;

SELECT DISTINCT
  cst_marital_status
FROM
  `data-with-baraa-sql-projects.silver.crm_cust_info`;

--------------///----------------
--Query for Table 2 crm_prd_info.
--------------///----------------
--Check for Nulls or Duplicates in Primary Key.
SELECT 
  prd_id,
  count(1)
FROM `data-with-baraa-sql-projects.silver.crm_prd_info`
GROUP BY prd_id
HAVING COUNT(1) > 1 OR prd_id IS NULL; 

--Check the matching between the foreign key cat_id and the primary key in target table erp_px_cat_g1v2.
SELECT
  cat_id,
FROM 
  `data-with-baraa-sql-projects.silver.crm_prd_info`
WHERE
  cat_id
  NOT IN
    ( 
    SELECT 
      ID
    FROM
      `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`
    );
--check the matching between the foreign key of modified 'prd_id' to the target table of 'crm_sales_details'
SELECT
  prd_key
FROM 
  `data-with-baraa-sql-projects.silver.crm_prd_info`
WHERE
  prd_key
  IN
    (
      SELECT 
        sls_prd_key
      FROM 
        `data-with-baraa-sql-projects.bronze.crm_sales_details`
    );--There are 177 matches of the foreign key to the target.
SELECT
  prd_key
FROM 
  `data-with-baraa-sql-projects.silver.crm_prd_info`
WHERE
  prd_key
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
  prd_key
FROM 
  `data-with-baraa-sql-projects.silver.crm_prd_info`
WHERE
  prd_key
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
    `data-with-baraa-sql-projects.silver.crm_prd_info`
WHERE
  prd_key != TRIM(prd_key)
  OR prd_key IS NULL;
SELECT
  prd_nm
FROM
    `data-with-baraa-sql-projects.silver.crm_prd_info`
WHERE
  prd_nm != TRIM(prd_nm)
  OR prd_nm IS NULL;
SELECT
  prd_line
FROM
    `data-with-baraa-sql-projects.silver.crm_prd_info`
WHERE
  prd_line != TRIM(prd_line)
  OR prd_line IS NULL;--Abundant spaces was taken care off in the silver layer

--check for NULLs and Negative Numbers.
SELECT
  prd_id,
  prd_cost
FROM
    `data-with-baraa-sql-projects.silver.crm_prd_info`
WHERE
  prd_cost IS NULL
  OR prd_cost < 0; --the NULLs in Bronze layer was gone in Silver Layer.

--Check for invalid Date data
SELECT
  prd_id,
  prd_key
  prd_nm,
  prd_start_dt,
  prd_end_dt
FROM
  `data-with-baraa-sql-projects.silver.crm_prd_info`
WHERE
  prd_start_dt > prd_end_dt --All invalid DATE data was cleaned and does not exist in the Silver Layer.
  --if we put 'prd_start_dt IS NULL' only here, there is still 0 results, which is correct.
  --if we put 'prd_end_dt IS NULL' only here, there is 295 results, comparing to the 197 results in the Bronze Layer. This means there is 102 rows of historical product information data in the table. We can confirm this by put 'prd_end_dt is NOT NULL' here. However, the current product information data with prd_end_dt as NULL is still without validation, and we can only assume their correctness due to the limit of this project scope.
ORDER BY
  prd_id;

----------------///-------------------
--Query for Table 3 crm_sales_details.
----------------///-------------------
--Check for Unwanted Spaces or NULLs
SELECT
  sls_ord_num 
FROM
  `data-with-baraa-sql-projects.silver.crm_sales_details`
WHERE
  sls_ord_num != TRIM(sls_ord_num)
  OR sls_ord_num IS NULL;--returns 0 rows.
SELECT
  sls_prd_key
FROM
  `data-with-baraa-sql-projects.silver.crm_sales_details`
WHERE
  sls_prd_key != TRIM(sls_prd_key)
  OR sls_prd_key IS NULL;--returns 0 rows.

--Check Integrity of the Foreign Keys.
SELECT
  sls_cust_id
FROM
  `data-with-baraa-sql-projects.silver.crm_sales_details`
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
  `data-with-baraa-sql-projects.silver.crm_sales_details`
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
  sls_order_dt,
FROM
  `data-with-baraa-sql-projects.silver.crm_sales_details`
WHERE
  sls_order_dt IS NULL --19 rows of 'NULL' value returned.
  OR LENGTH(CAST(sls_order_dt AS STRING)) != 10; -- No extra row returned.
SELECT
  sls_ship_dt
FROM
  `data-with-baraa-sql-projects.silver.crm_sales_details`
WHERE
  sls_ship_dt IS NULL
  OR LENGTH(CAST(sls_ship_dt AS STRING)) != 10; --0 row returned.
SELECT
  sls_due_dt
FROM
  `data-with-baraa-sql-projects.silver.crm_sales_details`
WHERE
  sls_due_dt IS NULL
  OR LENGTH(CAST(sls_due_dt AS STRING)) != 10; --0 row returned.

SELECT
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt
FROM
  `data-with-baraa-sql-projects.silver.crm_sales_details`
WHERE
  sls_order_dt > sls_ship_dt
  OR sls_order_dt > sls_due_dt; --returns 0 rows.

--Check Data Consistency for sales, quantity and price. Sales = Quantity*Price. No data can be negative, zero, or NULL. 
SELECT
  sls_sales,
  sls_quantity,
  sls_price
FROM
  `data-with-baraa-sql-projects.silver.crm_sales_details`
WHERE
  sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
  OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
  OR sls_sales != sls_quantity * sls_price; --Returns 0 rows.

---------------///----------------
--Query for Table 4 erp_cust_az12.
---------------///----------------
--Check integrity of Primary Key CID.
SELECT
  cid,
  count(1)
FROM
  `data-with-baraa-sql-projects.silver.erp_cust_az12`
GROUP BY
  cid
HAVING
  COUNT(1) > 1; --returns 0 rows.
SELECT
  cid 
FROM
  `data-with-baraa-sql-projects.silver.erp_cust_az12`
WHERE
  cid != TRIM(cid)
  OR cid IS NULL; --returns 0 rows.
SELECT
  cid
FROM
 `data-with-baraa-sql-projects.silver.erp_cust_az12` --returns 18,484 rows.
WHERE
  cid LIKE 'NAS%' --returns 0 rows.
  OR cid LIKE 'AW%'; --returns 18,484 rows.
SELECT
  cid
FROM
  `data-with-baraa-sql-projects.silver.erp_cust_az12`
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
  `data-with-baraa-sql-projects.silver.erp_cust_az12`
WHERE
  bdate IS NULL --returns 0 rows.
  OR bdate < DATE_SUB(CURRENT_DATE(), INTERVAL 101 YEAR) --returns 16 rows.
  OR bdate > DATE_SUB(CURRENT_DATE(), INTERVAL 18 YEAR); --returns 16 NULL rows.

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
  `data-with-baraa-sql-projects`.`silver`.`erp_cust_az12` AS t3
ON
  t1.cst_key = t3.cid
WHERE
  DATE_ADD(t3.bdate, INTERVAL 18 YEAR) >= t2.sls_order_dt
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY t1.cst_id ORDER BY t2.sls_order_dt DESC) = 1
ORDER BY
  last_order_date DESC;--Returns 0 rows.

--Data Standardization & Consistency.
SELECT DISTINCT
  gen
FROM
  `data-with-baraa-sql-projects.silver.erp_cust_az12`; --Returns 'n.a.','Male','Female'.

---------------///----------------
--Query for Table 5: erp_loc_a101.
---------------///----------------
--Check integrity of Primary Key CID.
SELECT
  cid,
  count(1)
FROM
  `data-with-baraa-sql-projects.silver.erp_loc_a101`
GROUP BY
  cid
HAVING
  COUNT(1) > 1; --returns 0 rows.
SELECT
  cid 
FROM
  `data-with-baraa-sql-projects.silver.erp_loc_a101`
WHERE
  cid != TRIM(cid)
  OR cid IS NULL; --returns 0 rows.
SELECT DISTINCT
  cid LIKE 'AW-%'
FROM
 `data-with-baraa-sql-projects.silver.erp_loc_a101`; --returns 'false' only.
SELECT
  cid
FROM
 `data-with-baraa-sql-projects.silver.erp_loc_a101` 
WHERE
  REPLACE(cid, '-', '') IN (
    SELECT
      cst_key
    FROM
      `data-with-baraa-sql-projects.silver.crm_cust_info`
  ); --returns 18,484 rows.

--Data Standardization & Consistency.
SELECT DISTINCT
  cntry
FROM
  `data-with-baraa-sql-projects.silver.erp_loc_a101`; --shows 'n.a', 'AUS', 'CAN', 'DEU', 'FRA', 'USA', 'GBR', 7 values in total.


----------------///-----------------
--Query for Table 6: erp_px_cat_g1v2
----------------///-----------------
--check for Integrity of Foreign Key
SELECT
  ID,
  count(1)
FROM
  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`
GROUP BY
  ID
HAVING
  COUNT(1) > 1; --returns 0 rows.
SELECT
  ID 
FROM
  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`
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
      `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`
    ); --returns 7 row of 'CO_PE', 1 value.
SELECT
  ID
FROM
  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`
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
  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`
WHERE CAT != TRIM(CAT)
  OR CAT IS NULL; --returns 0 rows.
SELECT
  subcat 
FROM
  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`
WHERE subcat != TRIM(subcat)
  OR subcat IS NULL; --returns 0 rows.
SELECT
  maintenance 
FROM
  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`
WHERE
  maintenance IS NULL; --returns 0 rows.

--Data Standardization & Consistency.
SELECT DISTINCT
  cat
FROM
  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`; --shows 'Accessories', 'Clothing', 'Components', 'Bikes', 4 values.
SELECT DISTINCT
  subcat
FROM
  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`; --shows 37 unique values.
SELECT DISTINCT
  maintenance
FROM
  `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`; --shows 'false' and 'true' 2 values. Since it's Boolean data type, it can only be 'true' or 'false', and 'null' of course.