/*  ---Data with Baraa SQL Full Course Practice Project---
              ---Script: Silver Layer DDL---

>>>Purpose:
This script serves as the DDL of all tables in the Silver Layer.

>>>System:
Google Cloud BigQuery

>>>Contents:
1. Schema(Dataset in BigQuery) Creation of project.silver
2. Drop and Create tables including all their columns in Silver Layer.

>>>Notes:
1. According to the Separation Of Concern Principle, the Bronze Layer is in charge of ingesting data from source systems(in this project, 'namely' the ERP and CRM system), while the silver layer is in charge of cleaning and transforming the data. Thus, the first task for the silver layer, or schema, also start with the DDL. 
2. This table was initially almost identical to the Bronze layer, Baraa only added one column: the 'dwh_create_date'.
3. As the data cleaning progresses in the silver layer, the table column definition also evolves accordingly. Changing points were noted in the comments inside the script.

>>>Special Thanks:
To Baraa and his team for leading me into the data world.*/

CREATE SCHEMA IF NOT EXISTS `data-with-baraa-sql-projects.silver`;

----------------///---------------
--Query for Table 1 crm_cust_info.
----------------///---------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.silver.crm_cust_info`;
CREATE TABLE `data-with-baraa-sql-projects.silver.crm_cust_info`
  (
  cst_id INT64,
  cst_key STRING,
  cst_firstname STRING,
  cst_lastname STRING,
  cst_marital_status STRING,
  cst_gndr STRING,
  cst_create_date DATE,
  dwh_create_date DATETIME DEFAULT CURRENT_DATETIME()
  );

--------------///----------------
--Query for Table 2 crm_prd_info.
--------------///----------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.silver.crm_prd_info`;
CREATE TABLE `data-with-baraa-sql-projects.silver.crm_prd_info`
  (
    prd_id       INT64,
    cat_id       STRING,  --prd_key broken down into two columns: cat_id and prd_key.
    prd_key      STRING,  --this is the new prd_key.
    prd_nm       STRING,
    prd_cost     INT64,
    prd_line     STRING,
    prd_start_dt DATE,    --changed from DATETIME to DATE.
    prd_end_dt   DATE,    --changed from DATETIME to DATE.
    dwh_create_date DATETIME DEFAULT CURRENT_DATETIME()
  );

----------------///-------------------
--Query for Table 3 crm_sales_details.
----------------///-------------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.silver.crm_sales_details`;
CREATE TABLE silver.crm_sales_details 
  (
    sls_ord_num  STRING,
    sls_prd_key  STRING,
    sls_cust_id  INT64,
    sls_order_dt DATE,    --changed from INT to DATE
    sls_ship_dt  DATE,    --changed from INT to DATE
    sls_due_dt   DATE,    --changed from INT to DATE
    sls_sales    INT64,
    sls_quantity INT64,
    sls_price    FLOAT64, --changed from INT to FLOAT
    dwh_create_date DATETIME DEFAULT CURRENT_DATETIME()
  );

---------------///----------------
--Query for Table 4 erp_cust_az12.
---------------///----------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.silver.erp_cust_az12`;
CREATE TABLE silver.erp_cust_az12 
  (
    cid    STRING,
    bdate  DATE,
    gen    STRING,
    dwh_create_date DATETIME DEFAULT CURRENT_DATETIME()
  );

---------------///----------------
--Query for Table 5: erp_loc_a101.
---------------///----------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.silver.erp_loc_a101`;
CREATE TABLE silver.erp_loc_a101 
  (
    cid    STRING,
    cntry  STRING,
    dwh_create_date DATETIME DEFAULT CURRENT_DATETIME()
  );

----------------///-----------------
--Query for Table 6: erp_px_cat_g1v2
----------------///-----------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.silver.erp_px_cat_g1v2`; 
CREATE TABLE silver.erp_px_cat_g1v2 
  (
    id           STRING,
    cat          STRING,
    subcat       STRING,
    maintenance  BOOLEAN, --HERE. The original script was STRING data type. The corresponding column in bronze layer table became BOOLEAN data type during load job process. Gemini says this was because the 'auto detect of schema' selected during the process. So, regardless of the DDL preset, BigQuery will at least force BOOLEAN data type if it considers the data so.
    dwh_create_date DATETIME DEFAULT CURRENT_DATETIME()
  );