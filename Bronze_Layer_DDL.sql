/*  ---Data with Baraa SQL Full Course Practice Project---
              ---Script: Bronze Layer DDL---

>>>Purpose:
This script serves as the DDL of all tables in the Bronze Layer.

>>>System:
Google Cloud BigQuery

>>>Contents:
1. Schema(Dataset in BigQuery) Creation of project.bronze.
2. Drop and Create tables including all their columns in Bronze Layer.

>>>Notes:
1. According to the Separation Of Concern Principle, the Bronze Layer is in charge of ingesting data from source systems(in this project, 'namely' the ERP and CRM system).
2. These DDL queries follows strictly with the column definition of the preset raw csv data.
3. This script creates the first schema and tables in the DBMS for the whole project.

>>>Special Thanks:
To Baraa and his team for leading me into the data world.*/

CREATE SCHEMA IF NOT EXISTS `data-with-baraa-sql-projects.bronze`;

----------------///---------------
--Query for Table 1 crm_cust_info.
----------------///---------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.bronze.crm_cust_info`;
CREATE TABLE `data-with-baraa-sql-projects.bronze.crm_cust_info`
  (
  cst_id INT64,
  cst_key STRING,
  cst_firstname STRING,
  cst_lastname STRING,
  cst_marital_status STRING,
  cst_gndr STRING,
  cst_create_date DATE
  );

--------------///----------------
--Query for Table 2 crm_prd_info.
--------------///----------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.bronze.crm_prd_info`;
CREATE TABLE `data-with-baraa-sql-projects.bronze.crm_prd_info`
  (
    prd_id       INT64,
    prd_key      STRING,
    prd_nm       STRING,
    prd_cost     INT64,
    prd_line     STRING,
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
  );

----------------///-------------------
--Query for Table 3 crm_sales_details.
----------------///-------------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.bronze.crm_sales_details`;
CREATE TABLE bronze.crm_sales_details 
  (
    sls_ord_num  STRING,
    sls_prd_key  STRING,
    sls_cust_id  INT64,
    sls_order_dt INT64,
    sls_ship_dt  INT64,
    sls_due_dt   INT64,
    sls_sales    INT64,
    sls_quantity INT64,
    sls_price    INT64
  );

---------------///----------------
--Query for Table 4 erp_cust_az12.
---------------///----------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.bronze.erp_cust_az12`;
CREATE TABLE bronze.erp_cust_az12 
  (
    cid    STRING,
    bdate  DATE,
    gen    STRING
  );

---------------///----------------
--Query for Table 5: erp_loc_a101.
---------------///----------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.bronze.erp_loc_a101`;
CREATE TABLE bronze.erp_loc_a101 
  (
    cid    STRING,
    cntry  STRING
  );

----------------///-----------------
--Query for Table 6: erp_px_cat_g1v2
----------------///-----------------
DROP TABLE IF EXISTS `data-with-baraa-sql-projects.bronze.erp_px_cat_g1v2`; 
CREATE TABLE bronze.erp_px_cat_g1v2 
  (
    id           STRING,
    cat          STRING,
    subcat       STRING,
    maintenance  STRING
  );