---------------------------------------------------------------///----------------------------------------------------------------
/*  ---Data with Baraa SQL Full Course Practice Project---
           ---Script: Exploratory Data Analysis---

>>>Purpose:
This script serves as the SQL query to do exploratory data analysis (EDA) for the data warehouse we have created.

>>>Contents:
1.  Data Base Exploration
2.  Dimension Exploration
3.  Date Exploration
4.  Measures Exploration
5.  Magnitude - Aggregate Measures by Dimension
6.  Ranking Analysis

>>>Special Thanks:
To Baraa and his team for leading me into the data world.*/
---------------------------------------------------------------///----------------------------------------------------------------

#1 Data Base Exploration
#1.1 Check all tables in the Project/Data Warehouse.
SELECT
  table_catalog,
  table_schema,
  table_name,
  table_type
FROM
  `data-with-baraa-sql-projects`.`silver`.INFORMATION_SCHEMA.TABLES
UNION ALL
SELECT
  table_catalog,
  table_schema,
  table_name,
  table_type
FROM
  `data-with-baraa-sql-projects`.`gold`.INFORMATION_SCHEMA.TABLES
UNION ALL
SELECT
  table_catalog,
  table_schema,
  table_name,
  table_type
FROM
  `data-with-baraa-sql-projects`.`bronze`.INFORMATION_SCHEMA.TABLES;

#1.2 Check all columns information in one or all tables in the project.
SELECT column_name,
  data_type,
  table_schema,
  table_name
FROM
  `data-with-baraa-sql-projects`.gold.INFORMATION_SCHEMA.COLUMNS
WHERE
  table_name = 'fact_sales';

SELECT
  column_name,
  data_type,
  table_schema,
  table_name
FROM
  `data-with-baraa-sql-projects`.silver.INFORMATION_SCHEMA.COLUMNS
UNION ALL
SELECT
  column_name,
  data_type,
  table_schema,
  table_name
FROM
  `data-with-baraa-sql-projects`.gold.INFORMATION_SCHEMA.COLUMNS
UNION ALL
SELECT
  column_name,
  data_type,
  table_schema,
  table_name
FROM
  `data-with-baraa-sql-projects`.bronze.INFORMATION_SCHEMA.COLUMNS
;

#2 Dimension Exploration
#2.1 Identify unique values or categories in each dimension.
SELECT DISTINCT
  country
FROM
  `data-with-baraa-sql-projects.gold.dim_customer`;

SELECT DISTINCT
  category,
  sub_category,
  product_name
FROM
  `data-with-baraa-sql-projects.gold.dim_product`
ORDER BY
  1,2,3;

#3 Date Exploration
#3.1 Find the date of first and last order.
SELECT
  MIN(order_date) first_order_date,
  MAX(order_date) last_order_date,
  DATE_DIFF(MAX(order_date),MIN(order_date),YEAR)
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`;

#3.2 Find the oldest and youngest customer.
SELECT
  MIN(birthday) oldest_birthdate,
  DATE_DIFF(CURRENT_DATE(),MIN(birthday),YEAR) oldest_age,
  MAX(birthday) youngest_birthdate,
  DATE_DIFF(CURRENT_DATE(),MAX(birthday),YEAR) youngest_age
FROM
  `data-with-baraa-sql-projects.gold.dim_customer`;

#4 Measures Exploration
#4.1 Find the total Sales.
SELECT
  SUM(sales)
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`;
#4.2 Find the total Sales Quantity.
SELECT
  SUM(quantity)
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`;
#4.3 Find the average Selling Price.
SELECT
  ROUND(AVG(Price), 2) AS average_price
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`;
#4.4 Find the number of orders.
SELECT
  COUNT(order_number) AS total_orders,
  COUNT(DISTINCT order_number) AS total_unique_orders
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`;
#4.5 Find the number of products.
SELECT
  COUNT(DISTINCT product_name) AS total_products,
  COUNT(DISTINCT P.product_key) AS total_product_keys,
  COUNT(DISTINCT S.product_key) AS facts_total_product_keys
FROM
  `data-with-baraa-sql-projects.gold.fact_sales` S
FULL JOIN
  `data-with-baraa-sql-projects.gold.dim_product` P
ON
 S.product_key = P.product_key;

#4.6 Find the number of customers.
SELECT
  COUNT(customer_id) customer_id,
  COUNT(DISTINCT customer_id) total_unique_customer_id
FROM
  `data-with-baraa-sql-projects.gold.dim_customer`;

#4.7 Find the number of customers who placed an order.
SELECT
  COUNT(DISTINCT customer_key) AS total_customers_who_placed_an_order
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`;

#4.8 Generate a Business Report of key performances.
SELECT
  'total_sales' AS measure_name, 
  SUM(sales) AS measure_value
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`
UNION ALL
SELECT
  'total_quantity' AS measure_name,
  SUM(quantity) AS measure_value
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`
UNION ALL
SELECT
  'average_price' AS measure_name,
  ROUND(AVG(price), 2) AS measure_value
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`
UNION ALL
SELECT
  'total_orders' AS measure_name,
  COUNT(DISTINCT order_number) AS measure_value
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`
UNION ALL
SELECT
  'total_products' AS measure_name,
  COUNT(DISTINCT product_key) AS measure_value
FROM
  `data-with-baraa-sql-projects.gold.dim_product`
UNION ALL
SELECT
  'total_customers' AS measure_name,
  COUNT(DISTINCT customer_key) AS measure_value
FROM
  `data-with-baraa-sql-projects.gold.dim_customer`;

#5 Magnitude - Aggregate Measures by Dimension
#5.1 Find total customers by country.
SELECT
  COUNT(customer_key) AS total_customers,
  country
FROM
  `data-with-baraa-sql-projects.gold.dim_customer`
GROUP BY
  country
ORDER BY
  total_customers DESC;

#5.2 Find total customers by gender.
SELECT 
  COUNT(customer_key) AS total_customers,
  gender
FROM
  `data-with-baraa-sql-projects.gold.dim_customer`
GROUP BY
  gender
ORDER BY
  total_customers DESC;

#5.3 Find total products by category.
SELECT
  COUNT(DISTINCT product_key) AS total_products,
  category
FROM
  `data-with-baraa-sql-projects.gold.dim_product`
GROUP BY
  category
ORDER BY
  total_products DESC;

#5.4 Find the average product cost of each category.
SELECT 
  category, 
  ROUND(AVG(product_cost),2) AS average_product_cost
FROM 
  `data-with-baraa-sql-projects`.`gold`.`dim_product`
GROUP BY 
  category
ORDER BY 
  AVG(product_cost) DESC;

#5.5 Find the total sales of each category.
SELECT
  P.category,
  SUM(S.sales) AS total_sales
FROM
  `data-with-baraa-sql-projects.gold.fact_sales` S
LEFT JOIN
  `data-with-baraa-sql-projects.gold.dim_product` P
ON
  S.product_key = P.product_key
GROUP BY
  P.category
ORDER BY
  total_sales DESC;

#5.5 Find the total sales by each customer.
SELECT
  C.customer_key,
  C.first_name,
  C.last_name,
  SUM(S.sales) AS total_sales
FROM
  `data-with-baraa-sql-projects.gold.fact_sales` S
LEFT JOIN
  `data-with-baraa-sql-projects.gold.dim_customer` C
ON
  S.customer_key = C.customer_key
GROUP BY
  C.customer_key,
  C.first_name,
  C.last_name
ORDER BY
  total_sales DESC;

#5.6 Find total sales quantity of each country.
SELECT
  c.country,
  SUM(s.quantity) AS total_sales_quantity
FROM
  `data-with-baraa-sql-projects.gold.fact_sales` s
LEFT JOIN
  `data-with-baraa-sql-projects.gold.dim_customer` c
ON
  s.customer_key = c.customer_key
GROUP BY
  c.country
ORDER BY
  total_sales_quantity DESC;

#6 Ranking Analysis
#6.1 Rank the top 5 products in sales
SELECT
  ROW_NUMBER() OVER (ORDER BY SUM(S.sales) DESC) AS sales_rank,
  P.product_name,
  FORMAT("%'d", SUM(S.sales)) AS total_sales
FROM
  `data-with-baraa-sql-projects`.`gold`.`fact_sales` AS S
INNER JOIN
  `data-with-baraa-sql-projects`.`gold`.`dim_product` AS P
ON
  S.product_key = P.product_key
GROUP BY
  P.product_name
ORDER BY
  SUM(S.sales) DESC
LIMIT
  5;

#6.2 Rank the bottom 5 selling products.
SELECT
  ROW_NUMBER() OVER (ORDER BY SUM(S.sales) ASC) AS sales_rank,
  P.product_name,
  FORMAT("%'d", SUM(S.sales)) AS total_sales
FROM `data-with-baraa-sql-projects`.`gold`.`fact_sales` AS S
INNER JOIN `data-with-baraa-sql-projects`.`gold`.`dim_product` AS P
  ON S.product_key = P.product_key
GROUP BY P.product_name
ORDER BY SUM(S.sales) ASC
LIMIT 5;