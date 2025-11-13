---------------------------------------------------------------///----------------------------------------------------------------
/*  ---Data with Baraa SQL Full Course Practice Project---
           ---Script: Advanced Data Analytics---

>>>Purpose:
1. This set of Queries are the practises I wrote in BigQuery under the instruction of Baraa in the last section of the practise project of his full SQL course.
2. They cover a large part of SQL skills such as Aggregation, CTE, Subqueries, Window Function, Case Statements, as well as typical business analysis practise including building a report on customer and product dimenstions.

>>>Contents:
1. Changes over Time.
2. cumulative analysis.
3. performance analysis.
4. part-to-whole analysis.
5. data segmentation.
6. creating report for customers and products.

>>>Special Thanks:
To Baraa and his team for leading me into the data world.*/
---------------------------------------------------------------///----------------------------------------------------------------

#1 Changes over Time
#1.1 Find the key sales figures of each year.
SELECT
  EXTRACT(YEAR FROM order_date) AS year,
  FORMAT("%'d",SUM(quantity)) AS total_quantity,
  FORMAT("%'d",SUM(sales)) AS total_sales,
  FORMAT("%'d",COUNT(DISTINCT order_number)) AS total_orders,
  FORMAT("%'d",COUNT(DISTINCT customer_key)) AS total_customers
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`
WHERE
 order_date IS NOT NULL
GROUP BY
  year
ORDER BY
  year DESC;

#1.2 Find the key figures of each month in year 2012.
SELECT
  FORMAT_DATE("%m", order_date) AS month,
  FORMAT("%'d",SUM(quantity)) AS total_quantity,
  FORMAT("%'d",SUM(sales)) AS total_sales,
  FORMAT("%'d",COUNT(DISTINCT order_number)) AS total_orders,
  FORMAT("%'d",COUNT(DISTINCT customer_key)) AS total_customers
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`
WHERE
  order_date IS NOT NULL
  AND
  EXTRACT(YEAR FROM order_date) = 2012
GROUP BY
  month
ORDER BY
  month;

#1.3 Find the key figures of each month of each year.
SELECT
  EXTRACT(YEAR FROM order_date) AS year,
  EXTRACT(MONTH FROM order_date) AS month,
  FORMAT("%'d",SUM(quantity)) AS total_quantity,
  FORMAT("%'d",SUM(sales)) AS total_sales,
  FORMAT("%'d",COUNT(DISTINCT order_number)) AS total_orders,
  FORMAT("%'d",COUNT(DISTINCT customer_key)) AS total_customers
FROM
  `data-with-baraa-sql-projects.gold.fact_sales`
WHERE
  order_date IS NOT NULL
GROUP BY
  year,
  month
ORDER BY
  year,
  month;

#2 Cumulative Analysis
#2.1 Find the running total of sales by months throughout the database
SELECT
  month_start,
  monthly_sales,
  SUM(monthly_sales) OVER (ORDER BY month_start) AS running_total_sales
FROM 
  (
    SELECT
      DATE_TRUNC(fact_sales.order_date, MONTH) AS month_start,
      SUM(fact_sales.sales) AS monthly_sales
    FROM
      `data-with-baraa-sql-projects`.`gold`.`fact_sales` AS fact_sales
    WHERE
      order_date IS NOT NULL
    GROUP BY
      month_start
  )
ORDER BY
  month_start;

#2.2 Find the running total of sales and running average of price of every month in each year of the dataset.
SELECT
  month_start,
  monthly_sales,
  SUM(monthly_sales) OVER (PARTITION BY DATE_TRUNC(month_start, YEAR) ORDER BY month_start) AS running_total_sales,
  ROUND(average_price, 2) AS average_price,
  ROUND( AVG(average_price) OVER (PARTITION BY DATE_TRUNC(month_start, YEAR) ORDER BY month_start), 2) AS running_average_price
FROM 
  (
    SELECT
      DATE_TRUNC(fact_sales.order_date, MONTH) AS month_start,
      SUM(fact_sales.sales) AS monthly_sales,
      AVG(fact_sales.price) AS average_price
    FROM
      `data-with-baraa-sql-projects`.`gold`.`fact_sales` AS fact_sales
    WHERE 
      fact_sales.order_date IS NOT NULL
    GROUP BY
      month_start
  )
ORDER BY
  month_start;

#3 Performance Analysis - Use Window Function to compare key business performance indicaters.
WITH product_year_sales AS
  (
    SELECT
      DATE_TRUNC(order_date, YEAR) AS year,
      product_name,
      SUM(sales) AS total_sales
    FROM
      `data-with-baraa-sql-projects.gold.fact_sales` S
    LEFT JOIN
      `data-with-baraa-sql-projects.gold.dim_product` P
    ON
    S.product_key = P.product_key
    WHERE
      order_date IS NOT NULL
    GROUP BY
      year,
      product_name
  )
SELECT
  product_name,
  year,
  total_sales,
  ROUND((total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY year)), 2) AS yoy_change
FROM
  product_year_sales
ORDER BY
  product_name,
  year;

#4 Part-to-Whole Analysis
WITH category_sales AS
  (
    SELECT
      P.category,
      P.sub_category,
      SUM(sales) AS total_sales
    FROM
      `data-with-baraa-sql-projects.gold.fact_sales` S
    LEFT JOIN
      `data-with-baraa-sql-projects.gold.dim_product` P
    ON
      S.product_key = P.product_key
    GROUP BY
      P.category,
      P.sub_category
  )
SELECT
  category,
  sub_category,
  total_sales,
  CONCAT(ROUND((total_sales / sum(total_sales) OVER ())*100, 2),"%") AS percentage_of_total_sales,
  sum(total_sales) OVER (PARTITION BY category),
  CONCAT(ROUND((total_sales / sum(total_sales) OVER (PARTITION BY category))*100, 2),"%") AS percentage_of_category_sales
FROM
  category_sales
ORDER BY
  category,
  sub_category;

#5 Data Segmentation - Measure by Measure, Categorization
#5.1 Find the margin of the products and segment them into 4 groups <25%, 25%-50%, 50%-75%, >75%.
WITH ProductMargin AS 
  (
    SELECT
      P.product_key,
      P.product_name,
      P.product_cost,
      AVG(S.price) AS average_sales_price,
      (AVG(S.price) - P.product_cost) / AVG(S.price) AS calculated_margin
    FROM `data-with-baraa-sql-projects`.`gold`.`fact_sales` AS S
    LEFT JOIN `data-with-baraa-sql-projects`.`gold`.`dim_product` AS P
    ON S.product_key = P.product_key
    GROUP BY P.product_key, P.product_name, P.product_cost
  )
SELECT
  margin_group,
  COUNT(product_name) AS product_count
FROM
  (
    SELECT
      product_name,
      ROUND(calculated_margin * 100, 2) AS margin_percentage,
      CASE
        WHEN calculated_margin < 0.25 THEN '0-25%'
        WHEN calculated_margin >= 0.25 AND calculated_margin < 0.50 THEN '25%-50%'
        WHEN calculated_margin >= 0.50 AND calculated_margin < 0.75 THEN '50%-75%'
        WHEN calculated_margin >= 0.75 THEN '75% Above'
        ELSE 'N/A'
        END
        AS margin_group
    FROM `ProductMargin`
    ORDER BY margin_percentage DESC
  )
GROUP BY
  margin_group
ORDER BY
  margin_group;

#5.2 Group customers into three categories "VIP", "Regular" and "New", and count their respective numbers.
WITH 
  CustomerSales AS 
  (
    SELECT
      fact_sales.customer_key,
      SUM(fact_sales.sales) AS total_sales,
      MIN(fact_sales.order_date) AS first_order_date,
      MAX(fact_sales.order_date) AS last_order_date
    FROM `data-with-baraa-sql-projects`.`gold`.`fact_sales` AS fact_sales
    WHERE fact_sales.order_date IS NOT NULL
    GROUP BY fact_sales.customer_key
  ),
  RankedCustomers AS 
  (
    SELECT
      customer_key,
      total_sales,
      first_order_date,
      last_order_date,
      NTILE(10) OVER (ORDER BY total_sales DESC) AS sales_quartile  -- Divide customers into 10 equal groups based on sales.
    FROM `CustomerSales`
  ),
  CustomerCategories AS (
    SELECT
      customer_key,
      CASE
        WHEN sales_quartile = 1 
          THEN 'VIP'  -- Top 10% by sales
        WHEN first_order_date >= DATE_SUB(last_order_date, INTERVAL 3 MONTH)
          THEN 'New'  -- First order within the last year
        ELSE 'Regular'
        END
        AS customer_category
    FROM `RankedCustomers`
  )
SELECT 
  customer_category, 
  COUNT(DISTINCT customer_key) AS count_of_customers
FROM 
  `CustomerCategories`
GROUP BY 
  customer_category
ORDER BY 
  count_of_customers DESC;

#6 Creating Report
#6.1 Customer Report
DROP VIEW IF EXISTS `data-with-baraa-sql-projects.gold.report_customers`;
CREATE VIEW `data-with-baraa-sql-projects.gold.report_customers` AS
WITH base_query AS
  (
    SELECT
      S.order_number,
      S.product_key,
      S.order_date,
      S.quantity,
      S.sales,
      C.customer_key,
      C.customer_number,
      CONCAT(C.first_name,' ',C.last_name) name,
      DATE_DIFF(CURRENT_DATE(), C.birthday, YEAR) age
    FROM
      `data-with-baraa-sql-projects.gold.fact_sales` S
    LEFT JOIN
      `data-with-baraa-sql-projects.gold.dim_customer` C
    ON
      S.customer_key = C.customer_key
    WHERE
      S.order_date IS NOT NULL
  )
,customer_aggregation AS
  (
    SELECT
      customer_key,
      customer_number,
      name,
      age,
      MAX(order_date) AS last_order_date,
      COUNT(DISTINCT order_number) AS total_orders,
      SUM(quantity) AS total_quantity,
      SUM(sales) AS total_sales,
      COUNT(DISTINCT product_key) AS total_products,
      NTILE(10) OVER(ORDER BY SUM(sales) DESC) AS sales_quartile,
      DATE_DIFF(MAX(order_date), MIN(order_date), MONTH) AS order_lifespan
    FROM
      base_query
    GROUP BY
      customer_key,
      customer_number,
      name,
      age
  )
SELECT
  customer_aggregation.customer_key,
  customer_aggregation.customer_number,
  customer_aggregation.name,
  customer_aggregation.age,
  CASE 
    WHEN age<20 THEN 'Under 20'
    WHEN age BETWEEN 20 and 29 THEN '20-29'
    WHEN age BETWEEN 30 and 39 THEN '30-39'
    WHEN age BETWEEN 40 and 49 THEN '40-49'
    ELSE '50 and above'
  END AS age_group,
  CASE
    WHEN order_lifespan >= 12 AND sales_quartile = 1 THEN 'VIP'
    WHEN order_lifespan >= 12 AND sales_quartile <>1 THEN 'REGULAR'
    ELSE 'NEW'
  END AS customer_category,
  customer_aggregation.order_lifespan,
  DATE_DIFF(CURRENT_DATE(), last_order_date, MONTH) AS months_since_last_order,
  customer_aggregation.total_orders,
  customer_aggregation.total_products,
  customer_aggregation.total_quantity,
  customer_aggregation.total_sales,
  CASE
    WHEN customer_aggregation.total_sales = 0 THEN 0
    ELSE ROUND(customer_aggregation.total_sales / customer_aggregation.total_orders, 2)
  END AS average_order_value,
  CASE
    WHEN order_lifespan = 0 THEN customer_aggregation.total_sales
    ELSE ROUND(customer_aggregation.total_sales / order_lifespan, 2)
  END AS average_monthly_spending
FROM
  customer_aggregation;

#6.2 Product Report.
DROP VIEW IF EXISTS `data-with-baraa-sql-projects.gold.report_products`;

CREATE VIEW `data-with-baraa-sql-projects.gold.report_products` AS
WITH base_query AS
  (
    SELECT
      S.order_number,
      S.customer_key,
      S.product_key,
      S.order_date,
      S.quantity,
      S.sales,
      P.product_name,
      P.category,
      P.sub_category,
      P.product_cost
    FROM
      `data-with-baraa-sql-projects.gold.fact_sales` S
    LEFT JOIN
      `data-with-baraa-sql-projects.gold.dim_product` P
    ON
      S.product_key = P.product_key
    WHERE
      S.order_date IS NOT NULL 
  )
,product_aggregation AS
  (
    SELECT
      product_key,
      product_name,
      category,
      sub_category,
      product_cost,
      COUNT(DISTINCT order_number) AS total_orders,
      COUNT(DISTINCT customer_key) AS total_customers,
      SUM(quantity) AS total_quantity,
      SUM(sales) AS total_sales,
      AVG(quantity) AS average_quantity,
      AVG(sales) AS average_sales,
      DATE_DIFF(MAX(order_date), MIN(order_date), MONTH) AS order_lifespan,
      MAX(order_date) AS last_order_date
    FROM
      base_query
    GROUP BY 
      product_key,
      product_name,
      category,
      sub_category,
      product_cost
  )
SELECT
  product_aggregation.product_key,
  product_aggregation.product_name,
  product_aggregation.category,
  product_aggregation.sub_category,
  product_aggregation.product_cost,
  product_aggregation.total_orders,
  product_aggregation.total_customers,
  product_aggregation.total_quantity,
  product_aggregation.average_quantity,
  product_aggregation.total_sales,
  product_aggregation.average_sales,
  product_aggregation.order_lifespan,
  DATE_DIFF(CURRENT_DATE(), product_aggregation.last_order_date, MONTH) AS months_since_last_sales,
  CASE
    WHEN product_aggregation.total_sales = 0 THEN 0
    ELSE ROUND(product_aggregation.total_sales / product_aggregation.total_orders, 2)
  END AS average_order_value,
  CASE
    WHEN order_lifespan = 0 THEN product_aggregation.total_sales
    ELSE ROUND(product_aggregation.total_sales / order_lifespan, 2)
  END AS average_monthly_sales
FROM
  product_aggregation
ORDER BY
  product_aggregation.total_sales DESC;