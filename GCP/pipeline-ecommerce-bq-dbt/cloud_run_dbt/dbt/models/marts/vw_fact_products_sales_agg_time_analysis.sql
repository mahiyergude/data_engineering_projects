{{ config(materialized='view') }}

WITH MonthlySales AS (
  -- Aggregate total sales for each product category per month
  
  SELECT
    P.product_category_name,
    EXTRACT(YEAR FROM O.order_purchase_timestamp) AS sales_year,
    EXTRACT(MONTH FROM O.order_purchase_timestamp) AS sales_month,
    SUM(IFNULL(I.price, 0) + IFNULL(I.freight_value, 0)) AS total_monthly_sales
  FROM
    {{ ref('fact_order_items') }} I
  
  LEFT JOIN
    {{ ref('dim_orders') }} O ON I.order_id = O.order_id
  
  LEFT JOIN
    {{ ref('dim_products') }} P ON I.product_id = P.product_id
  
  WHERE
    O.order_purchase_timestamp IS NOT NULL
    AND P.product_category_name IS NOT NULL
  
  GROUP BY
    1, 2, 3
),

MonthlyComparison AS (
  -- Calculate previous month and previous year's sales using LAG
  SELECT
    product_category_name,
    sales_year,
    sales_month,
    PARSE_DATE('%Y-%m', CAST(sales_year AS STRING) || '-' || LPAD(CAST(sales_month AS STRING), 2, '0')) as sales_month_date,
    total_monthly_sales,
    LAG(total_monthly_sales, 1) OVER (
        PARTITION BY product_category_name
        ORDER BY sales_year, sales_month
      ) AS previous_month_sales,
    LAG(total_monthly_sales, 12) OVER (
        PARTITION BY product_category_name
        ORDER BY sales_year, sales_month
      ) AS previous_year_sales
  FROM
    MonthlySales
)
-- Final Selection and Calculation of Changes
SELECT
  product_category_name,
  sales_year,
  sales_month,
  sales_month_date,
  total_monthly_sales,
  previous_month_sales,
  previous_year_sales,
  total_monthly_sales - previous_month_sales AS mom_change,
  SAFE_DIVIDE(
    (total_monthly_sales - previous_month_sales),
    previous_month_sales
  ) * 100 AS mom_change_pct,
  total_monthly_sales - previous_year_sales AS yoy_change,
  SAFE_DIVIDE(
    (total_monthly_sales - previous_year_sales),
    previous_year_sales
  ) * 100 AS yoy_change_pct
FROM
  MonthlyComparison
