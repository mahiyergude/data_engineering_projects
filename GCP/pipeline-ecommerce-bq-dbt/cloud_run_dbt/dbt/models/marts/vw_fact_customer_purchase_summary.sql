{{ config(materialized='view') }}

WITH AGG_CUSTOMER_ORDER AS (
  SELECT
    C.customer_unique_id,
    O.order_id,
    O.order_purchase_timestamp,
    COUNT(order_item_id) as qty_itens,
    SUM(I.price+I.freight_value) total_purchased_value,
  FROM
    {{ ref('fact_order_items') }} I
  
  LEFT JOIN
    {{ ref('dim_orders') }} O ON I.order_id = O.order_id
  
  LEFT JOIN
    {{ ref('dim_customers') }} C ON C.customer_id = O.customer_id
  
  GROUP BY
    1, 2, 3
)

SELECT
  customer_unique_id,
  COUNT(DISTINCT order_id) AS num_orders,
  SUM(qty_itens) as total_itens,
  SUM(total_purchased_value) AS total_purchased_value,
  MIN(order_purchase_timestamp) first_order_purchase_timestamp,
  MAX(order_purchase_timestamp) last_order_purchase_timestamp

FROM AGG_CUSTOMER_ORDER

GROUP BY 1
