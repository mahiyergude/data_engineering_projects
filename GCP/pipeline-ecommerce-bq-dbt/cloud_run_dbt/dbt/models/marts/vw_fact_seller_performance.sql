WITH AGG_SELLER_ORDER AS (
  SELECT
    I.seller_id,
    I.product_id,
    O.order_id,
    COUNT(order_item_id) as qty_itens,
    SUM(I.price+I.freight_value) total_sales,
  FROM
    {{ ref('fact_order_items') }} I
  
  LEFT JOIN
    {{ ref('dim_orders') }} O ON I.order_id = O.order_id
  
  GROUP BY
    1, 2, 3
)

SELECT
  seller_id,
  COUNT(DISTINCT order_id) AS num_orders,
  SUM(qty_itens) as total_itens,
  COUNT(DISTINCT product_id) AS num_distinct_products_sold,
  SUM(total_sales) AS total_sales_value,
  AVG(total_sales) AS avg_price_order


FROM AGG_SELLER_ORDER

GROUP BY 1
