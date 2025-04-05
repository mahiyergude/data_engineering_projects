{{ config(materialized='table') }}


SELECT
    S.order_id
    ,S.order_item_id
    ,S.product_id
    ,S.seller_id
    ,S.shipping_limit_date
    ,S.price
    ,S.freight_value
    ,CURRENT_TIMESTAMP() AS last_extract_ts

FROM `ecommerce-analysis-455200.ecommerce_raw.olist_order_items_dataset` S