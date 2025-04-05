{{ config(materialized='table') }}


SELECT
    S.order_id
    ,S.customer_id
    ,S.order_status
    ,S.order_purchase_timestamp
    ,S.order_approved_at
    ,S.order_delivered_carrier_date
    ,S.order_delivered_customer_date
    ,S.order_estimated_delivery_date
    ,CURRENT_TIMESTAMP() AS last_extract_ts

FROM `ecommerce-analysis-455200.ecommerce_raw.olist_orders_dataset` S