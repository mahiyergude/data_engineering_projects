{{ config(materialized='table') }}


SELECT
    S.order_id
    ,S.payment_sequential
    ,S.payment_type
    ,S.payment_installments
    ,S.payment_value
    ,CURRENT_TIMESTAMP() AS last_extract_ts

FROM `ecommerce-demo-123.ecommerce_raw.olist_order_payments_dataset` S
