{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        on_schema_change='fail',
        partition_by={
            "field": "order_purchase_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ["order_status","customer_id"],
    )
}}

with source_stg as (
    SELECT *
    FROM {{ ref('dim_orders_stg') }}
    
    {% if is_incremental() %}
        where last_extract_ts > (SELECT max(last_extract_ts) FROM {{ this }})
    {% endif %}
)

SELECT
    S.order_id
    ,S.customer_id
    ,S.order_status
    ,S.order_purchase_timestamp
    ,S.order_approved_at
    ,S.order_delivered_carrier_date
    ,S.order_delivered_customer_date
    ,S.order_estimated_delivery_date
    ,S.last_extract_ts
FROM source_stg S
