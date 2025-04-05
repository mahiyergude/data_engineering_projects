{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['order_id','order_item_id'],
        on_schema_change='fail',
        cluster_by = ["order_id","order_item_id"],
    )
}}

with source_stg as (
    SELECT *
    FROM {{ ref('fact_order_items_stg') }}
    
    {% if is_incremental() %}
        where last_extract_ts > (SELECT max(last_extract_ts) FROM {{ this }})
    {% endif %}
)

SELECT
    S.order_id
    ,S.order_item_id
    ,S.product_id
    ,S.seller_id
    ,S.shipping_limit_date
    ,S.price
    ,S.freight_value
    ,S.last_extract_ts
FROM source_stg S
