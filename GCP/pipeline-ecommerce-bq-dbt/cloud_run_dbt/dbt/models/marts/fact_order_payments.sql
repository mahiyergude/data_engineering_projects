{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['order_id','payment_sequential'],
        on_schema_change='fail',
        cluster_by = ["payment_type","order_id"],
    )
}}

with source_stg as (
    SELECT *
    FROM {{ ref('fact_order_payments_stg') }}
    
    {% if is_incremental() %}
        where last_extract_ts > (SELECT max(last_extract_ts) FROM {{ this }})
    {% endif %}
)

SELECT
    S.order_id
    ,S.payment_sequential
    ,S.payment_type
    ,S.payment_installments
    ,S.payment_value
    ,S.last_extract_ts
FROM source_stg S
