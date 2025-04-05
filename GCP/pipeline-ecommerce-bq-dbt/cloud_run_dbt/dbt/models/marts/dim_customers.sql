{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        on_schema_change='fail',
        cluster_by = ["customer_state","customer_city","customer_id"],
        )
}}

with source_stg as (
    SELECT *
    FROM {{ ref('dim_customers_stg') }}
    
    {% if is_incremental() %}
        where last_extract_ts > (SELECT max(last_extract_ts) FROM {{ this }})
    {% endif %}
)

SELECT
    S.customer_id
    ,S.customer_unique_id
    ,S.customer_city
    ,S.customer_state
    ,S.last_extract_ts
FROM source_stg S
