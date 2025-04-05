{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='seller_id',
        on_schema_change='fail',
        cluster_by = ["city","state"],
    )
}}

with source_stg as (
    SELECT *
    FROM {{ ref('dim_sellers_stg') }}
    
    {% if is_incremental() %}
        where last_extract_ts > (SELECT max(last_extract_ts) FROM {{ this }})
    {% endif %}
)

SELECT
    S.seller_id
    ,S.city
    ,S.state
    ,S.last_extract_ts
FROM source_stg S
