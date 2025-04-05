{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='product_id',
        on_schema_change='fail',
        cluster_by = ["product_category_name"],
    )
}}

with source_stg as (
    SELECT *
    FROM {{ ref('dim_products_stg') }}
    
    {% if is_incremental() %}
        where last_extract_ts > (SELECT max(last_extract_ts) FROM {{ this }})
    {% endif %}
)

SELECT
    S.product_id
    ,S.product_category_name
    ,S.product_name_lenght
    ,S.product_description_lenght
    ,S.product_photos_qty
    ,S.product_weight_g
    ,S.product_length_cm
    ,S.product_height_cm
    ,S.product_width_cm
    ,S.last_extract_ts
FROM source_stg S
