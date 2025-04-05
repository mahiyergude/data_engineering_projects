{{ config(materialized='table') }}

SELECT DISTINCT
    S.product_id
    ,S.product_category_name
    ,S.product_name_lenght
    ,S.product_description_lenght
    ,S.product_photos_qty
    ,S.product_weight_g
    ,S.product_length_cm
    ,S.product_height_cm
    ,S.product_width_cm
    ,CURRENT_TIMESTAMP() AS last_extract_ts

FROM `ecommerce-analysis-455200.ecommerce_raw.olist_products_dataset` S
 