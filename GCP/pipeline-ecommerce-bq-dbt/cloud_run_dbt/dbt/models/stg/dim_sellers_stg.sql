{{ config(materialized='table') }}

WITH GEOLOC AS (
    SELECT *
    FROM 
        (
        SELECT DISTINCT 
            geolocation_zip_code_prefix
            ,geolocation_city
            ,geolocation_state 
        FROM`ecommerce-analysis-455200.ecommerce_raw.olist_geolocation_dataset` 
        )

    --Removing cases where the zip_code is returning different city names
    QUALIFY COUNT(geolocation_zip_code_prefix) OVER(PARTITION BY geolocation_zip_code_prefix)=1
)

SELECT
    S.seller_id
    ,COALESCE(G.geolocation_city, S.seller_city) as city
    ,COALESCE(G.geolocation_state, S.seller_state) as state
    ,CURRENT_TIMESTAMP() AS last_extract_ts

FROM `ecommerce-analysis-455200.ecommerce_raw.olist_sellers_dataset` S

LEFT JOIN GEOLOC AS G ON
    1=1 AND
    S.seller_zip_code_prefix = G.geolocation_zip_code_prefix