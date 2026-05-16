{{ config(materialized='table') }}

WITH orders AS (
    SELECT DISTINCT product_id FROM {{ ref('stg_orders') }}
    WHERE product_id IS NOT NULL
)

SELECT
    product_id,
    'Product ' || UPPER(SUBSTR(product_id, 6))           AS product_name,
    CASE
        WHEN RIGHT(product_id, 3)::INT BETWEEN 1   AND 100 THEN 'Electronics'
        WHEN RIGHT(product_id, 3)::INT BETWEEN 101 AND 200 THEN 'Clothing'
        WHEN RIGHT(product_id, 3)::INT BETWEEN 201 AND 300 THEN 'Home & Garden'
        WHEN RIGHT(product_id, 3)::INT BETWEEN 301 AND 400 THEN 'Sports'
        ELSE 'Other'
    END AS product_category,
    ROUND(10 + (RIGHT(product_id, 3)::INT * 0.5), 2)    AS base_price_usd
FROM orders