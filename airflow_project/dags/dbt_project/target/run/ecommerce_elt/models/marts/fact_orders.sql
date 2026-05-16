
  
    

create or replace transient table ECOMMERCE_DB.marts.fact_orders
    
    
    
    as (select * from (
            

WITH orders AS (
    SELECT * FROM ECOMMERCE_DB.staging.stg_orders
    WHERE data_quality_flag = 'valid'
),

products AS (
    SELECT product_id, product_category, product_name
    FROM ECOMMERCE_DB.marts.dim_products
),

first_orders AS (
    SELECT customer_id, MIN(order_date) AS first_order_date
    FROM ECOMMERCE_DB.staging.stg_orders
    WHERE data_quality_flag = 'valid'
    GROUP BY 1
)

SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.order_timestamp,
    o.order_status,
    o.order_amount_usd,
    o.discount_pct,
    o.net_amount_usd,
    o.currency,
    o.acquisition_channel,
    o.shipping_country,
    p.product_category,
    p.product_name,
    CASE
        WHEN o.order_date = f.first_order_date THEN TRUE
        ELSE FALSE
    END AS is_first_order,
    o._loaded_at

FROM orders o
LEFT JOIN products     p ON o.product_id  = p.product_id
LEFT JOIN first_orders f ON o.customer_id = f.customer_id
        )
        order by (
            order_date
        )
    )
;

alter table ECOMMERCE_DB.marts.fact_orders cluster by (order_date);
  