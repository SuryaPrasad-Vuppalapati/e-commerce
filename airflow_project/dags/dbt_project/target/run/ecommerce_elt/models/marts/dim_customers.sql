
  
    

create or replace transient table ECOMMERCE_DB.marts.dim_customers
    
    
    
    as (

WITH orders AS (
    SELECT *
    FROM ECOMMERCE_DB.staging.stg_orders
    WHERE data_quality_flag = 'valid'
),

first_channel AS (
    SELECT
        customer_id,
        acquisition_channel
    FROM orders
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY order_date ASC
    ) = 1
),

customer_orders AS (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(net_amount_usd) AS lifetime_value_usd,
        AVG(net_amount_usd) AS avg_order_value_usd
    FROM orders
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    f.acquisition_channel,
    c.first_order_date,
    c.last_order_date,
    c.total_orders,
    ROUND(c.lifetime_value_usd, 2) AS lifetime_value_usd,
    ROUND(c.avg_order_value_usd, 2) AS avg_order_value_usd,
    DATE_TRUNC('month', c.first_order_date) AS acquisition_cohort_month,
    DATEDIFF('day', c.last_order_date, CURRENT_DATE()) AS days_since_last_order,
    CASE
        WHEN c.lifetime_value_usd >= 1000 THEN 'high_value'
        WHEN c.lifetime_value_usd >= 200 THEN 'mid_value'
        ELSE 'low_value'
    END AS customer_segment
FROM customer_orders c
LEFT JOIN first_channel f
    ON c.customer_id = f.customer_id
    )
;


  