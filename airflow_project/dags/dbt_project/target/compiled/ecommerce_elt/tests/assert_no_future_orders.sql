-- Returns rows if any orders have a future date — test fails if this returns anything
SELECT
    order_id,
    order_date,
    CURRENT_DATE() AS today
FROM ECOMMERCE_DB.marts.fact_orders
WHERE order_date > CURRENT_DATE()