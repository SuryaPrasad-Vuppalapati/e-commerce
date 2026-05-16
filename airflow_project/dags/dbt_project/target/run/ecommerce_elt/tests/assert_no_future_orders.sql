
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Returns rows if any orders have a future date — test fails if this returns anything
SELECT
    order_id,
    order_date,
    CURRENT_DATE() AS today
FROM ECOMMERCE_DB.marts.fact_orders
WHERE order_date > CURRENT_DATE()
  
  
      
    ) dbt_internal_test