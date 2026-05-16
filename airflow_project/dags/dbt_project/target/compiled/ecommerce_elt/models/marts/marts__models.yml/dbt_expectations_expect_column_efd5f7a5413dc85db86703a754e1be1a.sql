






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and order_amount_usd >= 0 and order_amount_usd <= 50000
)
 as expression


    from ECOMMERCE_DB.marts.fact_orders
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







