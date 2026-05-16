






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and lifetime_value_usd >= 0 and lifetime_value_usd <= 1000000
)
 as expression


    from ECOMMERCE_DB.marts.dim_customers
    

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







