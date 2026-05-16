






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and cvr_pct >= 0 and cvr_pct <= 100
)
 as expression


    from ECOMMERCE_DB.marts.fct_daily_metrics
    

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







