
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select metric_date
from ECOMMERCE_DB.marts.fct_daily_metrics
where metric_date is null



  
  
      
    ) dbt_internal_test