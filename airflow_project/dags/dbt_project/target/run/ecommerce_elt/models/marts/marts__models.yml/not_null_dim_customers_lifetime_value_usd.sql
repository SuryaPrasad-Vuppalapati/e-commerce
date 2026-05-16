
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lifetime_value_usd
from ECOMMERCE_DB.marts.dim_customers
where lifetime_value_usd is null



  
  
      
    ) dbt_internal_test