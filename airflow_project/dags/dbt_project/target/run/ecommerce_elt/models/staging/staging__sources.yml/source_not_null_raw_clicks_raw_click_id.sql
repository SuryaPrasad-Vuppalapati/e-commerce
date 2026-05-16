
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select click_id
from ECOMMERCE_DB.raw.clicks_raw
where click_id is null



  
  
      
    ) dbt_internal_test