
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select session_id
from ECOMMERCE_DB.raw.sessions_raw
where session_id is null



  
  
      
    ) dbt_internal_test