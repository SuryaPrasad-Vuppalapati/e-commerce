
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        acquisition_channel as value_field,
        count(*) as n_records

    from ECOMMERCE_DB.marts.fact_orders
    group by acquisition_channel

)

select *
from all_values
where value_field not in (
    'organic','paid_search','email','social','direct','affiliate'
)



  
  
      
    ) dbt_internal_test