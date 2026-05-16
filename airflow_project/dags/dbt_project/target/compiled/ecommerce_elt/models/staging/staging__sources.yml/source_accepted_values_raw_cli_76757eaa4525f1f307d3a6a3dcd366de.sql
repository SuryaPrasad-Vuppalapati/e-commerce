
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from ECOMMERCE_DB.raw.clicks_raw
    group by event_type

)

select *
from all_values
where value_field not in (
    'page_view','product_view','add_to_cart','checkout','purchase'
)


