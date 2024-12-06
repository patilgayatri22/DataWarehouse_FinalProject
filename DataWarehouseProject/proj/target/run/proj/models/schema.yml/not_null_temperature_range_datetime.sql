select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select datetime
from weather.data.temperature_range
where datetime is null



      
    ) dbt_internal_test