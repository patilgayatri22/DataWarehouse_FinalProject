
    
    

select
    datetime as unique_field,
    count(*) as n_records

from weather.data.temperature_range
where datetime is not null
group by datetime
having count(*) > 1


