WITH cumulative_precip_data AS (
    SELECT 
        datetime, 
        precip, 
        SUM(precip) OVER (ORDER BY datetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_precip
    FROM {{ ref('daily_weather') }}
)
SELECT 
    datetime, 
    precip, 
    cumulative_precip
FROM cumulative_precip_data
ORDER BY datetime DESC
