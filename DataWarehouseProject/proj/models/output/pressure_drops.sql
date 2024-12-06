WITH weather_data AS (
    SELECT 
        datetime, 
        pressure, 
        LAG(pressure) OVER (ORDER BY datetime) AS prev_pressure,
        (pressure - LAG(pressure) OVER (ORDER BY datetime)) AS pressure_change,
        CASE 
            WHEN (pressure - LAG(pressure) OVER (ORDER BY datetime)) < -2 THEN 'Yes'
            ELSE 'No'
        END AS pressure_drop
    FROM {{ ref('daily_weather') }}
)
SELECT 
    datetime, 
    pressure, 
    prev_pressure, 
    pressure_change, 
    pressure_drop
FROM weather_data