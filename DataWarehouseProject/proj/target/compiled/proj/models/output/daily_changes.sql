WITH weather_data AS (
    SELECT 
        datetime, 
        temp, 
        humidity,
        LAG(temp) OVER (ORDER BY datetime) AS prev_temp,
        LAG(humidity) OVER (ORDER BY datetime) AS prev_humidity
    FROM weather.data.custom_daily_weather_view
),
calculated_changes AS (
    SELECT 
        datetime, 
        temp, 
        prev_temp,
        ((temp - prev_temp) / NULLIF(prev_temp, 0)) * 100 AS temp_change,
        humidity, 
        prev_humidity,
        ((humidity - prev_humidity) / NULLIF(prev_humidity, 0)) * 100 AS humidity_change
    FROM weather_data
)
SELECT 
    datetime, 
    temp, 
    prev_temp,
    temp_change,
    humidity, 
    prev_humidity,
    humidity_change
FROM calculated_changes
ORDER BY datetime