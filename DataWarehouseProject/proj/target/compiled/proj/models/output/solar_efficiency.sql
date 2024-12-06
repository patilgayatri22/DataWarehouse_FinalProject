WITH weather_data AS (
    SELECT 
        DATETIME, 
        solarradiation, 
        cloudcover, 
        CASE 
            WHEN cloudcover > 0 THEN solarradiation / cloudcover
            ELSE NULL
        END AS solar_efficiency
    FROM weather.data.custom_daily_weather_view
)
SELECT 
    DATETIME, 
    solarradiation, 
    cloudcover, 
    solar_efficiency
FROM weather_data