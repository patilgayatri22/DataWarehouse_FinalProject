
  
    

        create or replace transient table weather.data.temperature_range
         as
        (WITH temperature_range AS (
    SELECT
        DATETIME,
        TEMPMAX,
        TEMPMIN,
        (TEMPMAX - TEMPMIN) AS TEMP_RANGE,
        CONDITIONS
    FROM weather.data.custom_daily_weather_view
)
SELECT 
    DATETIME, 
    TEMPMAX, 
    TEMPMIN, 
    TEMP_RANGE, 
    CONDITIONS
FROM temperature_range
ORDER BY DATETIME DESC
        );
      
  