
  create or replace   view weather.data.custom_daily_weather_view
  
   as (
    WITH weather_data AS (
    SELECT
        *
    FROM weather.source.daily_weather
)

SELECT * FROM weather_data
  );

