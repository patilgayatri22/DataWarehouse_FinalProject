WITH weather_data AS (
    SELECT
        *
    FROM weather.source.daily_weather
)

SELECT * FROM weather_data