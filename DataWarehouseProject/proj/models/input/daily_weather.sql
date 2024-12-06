WITH weather_data AS (
    SELECT
        *
    FROM {{ source('raw_data', 'daily_weather') }}
)

SELECT * FROM weather_data