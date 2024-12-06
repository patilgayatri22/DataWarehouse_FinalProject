WITH daylight_hours_data AS (
    SELECT 
        datetime, 
        CAST(CONCAT(datetime, ' ', sunrise) AS TIMESTAMP) AS sunrise_ts,
        CAST(CONCAT(datetime, ' ', sunset) AS TIMESTAMP) AS sunset_ts,
        DATEDIFF('second', 
                 CAST(CONCAT(datetime, ' ', sunrise) AS TIMESTAMP), 
                 CAST(CONCAT(datetime, ' ', sunset) AS TIMESTAMP)
        ) / 3600.0 AS daylight_hours
    FROM {{ ref('daily_weather') }}
)
SELECT 
    datetime, 
    sunrise_ts AS sunrise, 
    sunset_ts AS sunset, 
    daylight_hours
FROM daylight_hours_data
ORDER BY datetime DESC
