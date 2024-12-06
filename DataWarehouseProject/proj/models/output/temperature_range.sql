WITH temperature_range AS (
    SELECT
        DATETIME,
        TEMPMAX,
        TEMPMIN,
        (TEMPMAX - TEMPMIN) AS TEMP_RANGE,
        CONDITIONS
    FROM {{ ref('daily_weather') }}
)
SELECT 
    DATETIME, 
    TEMPMAX, 
    TEMPMIN, 
    TEMP_RANGE, 
    CONDITIONS
FROM temperature_range
ORDER BY DATETIME DESC