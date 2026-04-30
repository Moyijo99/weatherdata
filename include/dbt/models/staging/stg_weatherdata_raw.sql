WITH base AS (
    SELECT * FROM {{ source('raw', 'abuja_hourly') }}
),
typed AS (
    SELECT
        CAST(`time` AS TIMESTAMP) AS observed_at,
        CAST(temperature_c AS FLOAT64) AS temperature_c,
        CAST(precipitation_mm AS FLOAT64) AS precipitation_mm,
        CAST(windspeed_kmh AS FLOAT64) AS windspeed_kmh,
        CAST(`ingested_at` AS TIMESTAMP) AS ingested_at
    FROM base
    WHERE `time` IS NOT NULL
)
SELECT *
FROM typed
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY observed_at
    ORDER BY ingested_at DESC
) = 1
 