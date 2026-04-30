-- Daily rollups of hourly *forecast* data (Open-Meteo), not observed station readings.
SELECT
    DATE(observed_at, 'Africa/Lagos') AS date_day,
    ROUND(AVG(temperature_c), 2) AS avg_temperature_c,
    ROUND(MAX(temperature_c), 2) AS max_temperature_c,
    ROUND(SUM(precipitation_mm), 2) AS total_precipitation_mm,
    ROUND(MAX(windspeed_kmh), 2) AS max_windspeed_kmh,
    CASE
        WHEN SUM(precipitation_mm) > 20 THEN 'High Risk'
        WHEN SUM(precipitation_mm) > 10 THEN 'Moderate Risk'
        WHEN SUM(precipitation_mm) > 5 THEN 'Low Risk'
        ELSE 'No Risk'
    END AS precipitation_risk
FROM {{ ref('stg_weatherdata_raw') }}
GROUP BY date_day
ORDER BY date_day DESC
