SELECT
  DATE(TIMESTAMP(CONCAT(time, ':00'))) AS date,
  EXTRACT(YEAR FROM TIMESTAMP(CONCAT(time, ':00'))) AS year,
  EXTRACT(MONTH FROM TIMESTAMP(CONCAT(time, ':00'))) AS month,
  
  -- Temperature metrics
  MAX(temperature_2m) AS max_temperature,
  MIN(temperature_2m) AS min_temperature,
  AVG(temperature_2m) AS avg_temperature,
  
  -- Dewpoint and atmospheric conditions
  AVG(dewpoint_2m) AS avg_dewpoint,
  AVG(pressure_msl) AS avg_pressure,
  AVG(cloudcover) AS avg_cloudcover,
  AVG(visibility) AS avg_visibility,
  
  -- Precipitation and humidity
  SUM(precipitation) AS total_precipitation,
  AVG(relative_humidity_2m) AS avg_humidity,
  
  -- Wind metrics
  AVG(wind_speed_10m) AS avg_wind_speed,
  AVG(wind_direction_10m) AS avg_wind_direction,
  
  -- Categorize wind direction
  CASE 
    WHEN AVG(wind_direction_10m) BETWEEN 0 AND 45 OR AVG(wind_direction_10m) BETWEEN 315 AND 360 THEN 'North'
    WHEN AVG(wind_direction_10m) BETWEEN 46 AND 135 THEN 'East'
    WHEN AVG(wind_direction_10m) BETWEEN 136 AND 225 THEN 'South'
    WHEN AVG(wind_direction_10m) BETWEEN 226 AND 314 THEN 'West'
    ELSE 'Unknown'
  END AS wind_direction_category,
  
  -- Weather event counts
  weathercode,
  COUNT(*) AS record_count

FROM `weather-batch-project.weather_dataset.weather_table`

GROUP BY date, year, month, weathercode

ORDER BY date, weathercode;
