-- 1) Passes and speed per hour (Brasília time)
CREATE OR REFRESH MATERIALIZED VIEW veloradar.gold.passes_by_hour
COMMENT 'Passes and speed statistics per hour, in America/Sao_Paulo local time'
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
AS SELECT
  CAST(date_trunc('HOUR', convert_timezone('America/Sao_Paulo', event_time)) AS TIMESTAMP_NTZ) AS hour_local,
  COUNT(*)                 AS total_passes,
  ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
  ROUND(MAX(speed_kmh), 2) AS max_speed_kmh
FROM veloradar.silver.events
GROUP BY 1;

-- 2) Speed statistics per direction
CREATE OR REFRESH MATERIALIZED VIEW veloradar.gold.speed_by_direction
COMMENT 'Pass count and speed statistics per direction'
AS SELECT
  direction,
  COUNT(*)                  AS total_passes,
  ROUND(AVG(speed_kmh), 2)  AS avg_speed_kmh,
  ROUND(MAX(speed_kmh), 2)  AS max_speed_kmh,
  ROUND(MIN(speed_kmh), 2)  AS min_speed_kmh,
  ROUND(AVG(elapsed_ms), 0) AS avg_elapsed_ms
FROM veloradar.silver.events
GROUP BY direction;

-- 3) Speed distribution in 1 km/h bands
CREATE OR REFRESH MATERIALIZED VIEW veloradar.gold.speed_distribution
COMMENT 'Number of passes per 1 km/h speed band'
AS SELECT
  CAST(FLOOR(speed_kmh) AS INT) AS speed_band_start_kmh,
  CONCAT(CAST(FLOOR(speed_kmh) AS INT), '-', CAST(FLOOR(speed_kmh) AS INT) + 1, ' km/h') AS speed_band,
  COUNT(*) AS total_passes
FROM veloradar.silver.events
GROUP BY 1, 2;