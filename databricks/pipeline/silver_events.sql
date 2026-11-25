-- 1) Typed and validated events (temporary view, not published to the catalog)
CREATE TEMPORARY VIEW events_validated (
  CONSTRAINT valid_speed      EXPECT (speed_kmh > 0)   ON VIOLATION DROP ROW,
  CONSTRAINT valid_elapsed    EXPECT (elapsed_ms > 0)  ON VIOLATION DROP ROW,
  CONSTRAINT valid_direction  EXPECT (direction IN ('right_to_left', 'left_to_right')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_event_time EXPECT (event_time IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  CAST(kafka_partition AS INT)   AS kafka_partition,
  CAST(kafka_offset AS BIGINT)   AS kafka_offset,
  kafka_key                      AS device_id,
  CAST(id AS INT)                AS device_pass_seq,
  event                          AS event_type,
  CAST(start_sensor AS INT)      AS start_sensor,
  direction,
  CAST(elapsed_ms AS INT)        AS elapsed_ms,
  CAST(distance_cm AS DOUBLE)    AS distance_cm,
  CAST(speed_kmh AS DOUBLE)      AS speed_kmh,
  CAST(received_at AS TIMESTAMP) AS event_time,
  source_file,
  ingested_at
FROM STREAM(veloradar.bronze.events);

-- 2) Deduplicated silver table (one row per Kafka message)
CREATE OR REFRESH STREAMING TABLE veloradar.silver.events
COMMENT 'Validated, typed and deduplicated VeloRadar pass events';

CREATE FLOW silver_events_dedup AS AUTO CDC INTO veloradar.silver.events
FROM STREAM(events_validated)
KEYS (kafka_partition, kafka_offset)
SEQUENCE BY ingested_at
STORED AS SCD TYPE 1;