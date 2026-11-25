CREATE OR REFRESH STREAMING TABLE veloradar.bronze.events
COMMENT 'Raw VeloRadar pass events ingested from the landing volume'
AS SELECT
  *,
  _metadata.file_path              AS source_file,
  _metadata.file_modification_time AS source_file_modified_at,
  current_timestamp()              AS ingested_at
FROM STREAM read_files(
  '/Volumes/veloradar/landing/events/',
  format         => 'json',
  pathGlobFilter => 'events_*.jsonl'
);