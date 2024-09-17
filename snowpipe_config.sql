-- Create Snowpipe for automatic data loading
CREATE PIPE IF NOT EXISTS nyc_airbnb_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO raw_airbnb_data
  FROM @nyc_airbnb_stage
  FILE_FORMAT = csv_format;

-- View Snowpipe status
SHOW PIPES;

-- Manually refresh Snowpipe (if needed)
ALTER PIPE nyc_airbnb_pipe REFRESH;

-- View loading history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
   TABLE_NAME=>'raw_airbnb_data',
   START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));

-- Monitoring Snowpipe
SELECT SYSTEM$PIPE_STATUS('nyc_airbnb_pipe');