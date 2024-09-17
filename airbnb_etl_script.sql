-- Create database and warehouse
CREATE DATABASE IF NOT EXISTS nyc_airbnb;
CREATE WAREHOUSE IF NOT EXISTS compute_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

USE DATABASE nyc_airbnb;
USE WAREHOUSE compute_wh;

-- Create external stage
CREATE STAGE IF NOT EXISTS nyc_airbnb_stage
  URL = 's3://your-bucket-name/path/to/data/'
  CREDENTIALS = (AWS_KEY_ID = 'your_access_key' AWS_SECRET_KEY = 'your_secret_key');

-- Create file format
CREATE FILE FORMAT IF NOT EXISTS csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

-- Create table for raw data
CREATE TABLE IF NOT EXISTS raw_airbnb_data (
  id NUMBER,
  name STRING,
  host_id NUMBER,
  host_name STRING,
  neighbourhood_group STRING,
  neighbourhood STRING,
  latitude FLOAT,
  longitude FLOAT,
  room_type STRING,
  price NUMBER,
  minimum_nights NUMBER,
  number_of_reviews NUMBER,
  last_review STRING,
  reviews_per_month FLOAT,
  calculated_host_listings_count NUMBER,
  availability_365 NUMBER
);

-- Create table for transformed data
CREATE TABLE IF NOT EXISTS transformed_airbnb_data (
  id NUMBER,
  name STRING,
  host_id NUMBER,
  host_name STRING,
  neighbourhood_group STRING,
  neighbourhood STRING,
  latitude FLOAT,
  longitude FLOAT,
  room_type STRING,
  price NUMBER,
  minimum_nights NUMBER,
  number_of_reviews NUMBER,
  last_review DATE,
  reviews_per_month FLOAT,
  calculated_host_listings_count NUMBER,
  availability_365 NUMBER
);

-- Create stream to track changes in raw data
CREATE STREAM IF NOT EXISTS raw_airbnb_stream ON TABLE raw_airbnb_data;

-- Create stored procedure for data transformation
CREATE OR REPLACE PROCEDURE transform_airbnb_data()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT INTO transformed_airbnb_data
  SELECT
    id,
    name,
    host_id,
    host_name,
    neighbourhood_group,
    neighbourhood,
    latitude,
    longitude,
    room_type,
    IFF(price > 0, price, NULL) AS price,
    minimum_nights,
    number_of_reviews,
    IFF(last_review IS NULL OR last_review = '',
        (SELECT MIN(last_review) FROM raw_airbnb_data WHERE last_review IS NOT NULL AND last_review != ''),
        TO_DATE(last_review)) AS last_review,
    COALESCE(reviews_per_month, 0) AS reviews_per_month,
    calculated_host_listings_count,
    availability_365
  FROM raw_airbnb_stream
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

  RETURN 'Data transformation completed successfully';
END;
$$;

-- Create stream to track changes in transformed data
CREATE STREAM IF NOT EXISTS transformed_airbnb_stream ON TABLE transformed_airbnb_data;

-- Create stored procedure for data quality check
CREATE OR REPLACE PROCEDURE check_data_quality()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  invalid_records NUMBER;
BEGIN
  LET invalid_records := (SELECT COUNT(*)
                          FROM transformed_airbnb_stream
                          WHERE price IS NULL OR minimum_nights IS NULL OR availability_365 IS NULL);

  IF (invalid_records > 0) THEN
    RETURN 'Data quality check failed. ' || TO_VARCHAR(invalid_records) || ' invalid records found.';
  ELSE
    RETURN 'Data quality check passed successfully.';
  END IF;
END;
$$;

-- Create task for daily transformation and quality check
CREATE OR REPLACE TASK daily_airbnb_etl
  WAREHOUSE = compute_wh
  SCHEDULE = 'USING CRON 0 0 * * * America/New_York'
AS
BEGIN
  CALL transform_airbnb_data();
  CALL check_data_quality();
END;

-- Activate the task
ALTER TASK daily_airbnb_etl RESUME;

-- Procedure for data recovery
CREATE OR REPLACE PROCEDURE recover_data(error_time TIMESTAMP)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT INTO transformed_airbnb_data
  SELECT * FROM transformed_airbnb_data AT(TIMESTAMP => :error_time);

  RETURN 'Data recovered successfully';
END;
$$;

-- Procedure for monitoring changes
CREATE OR REPLACE PROCEDURE monitor_changes()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  raw_changes NUMBER;
  transformed_changes NUMBER;
BEGIN
  LET raw_changes := (SELECT COUNT(*) FROM raw_airbnb_stream);
  LET transformed_changes := (SELECT COUNT(*) FROM transformed_airbnb_stream);

  RETURN 'Changes detected: ' || TO_VARCHAR(raw_changes) || ' in raw data, ' || TO_VARCHAR(transformed_changes) || ' in transformed data.';
END;
$$;

-- Optimization: cluster the table
ALTER TABLE transformed_airbnb_data CLUSTER BY (neighbourhood_group, price);

-- Create materialized view for frequently used aggregations
CREATE OR REPLACE MATERIALIZED VIEW avg_price_by_neighbourhood AS
SELECT neighbourhood_group, AVG(price) as avg_price
FROM transformed_airbnb_data
GROUP BY neighbourhood_group;

-- Create Snowpipe for automatic data loading
CREATE PIPE IF NOT EXISTS nyc_airbnb_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO raw_airbnb_data
  FROM @nyc_airbnb_stage
  FILE_FORMAT = csv_format;
