# NYC Airbnb ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for New York City Airbnb data using Snowflake.

## Prerequisites

1. Snowflake account
2. SnowSQL CLI installed on your local machine
3. Access to cloud storage (AWS S3, Azure Blob Storage, or Google Cloud Storage)
4. NYC Airbnb dataset uploaded to your cloud storage

## Setup

1. Clone this repository to your local machine.
2. Open the `airbnb_etl_script.sql` file and update the following parameters:
   - Cloud storage URL
   - Credentials for accessing the cloud storage

## Running the Pipeline

1. Connect to your Snowflake account via SnowSQL:
   ```
   snowsql -a your_account -u your_username
   ```

2. Execute the SQL script:
   ```
   snowsql -f airbnb_etl_script.sql
   ```

## Running Snowpipe

Snowpipe is configured for automatic loading. However, if you need to run it manually:

```sql
ALTER PIPE nyc_airbnb_pipe REFRESH;
```

## Running Transformations

Transformations are scheduled to run automatically daily. For manual execution:

```sql
CALL transform_airbnb_data();
```

## Running Snowflake Tasks

The `daily_airbnb_etl` task is set to run automatically daily. For manual execution:

```sql
EXECUTE TASK daily_airbnb_etl;
```

## Monitoring the Pipeline

1. Check Snowpipe status:
   ```sql
   SELECT SYSTEM$PIPE_STATUS('nyc_airbnb_pipe');
   ```

2. Monitor data changes:
   ```sql
   CALL monitor_changes();
   ```

3. Check data quality:
   ```sql
   CALL check_data_quality();
   ```

4. View loading history:
   ```sql
   SELECT * 
   FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
      TABLE_NAME=>'raw_airbnb_data',
      START_TIME=> DATEADD(hours, -24, CURRENT_TIMESTAMP())));
   ```

## Data Recovery

In case of errors, you can recover data using Time Travel:

```sql
CALL recover_data('2023-09-17 10:00:00'::timestamp);
```

Replace the timestamp with the time you want to revert to.
