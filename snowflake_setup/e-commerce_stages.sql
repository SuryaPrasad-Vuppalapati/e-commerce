-- Replace <YOUR_BUCKET_NAME>, <YOUR_AWS_KEY_ID>, <YOUR_AWS_SECRET>, <YOUR_REGION>

USE ROLE SYSADMIN;
USE WAREHOUSE ECOMMERCE_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE                      = 'CSV'
    FIELD_DELIMITER           = ','
    RECORD_DELIMITER          = '\n'
    SKIP_HEADER               = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                   = ('NULL', 'null', '', 'N/A')
    EMPTY_FIELD_AS_NULL       = TRUE
    TRIM_SPACE                = TRUE;

CREATE OR REPLACE STAGE S3_RAW_STAGE
    URL         = 's3://surya-ecommerce-raw-data/raw/'
    CREDENTIALS = (
        AWS_KEY_ID     = '<replace_with_your_aws_key_id>',
        AWS_SECRET_KEY = '<replace_with_your_aws_secret_key>',
    )
    FILE_FORMAT = CSV_FORMAT;

-- Test: Snowflake should be able to list your S3 files
LIST @S3_RAW_STAGE;
-- If you see files, the stage works. If you see an error, check your credentials.

DROP SCHEMA IF EXISTS RAWECOMMERCE_DB.STAGING_staging;

desc table raw.clicks_raw;
desc table raw.sessions_raw;

-- Run in Snowflake to see the offending rows
SELECT
    metric_date,
    cvr_pct,
    total_orders,
    total_sessions
FROM STAGING.fct_daily_metricsECOMMERCE_DB
WHERE cvr_pct < 0 OR cvr_pct > 100
ORDER BY metric_date;


SHOW TABLES LIKE 'FCT_DAILY_METRICS' IN DATABASE ECOMMERCE_DB;

SELECT COUNT(*) FROM ECOMMERCE_DB.MARTS.FACT_ORDERS;
SELECT COUNT(*) FROM ECOMMERCE_DB.MARTS.FCT_DAILY_METRICS;
SELECT * FROM ECOMMERCE_DB.MARTS.FCT_DAILY_METRICS LIMIT 5;
SELECT * FROM ECOMMERCE_DB.MARTS.FACT_ORDERS LIMIT 5;

SELECT CURRENT_USER();

show warehouses;

GRANT USAGE ON WAREHOUSE ECOMMERCE_WH TO USER SURYAVUPPALAPATI;

SHOW GRANTS TO USER SURYAVUPPALAPATI;

SHOW NETWORK POLICIES;