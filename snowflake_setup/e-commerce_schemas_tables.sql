-- Creates RAW, STAGING, MARTS schemas and the raw landing tables

USE ROLE SYSADMIN;
USE WAREHOUSE ECOMMERCE_WH;
USE DATABASE ECOMMERCE_DB;

CREATE SCHEMA IF NOT EXISTS RAW     COMMENT = 'Raw data loaded from S3';
CREATE SCHEMA IF NOT EXISTS STAGING COMMENT = 'Cleaned by dbt staging models';
CREATE SCHEMA IF NOT EXISTS MARTS   COMMENT = 'Star schema for BI consumption';

GRANT ALL ON SCHEMA ECOMMERCE_DB.RAW     TO ROLE PIPELINE_ROLE;
GRANT ALL ON SCHEMA ECOMMERCE_DB.STAGING TO ROLE PIPELINE_ROLE;
GRANT ALL ON SCHEMA ECOMMERCE_DB.MARTS   TO ROLE PIPELINE_ROLE;

GRANT ALL ON FUTURE TABLES IN SCHEMA ECOMMERCE_DB.RAW     TO ROLE PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOMMERCE_DB.STAGING TO ROLE PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOMMERCE_DB.MARTS   TO ROLE PIPELINE_ROLE;

USE SCHEMA RAW;

-- All columns are VARCHAR in the raw layer — we cast types in dbt staging
CREATE TABLE IF NOT EXISTS ORDERS_RAW (
    order_id          VARCHAR(50),
    customer_id       VARCHAR(50),
    product_id        VARCHAR(50),
    order_date        VARCHAR(30),
    order_status      VARCHAR(30),
    order_amount      VARCHAR(20),
    currency          VARCHAR(10),
    channel           VARCHAR(50),
    discount_pct      VARCHAR(10),
    shipping_country  VARCHAR(50),
    _s3_file_path     VARCHAR(500),
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS CLICKS_RAW (
    click_id          VARCHAR(50),
    session_id        VARCHAR(50),
    customer_id       VARCHAR(50),
    product_id        VARCHAR(50),
    event_type        VARCHAR(50),
    event_timestamp   VARCHAR(30),
    page_url          VARCHAR(500),
    referrer_url      VARCHAR(500),
    device_type       VARCHAR(30),
    _s3_file_path     VARCHAR(500),
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SESSIONS_RAW (
    session_id        VARCHAR(50),
    customer_id       VARCHAR(50),
    session_start     VARCHAR(30),
    session_end       VARCHAR(30),
    page_views        VARCHAR(10),
    utm_source        VARCHAR(100),
    utm_medium        VARCHAR(100),
    utm_campaign      VARCHAR(200),
    device_type       VARCHAR(30),
    country           VARCHAR(50),
    _s3_file_path     VARCHAR(500),
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Verify:
SHOW TABLES IN SCHEMA RAW;
-- You should see: ORDERS_RAW, CLICKS_RAW, SESSIONS_RAW