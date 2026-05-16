-- include/sql/load_clicks.sql
COPY INTO RAW.CLICKS_RAW (
    click_id, session_id, customer_id, product_id,
    event_type, event_timestamp, page_url, referrer_url,
    device_type, _s3_file_path
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, METADATA$FILENAME
    FROM @RAW.S3_RAW_STAGE/clicks/date={{ ds }}/
)
FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;