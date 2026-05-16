-- include/sql/load_sessions.sql
COPY INTO RAW.SESSIONS_RAW (
    session_id, customer_id, session_start, session_end,
    page_views, utm_source, utm_medium, utm_campaign,
    device_type, country, _s3_file_path
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, METADATA$FILENAME
    FROM @RAW.S3_RAW_STAGE/sessions/date={{ ds }}/
)
FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;