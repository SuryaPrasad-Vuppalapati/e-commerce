COPY INTO RAW.ORDERS_RAW (
    order_id, customer_id, product_id, order_date, order_status,
    order_amount, currency, channel, discount_pct, shipping_country, _s3_file_path
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, METADATA$FILENAME
    FROM @RAW.S3_RAW_STAGE/orders/date={{ ds }}/   {# ← was %(execution_date)s #}
)
FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;