-- Cleans, types, and deduplicates raw orders.
-- Materialised as a VIEW.

WITH

source AS (
    SELECT * FROM {{ source('raw', 'orders_raw') }}
),

typed AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        TRY_TO_TIMESTAMP(order_date)                          AS order_timestamp,
        DATE(TRY_TO_TIMESTAMP(order_date))                    AS order_date,
        UPPER(TRIM(order_status))                             AS order_status,
        COALESCE(TRY_TO_DECIMAL(order_amount, 10, 2), 0.00)  AS order_amount_usd,
        UPPER(TRIM(currency))                                 AS currency,
        LOWER(TRIM(channel))                                  AS acquisition_channel,
        COALESCE(TRY_TO_NUMBER(discount_pct), 0)              AS discount_pct,
        UPPER(TRIM(shipping_country))                         AS shipping_country,
        _s3_file_path,
        _loaded_at
    FROM source
    WHERE order_id IS NOT NULL
),

-- Keep only the latest loaded version of each order_id (deduplication)
deduped AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY _loaded_at DESC
            ) AS _row_num
        FROM typed
    )
    WHERE _row_num = 1
),

final AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        order_timestamp,
        order_date,
        order_status,
        order_amount_usd,
        currency,
        acquisition_channel,
        discount_pct,
        shipping_country,
        _s3_file_path,
        _loaded_at,
        ROUND(order_amount_usd * (1 - discount_pct / 100.0), 2) AS net_amount_usd,
        CASE
            WHEN order_amount_usd <= 0               THEN 'zero_or_negative_amount'
            WHEN order_timestamp  >  CURRENT_TIMESTAMP() THEN 'future_order'
            WHEN customer_id      IS NULL             THEN 'missing_customer'
            ELSE 'valid'
        END                                                   AS data_quality_flag
    FROM deduped
)

SELECT * FROM final