WITH source AS (
    SELECT * FROM ECOMMERCE_DB.raw.clicks_raw
),

typed AS (
    SELECT
        click_id,
        session_id,
        customer_id,
        NULLIF(TRIM(product_id), '')                          AS product_id,
        LOWER(TRIM(event_type))                               AS event_type,
        TRY_TO_TIMESTAMP(event_timestamp)                     AS event_timestamp,
        DATE(TRY_TO_TIMESTAMP(event_timestamp))               AS event_date,
        NULLIF(TRIM(page_url), '')                            AS page_url,
        NULLIF(TRIM(referrer_url), '')                        AS referrer_url,
        LOWER(TRIM(device_type))                              AS device_type,
        _s3_file_path,
        _loaded_at
    FROM source
    WHERE click_id IS NOT NULL
),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY click_id
                ORDER BY _loaded_at DESC
            ) AS _row_num
        FROM typed
    )
    WHERE _row_num = 1
),

final AS (
    SELECT
        click_id,
        session_id,
        customer_id,
        product_id,
        event_type,
        event_timestamp,
        event_date,
        page_url,
        referrer_url,
        device_type,
        _s3_file_path,
        _loaded_at,
        CASE event_type
            WHEN 'page_view'    THEN 1
            WHEN 'product_view' THEN 2
            WHEN 'add_to_cart'  THEN 3
            WHEN 'checkout'     THEN 4
            WHEN 'purchase'     THEN 5
            ELSE 0
        END AS funnel_stage
    FROM deduped
)

SELECT * FROM final