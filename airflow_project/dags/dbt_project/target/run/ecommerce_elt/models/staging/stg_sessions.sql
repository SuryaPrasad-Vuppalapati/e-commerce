
  create or replace   view ECOMMERCE_DB.staging.stg_sessions
  
  
  
  
  as (
    WITH source AS (
    SELECT * FROM ECOMMERCE_DB.raw.sessions_raw
),

typed AS (
    SELECT
        session_id,
        customer_id,
        TRY_TO_TIMESTAMP(session_start)           AS session_start,
        TRY_TO_TIMESTAMP(session_end)             AS session_end,
        DATE(TRY_TO_TIMESTAMP(session_start))     AS session_date,
        COALESCE(TRY_TO_NUMBER(page_views), 0)   AS page_views,
        NULLIF(LOWER(TRIM(utm_source)), '')       AS utm_source,
        NULLIF(LOWER(TRIM(utm_medium)), '')       AS utm_medium,
        NULLIF(LOWER(TRIM(utm_campaign)), '')     AS utm_campaign,
        LOWER(TRIM(device_type))                  AS device_type,
        UPPER(TRIM(country))                      AS country,
        _s3_file_path,
        _loaded_at
    FROM source
    WHERE session_id IS NOT NULL
),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY session_id
                ORDER BY _loaded_at DESC
            ) AS _row_num
        FROM typed
    )
    WHERE _row_num = 1
),

final AS (
    SELECT
        session_id,
        customer_id,
        session_start,
        session_end,
        session_date,
        page_views,
        utm_source,
        utm_medium,
        utm_campaign,
        device_type,
        country,
        _s3_file_path,
        _loaded_at,
        DATEDIFF('second', session_start, session_end)  AS session_duration_secs,
        CASE
            WHEN page_views <= 1
             AND DATEDIFF('second', session_start, session_end) < 10
            THEN TRUE ELSE FALSE
        END AS is_bounce,
        CASE
            WHEN utm_medium = 'cpc'     THEN 'paid_search'
            WHEN utm_medium = 'social'  THEN 'paid_social'
            WHEN utm_medium = 'email'   THEN 'email'
            WHEN utm_medium = 'organic' THEN 'organic_search'
            WHEN utm_source = 'direct'  THEN 'direct'
            WHEN utm_medium = 'referral' THEN 'referral'
            ELSE 'other'
        END AS channel_group
    FROM deduped
    WHERE session_start IS NOT NULL
)

SELECT * FROM final
  );

