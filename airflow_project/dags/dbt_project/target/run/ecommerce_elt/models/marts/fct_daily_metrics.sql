
  
    

create or replace transient table ECOMMERCE_DB.marts.fct_daily_metrics
    
    
    
    as (select * from (
            

WITH

sessions_daily AS (
    SELECT
        session_date          AS metric_date,
        channel_group,
        COUNT(DISTINCT session_id)          AS total_sessions,
        COUNT(DISTINCT customer_id)         AS unique_visitors,
        SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) AS bounced_sessions,
        AVG(session_duration_secs)          AS avg_session_duration_secs
    FROM ECOMMERCE_DB.staging.stg_sessions
    GROUP BY 1, 2
),

clicks_daily AS (
    SELECT
        event_date            AS metric_date,
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) AS add_to_cart_sessions,
        COUNT(DISTINCT CASE WHEN event_type = 'checkout'    THEN session_id END) AS checkout_sessions,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase'    THEN session_id END) AS purchase_sessions
    FROM ECOMMERCE_DB.staging.stg_clicks
    GROUP BY 1
),

orders_daily AS (
    SELECT
        order_date            AS metric_date,
        acquisition_channel   AS channel_group,
        COUNT(DISTINCT order_id)            AS total_orders,
        COUNT(DISTINCT customer_id)         AS purchasing_customers,
        COUNT(DISTINCT CASE WHEN is_first_order THEN customer_id END) AS new_customers,
        SUM(net_amount_usd)                 AS total_revenue_usd,
        AVG(net_amount_usd)                 AS avg_order_value_usd,
        SUM(CASE WHEN order_status = 'refunded' THEN net_amount_usd ELSE 0 END) AS refund_amount_usd
    FROM ECOMMERCE_DB.marts.fact_orders
    GROUP BY 1, 2
),

combined AS (
    SELECT
        s.metric_date,
        s.channel_group,
        s.total_sessions,
        s.unique_visitors,
        s.bounced_sessions,
        ROUND(s.avg_session_duration_secs, 0)   AS avg_session_duration_secs,
        c.add_to_cart_sessions,
        c.checkout_sessions,
        c.purchase_sessions,
        COALESCE(o.total_orders, 0)             AS total_orders,
        COALESCE(o.new_customers, 0)            AS new_customers,
        COALESCE(o.total_revenue_usd, 0)        AS total_revenue_usd,
        COALESCE(o.avg_order_value_usd, 0)      AS avg_order_value_usd,
        COALESCE(o.refund_amount_usd, 0)        AS refund_amount_usd
    FROM sessions_daily s
    LEFT JOIN clicks_daily c ON s.metric_date   = c.metric_date
    LEFT JOIN orders_daily o ON s.metric_date   = o.metric_date
                             AND s.channel_group = o.channel_group
)

SELECT
    metric_date,
    channel_group,
    total_sessions,
    unique_visitors,
    bounced_sessions,
    avg_session_duration_secs,
    add_to_cart_sessions,
    checkout_sessions,
    purchase_sessions,
    total_orders,
    new_customers,
    total_revenue_usd,
    avg_order_value_usd,
    refund_amount_usd,

    -- CVR: sessions that ended in a purchase
    ROUND(
        CASE WHEN total_sessions > 0
             THEN (purchase_sessions::FLOAT / total_sessions) * 100
             ELSE 0 END, 2
    ) AS cvr_pct,

    -- Add-to-cart rate
    ROUND(
        CASE WHEN total_sessions > 0
             THEN (add_to_cart_sessions::FLOAT / total_sessions) * 100
             ELSE 0 END, 2
    ) AS add_to_cart_rate_pct,

    -- Bounce rate
    ROUND(
        CASE WHEN total_sessions > 0
             THEN (bounced_sessions::FLOAT / total_sessions) * 100
             ELSE 0 END, 2
    ) AS bounce_rate_pct,

    -- Revenue per session
    ROUND(
        CASE WHEN total_sessions > 0
             THEN total_revenue_usd / total_sessions
             ELSE 0 END, 2
    ) AS revenue_per_session_usd,

    -- CAC placeholder — connect ad spend data to fill this in
    0.00 AS ad_spend_usd,
    0.00 AS cac_usd

FROM combined
ORDER BY metric_date DESC, total_revenue_usd DESC
        )
        order by (
            metric_date
        )
    )
;

alter table ECOMMERCE_DB.marts.fct_daily_metrics cluster by (metric_date);
  