{{ config(materialized='table') }}

WITH historical AS (
    SELECT
        btc_date,
        close_price_usd,
        day_return_pct,
        fear_greed_value,
        fear_greed_label,
        intraday_range_pct,
        volume
    FROM USER_DB_GRIZZLY.ANALYTICS.FCT_BTC_DAILY
    WHERE btc_date IS NOT NULL
      AND close_price_usd IS NOT NULL
),

base AS (
    SELECT
        btc_date AS base_date,
        close_price_usd AS base_close,
        fear_greed_value AS base_fear_greed_value,
        fear_greed_label AS base_fear_greed_label
    FROM historical
    WHERE btc_date = (
        SELECT MAX(btc_date)
        FROM historical
    )
),

recent_return AS (
    SELECT
        AVG(day_return_pct) AS avg_return_7d
    FROM historical
    WHERE btc_date >= DATEADD(day, -7, (SELECT base_date FROM base))
      AND btc_date <= (SELECT base_date FROM base)
),

forecast_dates AS (
    SELECT
        1 AS day_ahead,
        DATEADD(day, 1, base_date) AS prediction_date
    FROM base

    UNION ALL

    SELECT
        2 AS day_ahead,
        DATEADD(day, 2, base_date) AS prediction_date
    FROM base

    UNION ALL

    SELECT
        3 AS day_ahead,
        DATEADD(day, 3, base_date) AS prediction_date
    FROM base
),

forecast AS (
    SELECT
        f.prediction_date,
        f.day_ahead,
        b.base_date,
        b.base_close,
        b.base_fear_greed_value,
        b.base_fear_greed_label,
        r.avg_return_7d,

        b.base_close * POWER(1 + r.avg_return_7d, f.day_ahead) AS predicted_close,

        CASE
            WHEN r.avg_return_7d > 0 THEN 'up'
            WHEN r.avg_return_7d < 0 THEN 'down'
            ELSE 'neutral'
        END AS predicted_direction
    FROM forecast_dates f
    CROSS JOIN base b
    CROSS JOIN recent_return r
),

realtime_actual AS (
    SELECT
        snapshot_date,
        close AS actual_realtime_close,
        fetched_at,
        snapshot_hour,
        volatility_pct,
        buy_pressure_level,
        taker_buy_ratio,
        ROW_NUMBER() OVER (
            PARTITION BY snapshot_date
            ORDER BY fetched_at DESC
        ) AS rn
    FROM {{ ref('fct_btc_intraday_market') }}
),

comparison AS (
    SELECT
        f.prediction_date,
        f.day_ahead,
        f.base_date,

        ROUND(f.base_close, 2) AS base_close,
        ROUND(f.predicted_close, 2) AS predicted_close,

        ROUND(a.actual_realtime_close, 2) AS actual_realtime_close,
        a.fetched_at AS actual_fetched_at,
        a.snapshot_hour AS actual_snapshot_hour,

        ROUND(a.actual_realtime_close - f.predicted_close, 2) AS error_usd,

        ROUND(
            (a.actual_realtime_close - f.predicted_close)
            / NULLIF(f.predicted_close, 0) * 100,
            4
        ) AS error_pct,

        f.predicted_direction,

        CASE
            WHEN a.actual_realtime_close > f.base_close THEN 'up'
            WHEN a.actual_realtime_close < f.base_close THEN 'down'
            ELSE 'neutral'
        END AS actual_direction,

        CASE
            WHEN f.predicted_direction =
                CASE
                    WHEN a.actual_realtime_close > f.base_close THEN 'up'
                    WHEN a.actual_realtime_close < f.base_close THEN 'down'
                    ELSE 'neutral'
                END
            THEN TRUE
            ELSE FALSE
        END AS direction_match,

        ROUND(f.avg_return_7d * 100, 4) AS avg_return_7d_pct,

        f.base_fear_greed_value,
        f.base_fear_greed_label,

        a.volatility_pct AS realtime_volatility_pct,
        a.buy_pressure_level,
        a.taker_buy_ratio,

        '7_day_avg_return_forecast' AS model_name,
        CURRENT_TIMESTAMP() AS model_created_at

    FROM forecast f
    LEFT JOIN realtime_actual a
        ON f.prediction_date = a.snapshot_date
       AND a.rn = 1
)

SELECT *
FROM comparison
ORDER BY prediction_date