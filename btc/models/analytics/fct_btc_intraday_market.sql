{{ config(materialized='table') }}

WITH source AS (
    SELECT
        fetched_at,
        snapshot_date,
        snapshot_hour,
        open,
        high,
        low,
        close,
        volume_btc,
        volume_usdt,
        trades,
        taker_buy_volume,
        taker_buy_quote,
        taker_buy_ratio,
        market_cap_usd,
        inserted_at
    FROM USER_DB_GOPHER.RAW.BTC_REALTIME
    WHERE fetched_at IS NOT NULL
),

enriched AS (
    SELECT
        fetched_at,
        snapshot_date,
        snapshot_hour,
        open,
        high,
        low,
        close,
        volume_btc,
        volume_usdt,
        trades,
        taker_buy_volume,
        taker_buy_quote,
        taker_buy_ratio,
        market_cap_usd,

        high - low AS price_spread_usd,

        ROUND((close - open) / NULLIF(open, 0) * 100, 4) AS snapshot_return_pct,

        ROUND((high - low) / NULLIF(open, 0) * 100, 4) AS volatility_pct,
        
        ROUND(volume_usdt / NULLIF(trades, 0), 2) AS avg_trade_size_usdt,

        CASE
            WHEN close > open THEN 'up'
            WHEN close < open THEN 'down'
            ELSE 'neutral'
        END AS snapshot_direction,

        CASE
            WHEN taker_buy_ratio >= 0.55 THEN 'strong_buy_pressure'
            WHEN taker_buy_ratio >= 0.50 THEN 'moderate_buy_pressure'
            WHEN taker_buy_ratio >= 0.45 THEN 'neutral_or_mild_sell_pressure'
            ELSE 'strong_sell_pressure'
        END AS buy_pressure_level,

        inserted_at
    FROM source
)

SELECT *
FROM enriched