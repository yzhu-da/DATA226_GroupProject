{{ config(materialized='table') }}

with source_data as (
    select
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
    from {{ source('raw', 'BTC_REALTIME') }}
    where fetched_at is not null
),

enriched as (
    select
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
        high - low as price_spread_usd,
        round((close - open) / nullif(open, 0) * 100, 4) as snapshot_return_pct,
        round((high - low) / nullif(open, 0) * 100, 4) as volatility_pct,
        round(volume_usdt / nullif(trades, 0), 2) as avg_trade_size_usdt,
        case
            when close > open then 'up'
            when close < open then 'down'
            else 'neutral'
        end as snapshot_direction,
        case
            when taker_buy_ratio >= 0.55 then 'strong_buy_pressure'
            when taker_buy_ratio >= 0.50 then 'moderate_buy_pressure'
            when taker_buy_ratio >= 0.45 then 'neutral_or_mild_sell_pressure'
            else 'strong_sell_pressure'
        end as buy_pressure_level,
        inserted_at,
        coalesce(inserted_at, fetched_at) as record_updated_at
    from source_data
)

select *
from enriched
