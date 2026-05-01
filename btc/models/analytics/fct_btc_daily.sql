with daily_prices as (
    select *
    from {{ ref('btc_daily_clean') }}
),

fear_greed as (
    select *
    from {{ ref('btc_fear_greed_clean') }}
)

select
    d.btc_date,
    d.open_price_usd,
    d.high_price_usd,
    d.low_price_usd,
    d.close_price_usd,
    d.volume,
    fg.fear_greed_value,
    fg.fear_greed_label,
    (d.close_price_usd - d.open_price_usd) as day_price_change_usd,
    (d.close_price_usd - d.open_price_usd) / nullif(d.open_price_usd, 0) as day_return_pct,
    (d.high_price_usd - d.low_price_usd) / nullif(d.low_price_usd, 0) as intraday_range_pct,
    d.source_fetched_at_utc as daily_source_fetched_at_utc,
    fg.source_fetched_at_utc as fear_greed_source_fetched_at_utc,
    d.loaded_to_snowflake_at as daily_loaded_to_snowflake_at,
    fg.loaded_to_snowflake_at as fear_greed_loaded_to_snowflake_at,
    greatest(
        coalesce(d.loaded_to_snowflake_at, to_timestamp_ntz('1900-01-01')),
        coalesce(fg.loaded_to_snowflake_at, to_timestamp_ntz('1900-01-01'))
    ) as record_updated_at
from daily_prices as d
left join fear_greed as fg
    on d.btc_date = fg.btc_date
