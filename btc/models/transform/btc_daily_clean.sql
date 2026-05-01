select
    date as btc_date,
    cast(open as float) as open_price_usd,
    cast(high as float) as high_price_usd,
    cast(low as float) as low_price_usd,
    cast(close as float) as close_price_usd,
    cast(volume as number(38, 0)) as volume,
    yfinance_fetched_at as source_fetched_at_utc,
    inserted_at as loaded_to_snowflake_at
from {{ source('raw', 'BTC_DAILY') }}
