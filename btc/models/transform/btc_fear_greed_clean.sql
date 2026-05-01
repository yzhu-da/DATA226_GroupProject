select
    date as btc_date,
    cast(fng_value as integer) as fear_greed_value,
    fng_label as fear_greed_label,
    fetched_at as source_fetched_at_utc,
    inserted_at as loaded_to_snowflake_at
from {{ source('raw', 'BTC_FEAR_GREED') }}
