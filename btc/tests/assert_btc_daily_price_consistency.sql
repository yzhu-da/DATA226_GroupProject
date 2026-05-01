select *
from {{ ref('btc_daily_clean') }}
where open_price_usd <= 0
   or high_price_usd <= 0
   or low_price_usd <= 0
   or close_price_usd <= 0
   or high_price_usd < open_price_usd
   or high_price_usd < close_price_usd
   or high_price_usd < low_price_usd
   or low_price_usd > open_price_usd
   or low_price_usd > close_price_usd
   or low_price_usd > high_price_usd
