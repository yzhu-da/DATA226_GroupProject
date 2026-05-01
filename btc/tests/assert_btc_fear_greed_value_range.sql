select *
from {{ ref('btc_fear_greed_clean') }}
where fear_greed_value < 0
   or fear_greed_value > 100
