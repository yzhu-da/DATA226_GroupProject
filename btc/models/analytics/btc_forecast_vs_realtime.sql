{{ config(materialized='table') }}

with historical as (
    select
        btc_date,
        close_price_usd,
        day_return_pct,
        fear_greed_value,
        fear_greed_label,
        intraday_range_pct,
        volume
    from {{ ref('fct_btc_daily') }}
    where btc_date is not null
      and close_price_usd is not null
),

base as (
    select
        btc_date as base_date,
        close_price_usd as base_close,
        fear_greed_value as base_fear_greed_value,
        fear_greed_label as base_fear_greed_label
    from historical
    where btc_date = (
        select max(btc_date)
        from historical
    )
),

recent_return as (
    select
        avg(day_return_pct) as avg_return_7d
    from historical
    where btc_date >= dateadd(day, -6, (select base_date from base))
      and btc_date <= (select base_date from base)
),

forecast_dates as (
    select
        1 as day_ahead,
        dateadd(day, 1, base_date) as prediction_date
    from base

    union all

    select
        2 as day_ahead,
        dateadd(day, 2, base_date) as prediction_date
    from base

    union all

    select
        3 as day_ahead,
        dateadd(day, 3, base_date) as prediction_date
    from base
),

forecast as (
    select
        f.prediction_date,
        f.day_ahead,
        b.base_date,
        b.base_close,
        b.base_fear_greed_value,
        b.base_fear_greed_label,
        r.avg_return_7d,
        b.base_close * power(1 + r.avg_return_7d, f.day_ahead) as predicted_close,
        case
            when r.avg_return_7d > 0 then 'up'
            when r.avg_return_7d < 0 then 'down'
            else 'neutral'
        end as predicted_direction
    from forecast_dates f
    cross join base b
    cross join recent_return r
),

realtime_actual as (
    select
        snapshot_date,
        close as actual_realtime_close,
        fetched_at,
        snapshot_hour,
        volatility_pct,
        buy_pressure_level,
        taker_buy_ratio,
        row_number() over (
            partition by snapshot_date
            order by fetched_at desc
        ) as rn
    from {{ ref('fct_btc_intraday_market') }}
),

comparison as (
    select
        f.prediction_date,
        f.day_ahead,
        f.base_date,
        round(f.base_close, 2) as base_close,
        round(f.predicted_close, 2) as predicted_close,
        round(a.actual_realtime_close, 2) as actual_realtime_close,
        a.fetched_at as actual_fetched_at,
        a.snapshot_hour as actual_snapshot_hour,
        round(a.actual_realtime_close - f.predicted_close, 2) as error_usd,
        round(
            (a.actual_realtime_close - f.predicted_close)
            / nullif(f.predicted_close, 0) * 100,
            4
        ) as error_pct,
        f.predicted_direction,
        case
            when a.actual_realtime_close > f.base_close then 'up'
            when a.actual_realtime_close < f.base_close then 'down'
            else 'neutral'
        end as actual_direction,
        case
            when f.predicted_direction =
                case
                    when a.actual_realtime_close > f.base_close then 'up'
                    when a.actual_realtime_close < f.base_close then 'down'
                    else 'neutral'
                end
            then true
            else false
        end as direction_match,
        round(f.avg_return_7d * 100, 4) as avg_return_7d_pct,
        f.base_fear_greed_value,
        f.base_fear_greed_label,
        a.volatility_pct as realtime_volatility_pct,
        a.buy_pressure_level,
        a.taker_buy_ratio,
        '7_day_avg_return_forecast' as model_name,
        current_timestamp() as model_created_at,
        greatest(
            to_timestamp_ntz(f.base_date),
            coalesce(a.fetched_at, to_timestamp_ntz(f.prediction_date))
        ) as record_updated_at
    from forecast f
    left join realtime_actual a
        on f.prediction_date = a.snapshot_date
       and a.rn = 1
)

select *
from comparison
order by prediction_date
