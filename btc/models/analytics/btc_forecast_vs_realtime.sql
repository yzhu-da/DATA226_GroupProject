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
      and day_return_pct is not null
),

historical_with_row_num as (
    select
        *,
        row_number() over (order by btc_date) as rn
    from historical
),

rolling_base as (
    select
        h.btc_date as base_date,
        h.close_price_usd as base_close,
        h.fear_greed_value as base_fear_greed_value,
        h.fear_greed_label as base_fear_greed_label,
        h.intraday_range_pct as base_intraday_range_pct,
        h.volume as base_volume,
        h.rn as base_rn
    from historical_with_row_num h
    where h.rn > 7
),

rolling_return as (
    select
        b.base_date,
        avg(h.day_return_pct) as avg_return_7d
    from rolling_base b
    join historical_with_row_num h
        on h.rn between b.base_rn - 7 and b.base_rn - 1
    group by b.base_date
),

forecast_horizon as (
    select 1 as day_ahead
    union all select 2
    union all select 3
    union all select 4
    union all select 5
),

forecast as (
    select
        dateadd(day, fh.day_ahead, b.base_date) as prediction_date,
        fh.day_ahead,
        b.base_date,
        b.base_close,
        b.base_fear_greed_value,
        b.base_fear_greed_label,
        b.base_intraday_range_pct,
        b.base_volume,
        r.avg_return_7d,

        b.base_close * power(1 + r.avg_return_7d, fh.day_ahead) as predicted_close,

        case
            when r.avg_return_7d > 0 then 'up'
            when r.avg_return_7d < 0 then 'down'
            else 'neutral'
        end as predicted_direction
    from rolling_base b
    join rolling_return r
        on b.base_date = r.base_date
    cross join forecast_horizon fh
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
            when a.actual_realtime_close is null then null
            when a.actual_realtime_close > f.base_close then 'up'
            when a.actual_realtime_close < f.base_close then 'down'
            else 'neutral'
        end as actual_direction,

        case
            when a.actual_realtime_close is null then null
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
        f.base_intraday_range_pct,
        f.base_volume,

        a.volatility_pct as realtime_volatility_pct,
        a.buy_pressure_level,
        a.taker_buy_ratio,

        case
            when a.actual_realtime_close is null then 'forecast_only'
            else 'validated_with_realtime'
        end as validation_status,

        'rolling_7_record_avg_return_multi_day_forecast' as model_name,
        current_timestamp() as model_created_at,
        coalesce(a.fetched_at, current_timestamp()) as record_updated_at

    from forecast f
    left join realtime_actual a
        on f.prediction_date = a.snapshot_date
       and a.rn = 1

    qualify row_number() over (
        partition by f.prediction_date
        order by f.base_date desc
    ) = 1
)

select *
from comparison
order by prediction_date