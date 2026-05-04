{% snapshot snapshot_btc_forecast_vs_realtime %}

{{
    config(
        target_schema='snapshot',
        unique_key='prediction_date',
        strategy='timestamp',
        updated_at='record_updated_at',
        invalidate_hard_deletes=true
    )
}}

select *
from {{ ref('btc_forecast_vs_realtime') }}

{% endsnapshot %}
