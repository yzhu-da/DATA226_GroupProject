{% snapshot snapshot_fct_btc_intraday_market %}

{{
    config(
        target_schema='snapshot',
        unique_key='fetched_at',
        strategy='timestamp',
        updated_at='record_updated_at',
        invalidate_hard_deletes=true
    )
}}

select *
from {{ ref('fct_btc_intraday_market') }}

{% endsnapshot %}
