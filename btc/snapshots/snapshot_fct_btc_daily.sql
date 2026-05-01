{% snapshot snapshot_fct_btc_daily %}

{{
    config(
        target_schema='snapshot',
        unique_key='btc_date',
        strategy='timestamp',
        updated_at='record_updated_at',
        invalidate_hard_deletes=true
    )
}}

select *
from {{ ref('fct_btc_daily') }}

{% endsnapshot %}
