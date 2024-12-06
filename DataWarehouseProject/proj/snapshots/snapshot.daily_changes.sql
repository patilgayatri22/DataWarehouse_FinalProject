{% snapshot snapshot_daily_changes %}
{{
    config(
        target_schema='snapshot',
        unique_key='datetime',
        strategy='timestamp',
        updated_at='datetime',
        invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ ref('daily_changes') }}
{% endsnapshot %}