{% snapshot snapshot_temperature_range %}
{{
    config(
        target_schema='snapshot',
        unique_key='datetime',
        strategy='timestamp',
        updated_at='datetime',
        invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ ref('temperature_range') }}
{% endsnapshot %}