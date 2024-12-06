{% snapshot snapshot_pressure_drops %}
{{
    config(
        target_schema='snapshot',
        unique_key='datetime',
        strategy='timestamp',
        updated_at='datetime',
        invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ ref('pressure_drops') }}
{% endsnapshot %}