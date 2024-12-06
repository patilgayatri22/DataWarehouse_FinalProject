{% snapshot snapshot_solar_efficiency %}
{{
    config(
        target_schema='snapshot',
        unique_key='datetime',
        strategy='timestamp',
        updated_at='datetime',
        invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ ref('solar_efficiency') }}
{% endsnapshot %}