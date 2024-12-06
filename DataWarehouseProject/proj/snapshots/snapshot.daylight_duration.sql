{% snapshot snapshot_daylight_duration %}
{{
    config(
        target_schema='snapshot',
        unique_key='datetime',
        strategy='timestamp',
        updated_at='datetime',
        invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ ref('daylight_duration') }}
{% endsnapshot %}