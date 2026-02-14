-- Staging: App Events
-- Clean and type-cast raw product usage events

with source as (
    select * from {{ source('raw', 'app_events') }}
),

staged as (
    select
        event_id,
        app_user_id,
        trim(event_type)                            as event_type,
        cast(timestamp as timestamp)                as event_at,
        try_parse_json(properties)                  as event_properties,
        'app' as source_system
    from source
)

select * from staged
where event_id is not null
