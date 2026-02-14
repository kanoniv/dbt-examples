-- Staging: Support Tickets
-- Clean and type-cast raw support ticket records

with source as (
    select * from {{ source('raw', 'support_tickets') }}
),

staged as (
    select
        ticket_id,
        support_user_id,
        trim(subject)                               as subject,
        trim(priority)                              as priority,
        trim(status)                                as ticket_status,
        cast(created_at as timestamp)               as created_at,
        case
            when trim(resolved_at) = '' then null
            else cast(resolved_at as timestamp)
        end                                         as resolved_at,
        case
            when resolved_at is not null and trim(resolved_at) != ''
            then datediff('hour', cast(created_at as timestamp), cast(resolved_at as timestamp))
            else null
        end                                         as resolution_hours,
        'support' as source_system
    from source
)

select * from staged
where ticket_id is not null
