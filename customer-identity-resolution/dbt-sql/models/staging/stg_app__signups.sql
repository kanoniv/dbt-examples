-- Staging: App Signups
-- Clean and type-cast raw product app signup records

with source as (
    select * from {{ source('raw', 'app_signups') }}
),

staged as (
    select
        app_user_id,
        lower(trim(email))                          as email,
        case when trim(first_name) = '' then null else trim(first_name) end as first_name,
        case when trim(last_name) = '' then null else trim(last_name) end   as last_name,
        trim(signup_source)                         as signup_source,
        trim(device_type)                           as device_type,
        trim(os)                                    as os,
        cast(created_at as timestamp)               as created_at,
        cast(last_login_at as timestamp)            as last_login_at,
        case when is_verified = 'true' then true else false end as is_verified,
        'app' as source_system
    from source
)

select * from staged
where app_user_id is not null
