-- Staging: Support Users
-- Clean and type-cast raw support user records
-- Support data is the messiest: many missing emails/phones, free-text display names

with source as (
    select * from {{ source('raw', 'support_users') }}
),

staged as (
    select
        support_user_id,
        trim(display_name)                          as display_name,
        -- Parse display_name into first/last
        trim(split_part(display_name, ' ', 1))      as first_name,
        trim(
            case
                when display_name like '% % %'
                then substring(display_name from position(' ' in display_name) + 1)
                else split_part(display_name, ' ', 2)
            end
        )                                           as last_name,
        case
            when trim(email) = '' then null
            else lower(trim(email))
        end                                         as email,
        case
            when trim(phone) = '' then null
            else regexp_replace(phone, '[^0-9+]', '')
        end                                         as phone_clean,
        trim(company)                               as company_name,
        cast(created_at as timestamp)               as created_at,
        cast(last_seen_at as timestamp)             as last_seen_at,
        'support' as source_system
    from source
)

select * from staged
where support_user_id is not null
