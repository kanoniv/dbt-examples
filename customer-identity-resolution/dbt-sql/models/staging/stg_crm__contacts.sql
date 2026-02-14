-- Staging: CRM Contacts
-- Clean and type-cast raw CRM contact records

with source as (
    select * from {{ source('raw', 'crm_contacts') }}
),

staged as (
    select
        crm_contact_id,
        trim(first_name)                            as first_name,
        trim(last_name)                             as last_name,
        lower(trim(email))                          as email,
        regexp_replace(phone, '[^0-9+]', '')        as phone_clean,
        trim(company_name)                          as company_name,
        trim(title)                                 as title,
        trim(lead_source)                           as lead_source,
        cast(created_at as timestamp)               as created_at,
        cast(updated_at as timestamp)               as updated_at,
        case when is_active = 'true' then true else false end as is_active,
        'crm' as source_system
    from source
)

select * from staged
where crm_contact_id is not null
