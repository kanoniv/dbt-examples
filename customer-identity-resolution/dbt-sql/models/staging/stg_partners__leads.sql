-- Staging: Partner Leads
-- Clean and type-cast raw partner-submitted lead records
-- Partner data is very sparse -- often missing names or emails

with source as (
    select * from {{ source('raw', 'partner_leads') }}
),

staged as (
    select
        partner_lead_id,
        trim(partner_name)                          as partner_name,
        case when trim(first_name) = '' then null else trim(first_name) end as first_name,
        case when trim(last_name) = '' then null else trim(last_name) end   as last_name,
        case when trim(email) = '' then null else lower(trim(email)) end    as email,
        trim(company)                               as company_name,
        cast(estimated_arr as integer)              as estimated_arr,
        trim(stage)                                 as stage,
        cast(submitted_at as timestamp)             as submitted_at,
        'partners' as source_system
    from source
)

select * from staged
where partner_lead_id is not null
