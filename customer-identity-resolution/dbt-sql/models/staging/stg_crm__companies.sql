-- Staging: CRM Companies
-- Clean and type-cast raw CRM company records

with source as (
    select * from {{ source('raw', 'crm_companies') }}
),

staged as (
    select
        crm_company_id,
        trim(company_name)                          as company_name,
        lower(trim(domain))                         as domain,
        trim(industry)                              as industry,
        cast(employee_count as integer)             as employee_count,
        cast(created_at as timestamp)               as created_at,
        'crm' as source_system
    from source
)

select * from staged
where crm_company_id is not null
