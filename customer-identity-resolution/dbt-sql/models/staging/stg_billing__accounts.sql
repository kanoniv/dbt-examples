-- Staging: Billing Accounts
-- Clean and type-cast raw billing account records
-- Note: account_name may be "Last, First" or "First Last" format

with source as (
    select * from {{ source('raw', 'billing_accounts') }}
),

staged as (
    select
        billing_account_id,
        trim(account_name)                          as account_name,
        -- Parse name: handle "Last, First" vs "First Last"
        case
            when account_name like '%,%'
            then trim(split_part(account_name, ',', 2))
            else trim(split_part(account_name, ' ', 1))
        end                                         as first_name,
        case
            when account_name like '%,%'
            then trim(split_part(account_name, ',', 1))
            else trim(split_part(account_name, ' ', 2))
        end                                         as last_name,
        lower(trim(email))                          as email,
        trim(company_name)                          as company_name,
        trim(billing_address)                       as billing_address,
        trim(payment_method)                        as payment_method,
        trim(plan)                                  as plan_name,
        cast(mrr_cents as integer)                  as mrr_cents,
        round(cast(mrr_cents as numeric) / 100, 2)  as mrr_dollars,
        cast(created_at as timestamp)               as created_at,
        trim(status)                                as account_status,
        'billing' as source_system
    from source
)

select * from staged
where billing_account_id is not null
