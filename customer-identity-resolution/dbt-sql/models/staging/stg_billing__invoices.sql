-- Staging: Billing Invoices
-- Clean and type-cast raw billing invoice records

with source as (
    select * from {{ source('raw', 'billing_invoices') }}
),

staged as (
    select
        invoice_id,
        billing_account_id,
        cast(amount_cents as integer)               as amount_cents,
        round(cast(amount_cents as numeric) / 100, 2) as amount_dollars,
        upper(trim(currency))                       as currency,
        trim(status)                                as invoice_status,
        cast(issued_at as timestamp)                as issued_at,
        case
            when trim(paid_at) = '' then null
            else cast(paid_at as timestamp)
        end                                         as paid_at,
        'billing' as source_system
    from source
)

select * from staged
where invoice_id is not null
