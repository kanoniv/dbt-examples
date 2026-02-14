-- Staging: Partner Referrals
-- Clean and type-cast raw partner referral records

with source as (
    select * from {{ source('raw', 'partner_referrals') }}
),

staged as (
    select
        referral_id,
        partner_lead_id,
        trim(referring_partner)                     as referring_partner,
        trim(referral_type)                         as referral_type,
        case when trim(notes) = '' then null else trim(notes) end as notes,
        cast(created_at as timestamp)               as created_at,
        'partners' as source_system
    from source
)

select * from staged
where referral_id is not null
