-- Intermediate: Phone Normalization
-- Standardize phone numbers across all sources into E.164 format
-- Handles: parentheses, dashes, dots, spaces, missing country codes

-- MANUAL PAIN POINT: Phone formats are wildly inconsistent across sources.
-- CRM: "+12125551234", Billing: none, Support: "(212) 555-1234",
-- App: none, Partners: none. Even within one source, formats vary.

with all_phones as (
    select
        crm_contact_id as source_id,
        'crm' as source_system,
        phone_clean as phone
    from {{ ref('stg_crm__contacts') }}
    where phone_clean is not null and trim(phone_clean) != ''

    union all

    select
        support_user_id,
        'support',
        phone_clean
    from {{ ref('stg_support__users') }}
    where phone_clean is not null
),

cleaned as (
    select
        source_id,
        source_system,
        phone as raw_phone,
        -- Strip everything except digits and leading +
        regexp_replace(phone, '[^0-9]', '') as digits_only
    from all_phones
),

normalized as (
    select
        source_id,
        source_system,
        raw_phone,
        digits_only,
        case
            -- Already has country code (11 digits starting with 1)
            when length(digits_only) = 11 and left(digits_only, 1) = '1'
            then '+' || digits_only
            -- 10 digits  -- assume US, prepend +1
            when length(digits_only) = 10
            then '+1' || digits_only
            -- Has + prefix already in raw
            when left(raw_phone, 1) = '+' and length(digits_only) >= 10
            then '+' || digits_only
            -- Fallback: return what we have
            else '+' || digits_only
        end as normalized_phone
    from cleaned
    where length(digits_only) >= 10
)

select
    source_id,
    source_system,
    raw_phone,
    normalized_phone,
    -- Extract area code for blocking
    substring(normalized_phone, 3, 3) as area_code,
    -- Extract subscriber number (last 4 digits) for quick matching
    right(normalized_phone, 4) as last_four
from normalized
