-- Intermediate: Name Normalization
-- Standardize names across all 5 source systems into a consistent format
-- Handles: case normalization, initial expansion, nickname resolution, whitespace

-- MANUAL PAIN POINT: Every source has different name fields and formats.
-- CRM has first_name/last_name, Billing has "Last, First" account_name,
-- Support has display_name, App has optional names, Partners often missing names entirely.
-- This would need ongoing maintenance as sources change.

with crm_names as (
    select
        crm_contact_id as source_id,
        'crm' as source_system,
        first_name as raw_first,
        last_name as raw_last,
        email
    from {{ ref('stg_crm__contacts') }}
),

billing_names as (
    select
        billing_account_id as source_id,
        'billing' as source_system,
        first_name as raw_first,
        last_name as raw_last,
        email
    from {{ ref('stg_billing__accounts') }}
),

support_names as (
    select
        support_user_id as source_id,
        'support' as source_system,
        first_name as raw_first,
        last_name as raw_last,
        email
    from {{ ref('stg_support__users') }}
),

app_names as (
    select
        app_user_id as source_id,
        'app' as source_system,
        first_name as raw_first,
        last_name as raw_last,
        email
    from {{ ref('stg_app__signups') }}
),

partner_names as (
    select
        partner_lead_id as source_id,
        'partners' as source_system,
        first_name as raw_first,
        last_name as raw_last,
        email
    from {{ ref('stg_partners__leads') }}
),

unioned as (
    select * from crm_names
    union all select * from billing_names
    union all select * from support_names
    union all select * from app_names
    union all select * from partner_names
),

-- Step 1: Basic case normalization
case_normalized as (
    select
        source_id,
        source_system,
        email,
        raw_first,
        raw_last,
        -- Proper-case: first letter upper, rest lower
        upper(left(trim(raw_first), 1)) || lower(substring(trim(raw_first), 2)) as norm_first,
        upper(left(trim(raw_last), 1)) || lower(substring(trim(raw_last), 2)) as norm_last
    from unioned
    where raw_first is not null and raw_last is not null
      and trim(raw_first) != '' and trim(raw_last) != ''
),

-- Step 2: Resolve common nicknames
-- MAINTENANCE BURDEN: This list grows over time and is never complete
nickname_resolved as (
    select
        source_id,
        source_system,
        email,
        raw_first,
        raw_last,
        case norm_first
            when 'Bob'   then 'Robert'
            when 'Bill'  then 'William'
            when 'Dick'  then 'Richard'
            when 'Jim'   then 'James'
            when 'Mike'  then 'Michael'
            when 'Jen'   then 'Jennifer'
            when 'Liz'   then 'Elizabeth'
            when 'Pat'   then 'Patricia'
            when 'Chris' then 'Christopher'
            when 'Kate'  then 'Katherine'
            when 'Ben'   then 'Benjamin'
            when 'Nick'  then 'Nicholas'
            else norm_first
        end as normalized_first_name,
        norm_last as normalized_last_name
    from case_normalized
)

select
    source_id,
    source_system,
    email,
    raw_first,
    raw_last,
    normalized_first_name,
    normalized_last_name,
    -- Build a full normalized name for display
    normalized_first_name || ' ' || normalized_last_name as normalized_full_name,
    -- Soundex for fuzzy matching later
    soundex(normalized_first_name) as first_name_soundex,
    soundex(normalized_last_name) as last_name_soundex
from nickname_resolved
