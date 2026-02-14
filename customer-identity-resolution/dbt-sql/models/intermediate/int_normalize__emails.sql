-- Intermediate: Email Normalization
-- Standardize email addresses across all sources for matching
-- Handles: case, whitespace, gmail dot-trick, plus-addressing, domain aliases

-- MANUAL PAIN POINT: Email normalization rules are deceptively complex.
-- Gmail ignores dots, plus-addressing is common, company domain mergers
-- create aliases (acme.com → acme.io). Each edge case is another CASE statement.

with all_emails as (
    select crm_contact_id as source_id, 'crm' as source_system, email
    from {{ ref('stg_crm__contacts') }}
    where email is not null and trim(email) != ''

    union all

    select billing_account_id, 'billing', email
    from {{ ref('stg_billing__accounts') }}
    where email is not null and trim(email) != ''

    union all

    select support_user_id, 'support', email
    from {{ ref('stg_support__users') }}
    where email is not null

    union all

    select app_user_id, 'app', email
    from {{ ref('stg_app__signups') }}
    where email is not null and trim(email) != ''

    union all

    select partner_lead_id, 'partners', email
    from {{ ref('stg_partners__leads') }}
    where email is not null
),

parsed as (
    select
        source_id,
        source_system,
        email as raw_email,
        lower(trim(email)) as clean_email,
        split_part(lower(trim(email)), '@', 1) as local_part,
        split_part(lower(trim(email)), '@', 2) as domain
    from all_emails
),

normalized as (
    select
        source_id,
        source_system,
        raw_email,
        clean_email,
        local_part,
        domain,

        -- Strip plus-addressing: user+tag@domain → user@domain
        case
            when local_part like '%+%'
            then split_part(local_part, '+', 1)
            else local_part
        end as local_part_no_plus,

        -- Gmail dot-trick: f.i.r.s.t.last@gmail.com → firstlast@gmail.com
        case
            when domain in ('gmail.com', 'googlemail.com')
            then replace(
                case
                    when local_part like '%+%'
                    then split_part(local_part, '+', 1)
                    else local_part
                end,
                '.', ''
            )
            else case
                when local_part like '%+%'
                then split_part(local_part, '+', 1)
                else local_part
            end
        end as normalized_local,

        -- Normalize common domain aliases
        case domain
            when 'googlemail.com' then 'gmail.com'
            when 'hotmail.co.uk'  then 'hotmail.com'
            when 'live.com'       then 'outlook.com'
            else domain
        end as normalized_domain
    from parsed
)

select
    source_id,
    source_system,
    raw_email,
    clean_email,
    normalized_local || '@' || normalized_domain as normalized_email,
    normalized_local,
    normalized_domain,
    -- Flag corporate vs personal email
    case
        when normalized_domain in ('gmail.com', 'yahoo.com', 'outlook.com',
            'hotmail.com', 'icloud.com', 'protonmail.com', 'fastmail.com', 'aol.com')
        then 'personal'
        else 'corporate'
    end as email_type,
    -- Extract company domain for corporate email matching
    case
        when normalized_domain not in ('gmail.com', 'yahoo.com', 'outlook.com',
            'hotmail.com', 'icloud.com', 'protonmail.com', 'fastmail.com', 'aol.com')
        then normalized_domain
        else null
    end as corporate_domain
from normalized
