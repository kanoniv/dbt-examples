-- Intermediate: Company Name Normalization
-- Standardize company names across all sources for matching
-- Handles: suffixes (Inc, Corp, LLC), case, common abbreviations

-- MANUAL PAIN POINT: Company names are inconsistent everywhere.
-- "Acme Corp" vs "ACME" vs "acme corp" vs "Acme Corporation" -- all the same entity.
-- Without a reference database (D&B, Clearbit), normalization is heuristic-based.

with all_companies as (
    select crm_contact_id as source_id, 'crm' as source_system, company_name
    from {{ ref('stg_crm__contacts') }}
    where company_name is not null and trim(company_name) != ''

    union all

    select billing_account_id, 'billing', company_name
    from {{ ref('stg_billing__accounts') }}
    where company_name is not null and trim(company_name) != ''

    union all

    select support_user_id, 'support', company_name
    from {{ ref('stg_support__users') }}
    where company_name is not null and trim(company_name) != ''

    union all

    select partner_lead_id, 'partners', company_name
    from {{ ref('stg_partners__leads') }}
    where company_name is not null and trim(company_name) != ''
),

normalized as (
    select
        source_id,
        source_system,
        company_name as raw_company,
        -- Step 1: Trim and proper-case
        trim(company_name) as clean_company,
        -- Step 2: Strip common suffixes for matching
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                upper(trim(company_name)),
                                '\\s+(INC\\.?|INCORPORATED)$', ''),
                            '\\s+(CORP\\.?|CORPORATION)$', ''),
                        '\\s+(LLC|L\\.L\\.C\\.)$', ''),
                    '\\s+(LTD\\.?|LIMITED)$', ''),
                '\\s+(CO\\.?|COMPANY)$', '')
        ) as stripped_company,
        upper(trim(company_name)) as upper_company
    from all_companies
)

select
    source_id,
    source_system,
    raw_company,
    clean_company,
    stripped_company as normalized_company,
    -- Create a matching key: uppercase, no suffixes, no special chars
    regexp_replace(stripped_company, '[^A-Z0-9 ]', '') as company_match_key,
    -- Soundex for fuzzy company matching
    soundex(stripped_company) as company_soundex
from normalized
