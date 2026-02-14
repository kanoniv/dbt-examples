-- Intermediate: Pair Scoring
-- Score each candidate pair using field-level comparison functions
-- Produces a composite match score per pair

-- MANUAL PAIN POINT: This is the heart of identity resolution -- and the hardest
-- to get right manually. Each field comparison has different weight and threshold.
-- Jaro-Winkler for names, exact for email, normalized for phone...
-- Tuning these weights is guesswork without statistical learning (Fellegi-Sunter).

with pairs as (
    select * from {{ ref('int_xmatch__candidate_pairs') }}
),

spine as (
    select * from {{ ref('int_unified__contact_spine') }}
),

-- Enrich pairs with identity signals from both sides
enriched as (
    select
        p.spine_id_a,
        p.spine_id_b,
        p.source_a,
        p.source_b,
        p.num_blocking_keys,
        -- Side A
        a.normalized_first_name as first_a,
        a.normalized_last_name as last_a,
        a.normalized_email as email_a,
        a.normalized_phone as phone_a,
        a.normalized_company as company_a,
        a.normalized_zip as zip_a,
        a.first_name_soundex as first_soundex_a,
        a.last_name_soundex as last_soundex_a,
        a.company_match_key as company_key_a,
        -- Side B
        b.normalized_first_name as first_b,
        b.normalized_last_name as last_b,
        b.normalized_email as email_b,
        b.normalized_phone as phone_b,
        b.normalized_company as company_b,
        b.normalized_zip as zip_b,
        b.first_name_soundex as first_soundex_b,
        b.last_name_soundex as last_soundex_b,
        b.company_match_key as company_key_b
    from pairs p
    join spine a on p.spine_id_a = a.contact_spine_id
    join spine b on p.spine_id_b = b.contact_spine_id
),

-- Score each field comparison
scored as (
    select
        spine_id_a,
        spine_id_b,
        source_a,
        source_b,
        num_blocking_keys,

        -- Email: exact match = 3.0, null = 0
        case
            when email_a is null or email_b is null then 0
            when email_a = email_b then 3.0
            else -1.0
        end as email_score,

        -- Phone: exact match = 2.5, null = 0
        case
            when phone_a is null or phone_b is null then 0
            when phone_a = phone_b then 2.5
            else -1.0
        end as phone_score,

        -- First name: exact = 2.0, soundex = 1.0, mismatch = -0.5
        case
            when first_a is null or first_b is null then 0
            when lower(first_a) = lower(first_b) then 2.0
            when first_soundex_a = first_soundex_b then 1.0
            else -0.5
        end as first_name_score,

        -- Last name: exact = 2.0, soundex = 1.0, mismatch = -1.0
        case
            when last_a is null or last_b is null then 0
            when lower(last_a) = lower(last_b) then 2.0
            when last_soundex_a = last_soundex_b then 1.0
            else -1.0
        end as last_name_score,

        -- Company: exact key match = 1.5, mismatch = -0.5
        case
            when company_key_a is null or company_key_b is null then 0
            when company_key_a = company_key_b then 1.5
            else -0.5
        end as company_score,

        -- Zip code: exact match = 1.0
        case
            when zip_a is null or zip_b is null then 0
            when zip_a = zip_b then 1.0
            else 0
        end as zip_score

    from enriched
)

select
    spine_id_a,
    spine_id_b,
    source_a,
    source_b,
    num_blocking_keys,
    email_score,
    phone_score,
    first_name_score,
    last_name_score,
    company_score,
    zip_score,
    -- Composite score
    (email_score + phone_score + first_name_score +
     last_name_score + company_score + zip_score) as total_score,
    -- Classification based on hand-tuned thresholds
    -- MANUAL PAIN POINT: These thresholds are the most fragile part.
    -- Change one weight and the match/non-match boundary shifts.
    -- No statistical basis -- just "feels right" from manual review.
    case
        when (email_score + phone_score + first_name_score +
              last_name_score + company_score + zip_score) >= 5.0
        then 'match'
        when (email_score + phone_score + first_name_score +
              last_name_score + company_score + zip_score) >= 3.0
        then 'review'
        else 'non_match'
    end as match_classification
from scored
