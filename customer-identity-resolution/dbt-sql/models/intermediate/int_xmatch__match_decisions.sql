-- Intermediate: Match Decisions
-- Filter scored pairs into confirmed matches and review queue
-- Apply business rules on top of statistical scores

-- MANUAL PAIN POINT: After scoring, you need business rules:
-- "Never merge across different companies" or "Always merge exact email"
-- These rules accumulate over time into a tangled web of IF statements.

with scored_pairs as (
    select * from {{ ref('int_xmatch__pair_scores') }}
),

-- Apply business rules as overrides
decisions as (
    select
        spine_id_a,
        spine_id_b,
        source_a,
        source_b,
        total_score,
        match_classification,

        -- Override rules
        case
            -- Rule 1: Exact email match is always a match (even if score is low)
            when email_score = 3.0 and first_name_score >= 0
            then 'match'

            -- Rule 2: If company keys differ, require higher threshold
            when company_score < 0 and total_score < 7.0
            then 'non_match'

            -- Rule 3: Phone + last name exact is a match
            when phone_score = 2.5 and last_name_score = 2.0
            then 'match'

            -- Default: use score-based classification
            else match_classification
        end as final_decision,

        email_score,
        phone_score,
        first_name_score,
        last_name_score,
        company_score,
        zip_score,
        num_blocking_keys
    from scored_pairs
)

select
    spine_id_a,
    spine_id_b,
    source_a,
    source_b,
    total_score,
    final_decision,
    email_score,
    phone_score,
    first_name_score,
    last_name_score,
    company_score,
    zip_score,
    num_blocking_keys,
    -- Confidence bucket for reporting
    case
        when total_score >= 8.0 then 'high'
        when total_score >= 5.0 then 'medium'
        else 'low'
    end as confidence
from decisions
where final_decision in ('match', 'review')
