-- Intermediate: Review Queue
-- Pairs classified as "review" need human adjudication
-- Outputs a queue for analysts to manually approve/reject matches

-- MANUAL PAIN POINT: The review queue is the tax you pay for imprecise matching.
-- With hand-tuned thresholds, 10-20% of pairs land here. Each needs a human
-- to look at both records and decide. At scale, this is an FTE.

with review_pairs as (
    select
        spine_id_a,
        spine_id_b,
        source_a,
        source_b,
        total_score,
        email_score,
        phone_score,
        first_name_score,
        last_name_score,
        company_score,
        zip_score,
        confidence
    from {{ ref('int_xmatch__match_decisions') }}
    where final_decision = 'review'
),

spine as (
    select * from {{ ref('int_unified__contact_spine') }}
),

enriched as (
    select
        r.spine_id_a,
        r.spine_id_b,
        r.source_a,
        r.source_b,
        r.total_score,
        r.confidence,
        -- Side A details for human review
        a.normalized_full_name as name_a,
        a.normalized_email as email_a,
        a.normalized_phone as phone_a,
        a.normalized_company as company_a,
        -- Side B details for human review
        b.normalized_full_name as name_b,
        b.normalized_email as email_b,
        b.normalized_phone as phone_b,
        b.normalized_company as company_b,
        -- Score breakdown
        r.email_score,
        r.phone_score,
        r.first_name_score,
        r.last_name_score,
        r.company_score,
        r.zip_score
    from review_pairs r
    join spine a on r.spine_id_a = a.contact_spine_id
    join spine b on r.spine_id_b = b.contact_spine_id
)

select
    spine_id_a,
    spine_id_b,
    source_a,
    source_b,
    name_a,
    email_a,
    phone_a,
    company_a,
    name_b,
    email_b,
    phone_b,
    company_b,
    total_score,
    confidence,
    -- Reason why it's in review (not auto-matched)
    case
        when email_score = 0 and phone_score = 0 then 'No email or phone overlap -- name/company only'
        when company_score < 0 then 'Different companies -- score insufficient'
        when first_name_score < 1 then 'First name mismatch -- possible nickname'
        else 'Score in review range (' || total_score || ')'
    end as review_reason,
    current_timestamp as queued_at
from enriched
order by total_score desc
