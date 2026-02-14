-- Intermediate: Source Pair Analysis
-- Analyze match rates between each pair of source systems
-- Identifies which source combinations produce the most/fewest matches

with decisions as (
    select * from {{ ref('int_xmatch__match_decisions') }}
),

pairs as (
    select * from {{ ref('int_xmatch__candidate_pairs') }}
)

select
    d.source_a,
    d.source_b,
    d.source_a || ' ↔ ' || d.source_b as source_pair,
    count(*) as total_decisions,
    sum(case when d.final_decision = 'match' then 1 else 0 end) as match_count,
    sum(case when d.final_decision = 'review' then 1 else 0 end) as review_count,
    round(100.0 * sum(case when d.final_decision = 'match' then 1 else 0 end) /
        nullif(count(*), 0), 1) as match_rate_pct,
    round(100.0 * sum(case when d.final_decision = 'review' then 1 else 0 end) /
        nullif(count(*), 0), 1) as review_rate_pct,
    avg(d.total_score) as avg_match_score,
    -- Field-level contribution analysis
    avg(d.email_score) as avg_email_contribution,
    avg(d.phone_score) as avg_phone_contribution,
    avg(d.first_name_score) as avg_first_name_contribution,
    avg(d.last_name_score) as avg_last_name_contribution,
    avg(d.company_score) as avg_company_contribution
from decisions d
group by d.source_a, d.source_b
order by match_count desc
