-- Mart: Source Quality Report
-- Data quality metrics per source system
-- Identifies which sources are clean vs. which need attention

with spine as (
    select * from {{ ref('int_unified__contact_spine') }}
),

unmatched as (
    select * from {{ ref('int_resolve__unmatched_records') }}
),

conflicts as (
    select * from {{ ref('int_resolve__conflict_log') }}
),

source_metrics as (
    select
        source_system,
        count(*) as total_records,
        -- Completeness: what % of records have each field?
        round(100.0 * count(normalized_email) / count(*), 1) as email_completeness_pct,
        round(100.0 * count(normalized_phone) / count(*), 1) as phone_completeness_pct,
        round(100.0 * count(normalized_first_name) / count(*), 1) as name_completeness_pct,
        round(100.0 * count(normalized_company) / count(*), 1) as company_completeness_pct,
        avg(identity_signal_count) as avg_identity_signals,
        -- Matchability: how many records had enough signals for matching?
        round(100.0 * count(case when identity_signal_count >= 2 then 1 end) / count(*), 1)
            as matchable_pct
    from spine
    group by source_system
),

unmatched_rates as (
    select
        source_system,
        count(*) as unmatched_count
    from unmatched
    group by source_system
),

conflict_rates as (
    select
        source_system,
        count(*) as conflict_count
    from conflicts
    group by source_system
)

select
    sm.source_system,
    sm.total_records,
    sm.email_completeness_pct,
    sm.phone_completeness_pct,
    sm.name_completeness_pct,
    sm.company_completeness_pct,
    sm.avg_identity_signals,
    sm.matchable_pct,
    coalesce(ur.unmatched_count, 0) as unmatched_count,
    round(100.0 * coalesce(ur.unmatched_count, 0) / sm.total_records, 1) as unmatched_rate_pct,
    coalesce(cr.conflict_count, 0) as conflict_count,
    -- Overall quality grade
    case
        when sm.email_completeness_pct >= 80 and sm.name_completeness_pct >= 80
             and sm.matchable_pct >= 90 then 'A'
        when sm.email_completeness_pct >= 60 and sm.name_completeness_pct >= 60
             and sm.matchable_pct >= 70 then 'B'
        when sm.email_completeness_pct >= 40 and sm.matchable_pct >= 50 then 'C'
        else 'D'
    end as quality_grade
from source_metrics sm
left join unmatched_rates ur on sm.source_system = ur.source_system
left join conflict_rates cr on sm.source_system = cr.source_system
order by sm.source_system
