-- Intermediate: Partner Lead Deduplication
-- Find duplicate partner leads (same person referred by multiple partners)
-- Partner data is the sparsest -- many records missing names or emails

-- MANUAL PAIN POINT: Partner leads often overlap. The same prospect gets referred
-- by 2-3 partners. With 30% missing emails and 30% missing names, dedup is unreliable.
-- False negatives mean you count the same deal 3x in the pipeline.

with leads as (
    select
        s.contact_spine_id,
        s.source_id,
        s.normalized_first_name,
        s.normalized_last_name,
        s.first_name_soundex,
        s.last_name_soundex,
        s.normalized_email,
        s.normalized_company,
        s.company_match_key,
        p.estimated_arr,
        p.stage,
        p.submitted_at
    from {{ ref('int_unified__contact_spine') }} s
    join {{ ref('stg_partners__leads') }} p
        on s.source_id = p.partner_lead_id
    where s.source_system = 'partners'
),

-- Email-based duplicates
email_matches as (
    select normalized_email, count(*) as cnt
    from leads
    where normalized_email is not null
    group by normalized_email
    having count(*) > 1
),

-- Company + name soundex matches (for leads missing email)
company_name_matches as (
    select
        a.contact_spine_id as spine_id_a,
        b.contact_spine_id as spine_id_b
    from leads a
    join leads b
        on a.company_match_key = b.company_match_key
        and a.last_name_soundex = b.last_name_soundex
        and a.contact_spine_id < b.contact_spine_id
    where a.company_match_key is not null
      and a.last_name_soundex is not null
      and a.normalized_email is null  -- Only for email-less records
),

-- Rank: prefer furthest stage, then highest ARR
ranked as (
    select
        l.contact_spine_id,
        l.source_id as partner_lead_id,
        l.normalized_email,
        l.normalized_first_name,
        l.normalized_last_name,
        l.normalized_company,
        l.stage,
        l.estimated_arr,
        row_number() over (
            partition by coalesce(l.normalized_email, l.company_match_key || '|' || l.last_name_soundex)
            order by
                case l.stage
                    when 'closed_won' then 0
                    when 'negotiation' then 1
                    when 'engaged' then 2
                    when 'qualified' then 3
                    when 'new' then 4
                    when 'closed_lost' then 5
                end,
                l.estimated_arr desc,
                l.submitted_at asc
        ) as survivor_rank
    from leads l
)

select
    contact_spine_id,
    partner_lead_id,
    normalized_email,
    normalized_first_name,
    normalized_last_name,
    normalized_company,
    stage,
    estimated_arr,
    case when survivor_rank = 1 then true else false end as is_survivor,
    survivor_rank
from ranked
