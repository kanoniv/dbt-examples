-- Intermediate: Unmatched Records
-- Identify records that didn't match to any other source system
-- These are either truly unique or matching failures

-- MANUAL PAIN POINT: High unmatched rates are a red flag, but diagnosing
-- WHY a record didn't match requires manual investigation. Was it missing
-- identifiers? Different name spelling? Wrong blocking key?

with all_spine as (
    select
        contact_spine_id,
        source_id,
        source_system,
        normalized_first_name,
        normalized_last_name,
        normalized_email,
        normalized_phone,
        normalized_company,
        identity_signal_count
    from {{ ref('int_unified__contact_spine') }}
),

matched as (
    select distinct contact_spine_id
    from {{ ref('int_xmatch__entity_graph') }}
),

unmatched as (
    select
        a.contact_spine_id,
        a.source_id,
        a.source_system,
        a.normalized_first_name,
        a.normalized_last_name,
        a.normalized_email,
        a.normalized_phone,
        a.normalized_company,
        a.identity_signal_count,
        -- Diagnose why this record didn't match
        case
            when a.identity_signal_count <= 1
            then 'insufficient_signals'
            when a.normalized_email is null and a.normalized_phone is null
            then 'no_email_or_phone'
            when a.normalized_first_name is null or a.normalized_last_name is null
            then 'missing_name'
            else 'no_cross_source_match'
        end as unmatched_reason
    from all_spine a
    left join matched m on a.contact_spine_id = m.contact_spine_id
    where m.contact_spine_id is null
)

select
    *,
    -- Source-level unmatched stats
    count(*) over (partition by source_system) as unmatched_in_source,
    count(*) over () as total_unmatched
from unmatched
