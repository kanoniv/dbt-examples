-- Intermediate: Entity Graph
-- Maps every source record to its resolved entity + golden record
-- This is the final linkage table before marts

-- MANUAL PAIN POINT: Maintaining the full provenance graph -- which source records
-- belong to which entity, what scores linked them -- requires joining 5+ models.
-- When a match is wrong, tracing WHY it was linked is a forensic exercise.

with clusters as (
    select * from {{ ref('int_xmatch__transitive_closure') }}
),

golden as (
    select * from {{ ref('int_xmatch__golden_record') }}
),

spine as (
    select * from {{ ref('int_unified__contact_spine') }}
),

decisions as (
    select * from {{ ref('int_xmatch__match_decisions') }}
    where final_decision = 'match'
)

select
    c.contact_spine_id,
    s.source_id,
    s.source_system,
    c.resolved_entity_id,
    g.entity_number,
    g.golden_full_name,
    g.golden_email,
    g.golden_phone,
    g.golden_company,
    s.normalized_first_name as source_first_name,
    s.normalized_last_name as source_last_name,
    s.normalized_email as source_email,
    s.normalized_phone as source_phone,
    s.normalized_company as source_company,
    s.identity_signal_count
from clusters c
join spine s on c.contact_spine_id = s.contact_spine_id
join golden g on c.resolved_entity_id = g.resolved_entity_id
